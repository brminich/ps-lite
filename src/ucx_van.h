#ifndef PS_UCX_VAN_H_
#define PS_UCX_VAN_H_

#ifdef DMLC_USE_UCX

#include <ucp/api/ucp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netdb.h>

#include <list>

#define CHECK_STATUS(_status) CHECK((_status) == UCS_OK)

namespace ps {

#define UCX_REQUEST_FREE(_req) \
  do { \
    (_req)->data.buffer   = nullptr; \
    (_req)->data.raw_meta = nullptr; \
    (_req)->completed     = false; \
    ucp_request_free(_req); \
  } while(0)

class UCXVan;

struct UCXBuffer {
  char    *raw_meta;
  char    *buffer;
  int     sender;
};

struct UCXRequest {
  UCXVan    *van;
  bool      completed;
  UCXBuffer data;
};

class UCXVan : public Van {
 public:
  UCXVan() {
    setenv("UCX_USE_MT_MUTEX", "y", 0);
    setenv("UCX_IB_NUM_PATHS", "2", 0);
    setenv("UCX_SOCKADDR_CM_ENABLE", "y", 0);
  }

  ~UCXVan() {}

 protected:
  enum Tags { UCX_TAG_META = 0, UCX_TAG_DATA };

  void Start(int customer_id) override {
    start_mu_.lock();
    should_stop_ = false;
    ucp_config_t *config;

    ucs_status_t status = ucp_config_read("PSLITE", NULL, &config);
    CHECK_STATUS(status) << "ucp_config_read failed: " << ucs_status_string(status);

    // Initialize UCX context
    ucp_params_t ctx_params;
    ctx_params.field_mask        = UCP_PARAM_FIELD_FEATURES          |
                                   UCP_PARAM_FIELD_REQUEST_SIZE      |
                                   UCP_PARAM_FIELD_TAG_SENDER_MASK   |
                                   UCP_PARAM_FIELD_REQUEST_INIT      |
                                   UCP_PARAM_FIELD_ESTIMATED_NUM_EPS;
    ctx_params.features          = UCP_FEATURE_TAG;
    ctx_params.request_size      = sizeof(UCXRequest);
    ctx_params.tag_sender_mask   = 0ul;
    ctx_params.request_init      = RequestInit;
    ctx_params.estimated_num_eps = GetEnv("DMLC_NUM_WORKER", 0) +
                                   GetEnv("DMLC_NUM_SERVER", 0);

    status = ucp_init(&ctx_params, config, &context_);
    ucp_config_release(config);
    CHECK_STATUS(status) << "ucp_init failed: " << ucs_status_string(status);

    // Create UCP worker
    ucp_worker_params_t w_params;
    w_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    w_params.thread_mode = UCS_THREAD_MODE_MULTI;
    status = ucp_worker_create(context_, &w_params, &worker_);
    CHECK_STATUS(status) << "ucp_worker_create failed: " << ucs_status_string(status);

    polling_thread_.reset(new std::thread(&UCXVan::PollUCX, this));
    role_s_ = (Postoffice::Get()->is_scheduler() ? "scheduler" :
               (my_node_.role == Node::WORKER ? "worker" : "server"));
    role_s_ += "(" + (my_node_.id == Node::kEmpty ? "?" : std::to_string(my_node_.id))  + ")";

    PS_VLOG(1)<< role_s_ << "Started with numeps " << ctx_params.estimated_num_eps;

    start_mu_.unlock();
    Van::Start(customer_id);
    role_s_ = (Postoffice::Get()->is_scheduler() ? "scheduler" :
               (my_node_.role == Node::WORKER ? "worker" : "server"));
    role_s_ += "(" + (my_node_.id == Node::kEmpty ? "?" : std::to_string(my_node_.id))  + ")";
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    should_stop_ = true;
    PS_VLOG(1) << "Stopping polling_thread_";
    polling_thread_->join();
    polling_thread_.reset();

    std::list<void*> close_reqs;
    for (auto& it : endpoints_) {
      void *req = ucp_ep_close_nb(it.second, UCP_EP_CLOSE_MODE_FLUSH);
      if (UCS_PTR_IS_PTR(req)) {
        close_reqs.push_back(req);
      } else if (UCS_PTR_STATUS(req) != UCS_OK) {
        LOG(ERROR) << "failed to close ep(" << it.first << ") " << it.second;
      }
    }

    while (close_reqs.size()) {
      ucp_worker_progress(worker_);
      for (auto it = close_reqs.begin(); it != close_reqs.end();) {
        ucs_status_t status = ucp_request_check_status(*it);
        if (status != UCS_INPROGRESS) {
          ucp_request_free(*it);
          it = close_reqs.erase(it);
        } else {
          ++it;
        }
      }
    }

    ucp_listener_destroy(listener_);
    ucp_worker_destroy(worker_);
    ucp_cleanup(context_);

    for (auto& it : rpool_) {
      free(it.second.first);
    }
  }

  int Bind(const Node &node, int max_retry) override {
    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    struct sockaddr_in addr = {};
    if (val) {
      PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
      addr.sin_addr.s_addr = inet_addr(val);
    } else {
      addr.sin_addr.s_addr = INADDR_ANY;
    }
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(node.port);

    ucp_listener_params_t params;
    params.field_mask       = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                              UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr    = (const struct sockaddr*)&addr;
    params.sockaddr.addrlen = sizeof(addr);
    params.conn_handler.cb  = ConnHandler;
    params.conn_handler.arg = this;

    ucs_status_t status = ucp_listener_create(worker_, &params, &listener_);
    CHECK_STATUS(status) << "ucp_listener_create failed: " << ucs_status_string(status);

    PS_VLOG(1) << role_s_ << "Bound to " << addr.sin_addr.s_addr << " port: " << node.port;

    ucp_listener_attr_t attr;
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    status = ucp_listener_query(listener_, &attr);
    char ip_str[128];
    PS_VLOG(1) << role_s_ << "Listening on "
     << inet_ntop(AF_INET, &((struct sockaddr_in*)(&attr.sockaddr))->sin_addr, ip_str, 128)
     << ntohs(((struct sockaddr_in*)&attr.sockaddr)->sin_port);

    return ntohs(((struct sockaddr_in*)&attr.sockaddr)->sin_port);
  }

  void Connect(const Node &node) override {
    PS_VLOG(1) << role_s_ << "Connecting to Node " << node.id;
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    if (node.id == Node::kEmpty) {
      PS_VLOG(1) << "Ignore request to connect to empty node id";
    }

    struct addrinfo *remote_addr;
    CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                         nullptr, &remote_addr),
             0);

    ucp_ep_params_t ep_params;
    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = remote_addr->ai_addr;
    ep_params.sockaddr.addrlen = remote_addr->ai_addrlen;

    ucp_ep_h ep;
    ucs_status_t status = ucp_ep_create(worker_, &ep_params, &ep);
    endpoints_[node.id] = ep;
    CHECK_STATUS(status) << "ucp_ep_create failed: " << ucs_status_string(status);
  }

  int SendMsg(Message &msg) override {
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);

    auto it = endpoints_.find(id);
    if (it == endpoints_.end()) {
      PS_VLOG(1) << "there is no UCX endpoint to node " << id;
      return -1;
    }

    if (!IsValidPushpull(msg)) {
      msg.meta.val_len = 0;
    } else {
      if (msg.meta.request) {
        msg.meta.key = DecodeKey(msg.data[0]);
      }
      if ((msg.meta.push && msg.meta.request) ||
          (!msg.meta.push && !msg.meta.request)) {
        msg.meta.val_len = msg.data[1].size();
      } else {
        msg.meta.val_len = 0;
      }
    }

    // Pack and send meta data
    int meta_size;
    char* meta_buf = nullptr;
    PackMeta(msg.meta, &meta_buf, &meta_size);

    // Pack node id to the high 32 bits of tag
    ucp_tag_t tag = ((ucp_tag_t)my_node_.id << 32) | UCX_TAG_META;
    PS_VLOG(2) << role_s_ << "Sending to ep(" << it->first << ") " << it->second
        << " data valid push " << IsValidPushpull(msg);
    UCXRequest *req = (UCXRequest*)ucp_tag_send_nb(it->second, meta_buf, meta_size,
                                                   ucp_dt_make_contig(1),
                                                   tag, TxReqCompleted);
    if (UCS_PTR_IS_PTR(req)) {
      req->data.raw_meta = meta_buf;
    } else {
      // Send was completed immediately
      delete[] meta_buf;
      if (UCS_PTR_IS_ERR(req)) {
        LOG(ERROR) << "Failed to send meta data: " << ucs_status_string(UCS_PTR_STATUS(req));
        return -1;
      }
    }

    size_t total_len = meta_size + msg.meta.data_size;

    if (msg.meta.val_len) {
      req = (UCXRequest*)ucp_tag_send_nb(it->second, msg.data[1].data(),
                                         msg.data[1].size(), ucp_dt_make_contig(1),
                                         UCX_TAG_DATA, TxReqCompleted);
      PS_VLOG(2) << "Send data, len " << msg.data[1].size() << " req " << req;
      if (UCS_PTR_IS_ERR(req)) {
        PS_VLOG(1) << "Failed to send data" << ucs_status_string(UCS_PTR_STATUS(req));
        return -1;
      }
    }

    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    UCXBuffer buf;
    recv_buffers_.WaitAndPop(&buf);

    // note size(2d param) is not really used by UnpackMeta
    UnpackMeta(buf.raw_meta, -1, &msg->meta);

    size_t total_len = GetPackMetaLen(msg->meta);
    msg->meta.sender = buf.sender;
    msg->meta.recver = my_node_.id;

    if (!IsValidPushpull(*msg) || (msg->meta.push && !msg->meta.request)) {
      CHECK_EQ(msg->meta.val_len, 0);
    PS_VLOG(2) << role_s_ << "Recv CTRL msg, sender " << buf.sender;
      return total_len;
    }else {
    PS_VLOG(2) << role_s_ << "Recv REAL  msg sender " << buf.sender <<  " len " << msg->meta.val_len ;
    }

    SArray<char> keys;
    keys.CopyFrom((char*)&msg->meta.key, sizeof(Key));
    msg->data.push_back(keys);

    SArray<char> vals;

    if (!msg->meta.push && msg->meta.request) {
      // pull request - just key and empty vals (kvapp check?)
      msg->data.push_back(vals);
      return total_len + keys.size();
    }

    // Push request or pull response - add data and length
    vals.reset(buf.buffer, msg->meta.val_len, [](void *) {});
    msg->data.push_back(vals);

    SArray<char> lens;
    lens.CopyFrom((char*)(&msg->meta.val_len), sizeof(int));
    msg->data.push_back(lens);

    total_len += keys.size() + vals.size() + lens.size();

    return total_len;
  }

 private:

  uint64_t DecodeKey(SArray<char> keys) {
    // Just one key is supported now
    CHECK_EQ(keys.size(), 8) << "Wrong key size " << keys.size();
    return *((uint64_t*)keys.data());
  }

  char *GetRxBuffer(uint64_t key, size_t size) {
    auto it = rpool_.find(key);
    if (it != rpool_.end()) {
      if (size <= it->second.second) {
        PS_VLOG(2) << role_s_ << "RX IN POOL by key " << key;
        return it->second.first;
      }
      // cached buffer is smaller than requested - free it and reallocate
      free(it->second.first);
    }

    char *buf;
    size_t page_size = sysconf(_SC_PAGESIZE);
    int ret          = posix_memalign((void**)&buf, page_size, size);
    CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
    CHECK(buf);
    memset(buf, 0, size);

    rpool_[key] = std::make_pair(buf, size);
    PS_VLOG(2) << role_s_ << "RX NEW by key " << key;

    return buf;
  }

  void PollUCX() {
    while (!should_stop_.load()) {
      ucp_tag_message_h msg;
      ucp_tag_recv_info_t info;
      int cnt = 0;

      while ((cnt == 0) && !should_stop_.load()) {
        cnt = ucp_worker_progress(worker_);
      }

      do {
        // Match only 32 bits of tag. Higher bits of meta data tag carry sender id
        msg = ucp_tag_probe_nb(worker_, UCX_TAG_META,
                               std::numeric_limits<uint32_t>::max(), 1, &info);
        if (msg != NULL) {
          // Some meta data is ready, post a receive to get it
          UCXRequest *meta_req = PostRecvMeta(msg, &info);
          if (meta_req->completed) {
            // Meta data received immediately, can post receive for a real
            // data, because it's length is known from meta
            PostRecvData(meta_req);
          }
        }
      } while ((msg != NULL) && !should_stop_.load());
    }
  }

  void PostRecvData(UCXRequest *meta_req) {
    RawMeta *meta = reinterpret_cast<RawMeta*>(meta_req->data.raw_meta);
    int val_len   = meta->val_len;
    if (val_len == 0) {
      CHECK_EQ(meta_req->data.buffer, nullptr);
      recv_buffers_.Push(meta_req->data);
      PS_VLOG(2) <<role_s_<< "Received just META sender " << meta_req->data.sender;
    } else {
      PS_VLOG(2) <<role_s_<< "PostRecvData len " << val_len << " send " << meta_req->data.sender;

      char *buf = GetRxBuffer(meta->key, val_len);

      UCXRequest *req = (UCXRequest*)ucp_tag_recv_nb(worker_, buf, val_len,
                                                     ucp_dt_make_contig(1), UCX_TAG_DATA,
                                                     std::numeric_limits<uint64_t>::max(),
                                                     RxDataCompleted);
      req->data.raw_meta = meta_req->data.raw_meta;
      req->data.sender   = meta_req->data.sender;
      req->data.buffer   = buf;
      req->van           = this;
      if (req->completed) {
        recv_buffers_.Push(req->data);
        UCX_REQUEST_FREE(req);
      }
    }
    // if request is not completed in-place, it will be handled
    // in RxDataCompleted callback

    UCX_REQUEST_FREE(meta_req); // meta req is not needed anymore
  }

  UCXRequest* PostRecvMeta(ucp_tag_message_h msg, ucp_tag_recv_info_t *info) {
    char *rmeta     = new char[info->length];
    UCXRequest *req = (UCXRequest*)ucp_tag_msg_recv_nb(worker_, rmeta, info->length,
                                                       ucp_dt_make_contig(1), msg,
                                                       RxMetaCompleted);
    req->van           = this;
    req->data.raw_meta = rmeta;
    req->data.sender   = (int)(info->sender_tag >> 32);
    PS_VLOG(2) << role_s_ << "RX MEta, sender " << req->data.sender << " tag "
               << info->sender_tag << " compl " << req->completed << std::flush;
    return req;
  }

  // UCX callbacks
  static void ConnHandler(ucp_conn_request_h conn_request, void *arg) {
    UCXVan *van = reinterpret_cast<UCXVan*>(arg);
    PS_VLOG(1) <<van->role_s_ << " !!!! GOT COnn request";

    ucp_ep_params_t ep_params;
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request    = conn_request;

    ucp_ep_h ep;
    ucs_status_t status = ucp_ep_create(van->worker_, &ep_params, &ep);
    CHECK_STATUS(status) << "Failed to create ep by request " << ucs_status_string(status);
  }

  static void TxReqCompleted(void *request, ucs_status_t status)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "TX request completed with " << ucs_status_string(status);

    delete [] req->data.raw_meta;

    UCX_REQUEST_FREE(req);
  }

  static void RxMetaCompleted(void *request, ucs_status_t status,
                              ucp_tag_recv_info_t *info)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "RxMetaCompleted failed with " << ucs_status_string(status);
    req->completed = true;
    if (req->data.raw_meta == nullptr) {
      // immediate completion
      return;
    }

    req->van->PostRecvData(req);
  }

  static void RxDataCompleted(void *request, ucs_status_t status,
                                 ucp_tag_recv_info_t *info)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "RxDataCompleted failed with " << ucs_status_string(status);
    req->completed = true;
    if (req->data.buffer == nullptr) {
      // immediate completion
      return;
    }
    req->van->recv_buffers_.Push(req->data);
    UCX_REQUEST_FREE(req); // can release meta request back to UCX now
  }

  static void RequestInit(void *request) {
    UCXRequest *req    = reinterpret_cast<UCXRequest*>(request);
    req->data.buffer   = nullptr;
    req->data.raw_meta = nullptr;
    req->completed     = false;
  }

  ucp_context_h  context_;
  ucp_worker_h   worker_;
  ucp_listener_h listener_;
  std::unique_ptr<std::thread> polling_thread_;
  ThreadsafeQueue<UCXBuffer> recv_buffers_;
  std::unordered_map<int, ucp_ep_h> endpoints_;
  std::atomic<bool> should_stop_;
  std::string role_s_;
  std::unordered_map<Key, std::pair<char*, size_t>> rpool_;
};  // class UCXVan

};  // namespace ps

#endif  // DMLC_USE_UCX
#endif  // PS_UCX_VAN_H_
