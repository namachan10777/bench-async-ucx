#include <alloca.h>
#include <arpa/inet.h>
#include <bits/pthreadtypes.h>
#include <errno.h>
#include <ifaddrs.h>
#include <math.h>
#include <mpi.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <threads.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/config/types.h>
#include <ucs/sys/compiler_def.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>
#include <uct/api/uct.h>
#include <unistd.h>

#define UNUSED(x) (void)(x)

static uint16_t listen_port = 10102;

int bn_get_my_addr(struct sockaddr *if_addr_buf) {
  struct ifaddrs *ifaddr, *ifa;

  if (getifaddrs(&ifaddr) == -1) {
    perror("getifaddrs");
    exit(EXIT_FAILURE);
  }

  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL)
      continue;

    if ((strncmp(ifa->ifa_name, "enp", 3) == 0) &&
        (ifa->ifa_addr->sa_family == AF_INET)) {
      memcpy(if_addr_buf, ifa->ifa_addr, sizeof(struct sockaddr));
      struct sockaddr_in *inet4_view = (struct sockaddr_in *)if_addr_buf;
      // グローバル関数じゃない方がいいかも
      inet4_view->sin_port = listen_port;
      inet4_view->sin_family = AF_INET;
      memset(inet4_view->sin_zero, 0, 8);
      freeifaddrs(ifaddr);
      return 0;
    }
  }

  freeifaddrs(ifaddr);
  return -1;
}

static ucs_status_t bn_request_wait(ucp_worker_h ucp_worker, void *request) {
  ucs_status_t status;
  /* if operation was completed immediately */
  if (request == NULL) {
    return UCS_OK;
  }
  if (UCS_PTR_IS_ERR(request)) {
    ucp_request_free(request);
    return UCS_PTR_STATUS(request);
  }
  while ((status = ucp_request_check_status(request)) == UCS_INPROGRESS) {
    ucp_worker_progress(ucp_worker);
  }
  ucp_request_free(request);
  return status;
}

static ucs_status_t bn_close_ep(ucp_worker_h worker, ucp_ep_h ep) {
  ucp_request_param_t params = {
      .op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS,
      .flags = UCP_EP_CLOSE_MODE_FLUSH,
  };
  ucs_status_ptr_t close_req = ucp_ep_close_nbx(ep, &params);
  ucs_status_t status;
  if ((status = bn_request_wait(worker, close_req)) != UCS_OK) {
    fprintf(stderr, "failed to close ep (%s)\n", ucs_status_string(status));
  };
  return status;
}

const char test_message[] = "Hello World!";

typedef enum bn_client_state_flag {
  BN_CLIENT_WILL_RECV_TAG,
  BN_CLIENT_DID_RECV_TAG,
  BN_CLIENT_WILL_SEND,
  BN_CLIENT_DID_SEND,
  BN_CLIENT_WILL_RECV,
  BN_CLIENT_DID_RECV,
  BN_CLIENT_WILL_SEND_FIN,
  BN_CLIENT_DID_SEND_FIN,
} bn_client_state_flag_t;

typedef struct bn_client_task_state {
  ucp_ep_h ep;
  _Bool ep_closed;
  uint8_t *buffer;
  size_t buffer_size;
  _Bool finished;
  bn_client_state_flag_t state_flag;
  ucp_request_param_t send_params;
  ucp_request_param_t recv_params;
  ucp_request_param_t stream_recv_params;
  size_t count;
  size_t stream_received_size;
  uint64_t tag;
} bn_client_task_state_t;

typedef struct bn_client_thread_state {
  bn_client_task_state_t *tasks;
  size_t task_count;
  _Bool finished;
} bn_client_thread_state_t;

bn_client_thread_state_t *client_threads;
size_t client_thread_count;

typedef struct bn_client_thread_ctx {
  ucp_context_h ctx;
  struct sockaddr *server_addr;
  size_t client_thread_idx;
} bn_client_thread_ctx_t;

static void bn_client_ep_error_handler(void *arg, ucp_ep_h ep,
                                       ucs_status_t status) {
  UNUSED(status);
  UNUSED(ep);
  bn_client_task_state_t *task = (bn_client_task_state_t *)arg;
  task->ep_closed = 1;
}

static size_t endpoint_pingpong_max = 5000000;

static void bn_client_send_cb(void *req, ucs_status_t status, void *arg) {
  bn_client_task_state_t *task_state = arg;
  if (status != UCS_OK) {
    fprintf(stderr, "client_recv_cb: %s\n", ucs_status_string(status));
    task_state->finished = 1;
    return;
  }
  if (req != NULL) {
    ucp_request_free(req);
  }
  switch (task_state->state_flag) {
  case BN_CLIENT_DID_SEND:
    task_state->state_flag = BN_CLIENT_WILL_RECV;
    break;
  case BN_CLIENT_DID_SEND_FIN:
    task_state->state_flag = BN_CLIENT_WILL_RECV;
    task_state->finished = 1;
    break;
  default:
    break;
  }
}

static void bn_cb_client_recv(void *req, ucs_status_t status,
                              const ucp_tag_recv_info_t *recv_info, void *arg) {
  UNUSED(recv_info);
  bn_client_task_state_t *task_state = arg;
  if (status != UCS_OK) {
    fprintf(stderr, "client_recv_cb: %s\n", ucs_status_string(status));
    task_state->finished = 1;
  }
  if (req != NULL) {
    ucp_request_free(req);
  }
  switch (task_state->state_flag) {
  case BN_CLIENT_DID_RECV:
    if (++task_state->count > endpoint_pingpong_max) {
      fprintf(stderr, "send FIN\n");
      task_state->state_flag = BN_CLIENT_WILL_SEND_FIN;
    } else {
      task_state->state_flag = BN_CLIENT_WILL_SEND;
    }
    break;
  default:
    break;
  }
}

static void bn_cb_client_stream_recv(void *req, ucs_status_t status,
                              size_t len, void *arg) {
  UNUSED(len);
  bn_client_task_state_t *task_state = arg;
  if (status != UCS_OK) {
    fprintf(stderr, "client_stream_recv_cb: %s\n", ucs_status_string(status));
    task_state->finished = 1;
  }
  if (req != NULL) {
    ucp_request_free(req);
  }
  switch (task_state->state_flag) {
  case BN_CLIENT_DID_RECV:
    if (++task_state->count > endpoint_pingpong_max) {
      fprintf(stderr, "send FIN\n");
      task_state->state_flag = BN_CLIENT_WILL_SEND_FIN;
    } else {
      task_state->state_flag = BN_CLIENT_WILL_SEND;
    }
    break;
  case BN_CLIENT_DID_RECV_TAG:
    task_state->tag = *(uint64_t*)task_state->buffer;
    task_state->state_flag = BN_CLIENT_WILL_SEND;
  default:
    break;
  }
}

static void bn_client_cleanup_tasks(ucp_worker_h worker, size_t thread_idx) {
  // リソースの解放
  for (size_t i = 0; i < client_threads[thread_idx].task_count; ++i) {
    bn_client_task_state_t *task_state = &client_threads[thread_idx].tasks[i];
    if (task_state->ep != NULL) {
      if (!client_threads[thread_idx].tasks[i].ep_closed) {
        bn_close_ep(worker, task_state->ep);
      }
      fprintf(stderr, "ep_destroy_begin\n");
      ucp_ep_destroy(task_state->ep);
      fprintf(stderr, "ep_destroy_end\n");
      free(task_state->buffer);
    }
  }
}

static ucs_status_t bn_client_init_tasks(ucp_worker_h worker, size_t thread_idx,
                                         struct sockaddr *server_addr) {
  ucs_status_t status;
  // タスクの初期化
  for (size_t i = 0; i < client_threads[thread_idx].task_count; ++i) {
    bn_client_task_state_t *task_state = &client_threads[thread_idx].tasks[i];
    task_state->state_flag = BN_CLIENT_WILL_RECV_TAG;
    task_state->finished = 0;
    task_state->buffer =
        malloc(client_threads[thread_idx].tasks[i].buffer_size);
    strcpy((char *)task_state->buffer, "Hello World!");
    task_state->ep_closed = 0;
    ucp_request_param_t send_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_DATATYPE |
                        UCP_OP_ATTR_FIELD_USER_DATA,
        .datatype = ucp_dt_make_contig(1),
        .cb.send = bn_client_send_cb,
        .user_data = task_state,
    };

    ucp_request_param_t recv_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_DATATYPE |
                        UCP_OP_ATTR_FIELD_USER_DATA,
        .datatype = ucp_dt_make_contig(1),
        .cb.recv = bn_cb_client_recv,
        .user_data = task_state,
    };

    ucp_request_param_t stream_recv_params = {
      .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE | UCP_OP_ATTR_FIELD_USER_DATA,
      .datatype = ucp_dt_make_contig(1),
      .cb.recv_stream = bn_cb_client_stream_recv,
      .user_data = task_state,
    };
    task_state->send_params = send_params;
    task_state->recv_params = recv_params;
    task_state->stream_recv_params = stream_recv_params;
    ucp_ep_params_t ep_params = {
        .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR |
                      UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                      UCP_EP_PARAM_FIELD_ERR_HANDLER,
        .err_mode = UCP_ERR_HANDLING_MODE_PEER,
        .flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
        .err_handler.cb = bn_client_ep_error_handler,
        .err_handler.arg = &client_threads[thread_idx].tasks[i],
        .sockaddr.addr = server_addr,
        .sockaddr.addrlen = sizeof(struct sockaddr),
    };
    if ((status = ucp_ep_create(worker, &ep_params, &task_state->ep)) !=
        UCS_OK) {
      bn_client_cleanup_tasks(worker, thread_idx);
      return status;
    }
  }
  return status;
}

static void *bn_client_worker_main(void *arg) {
  bn_client_thread_ctx_t *ctx = arg;
  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };
  ucs_status_t status;
  ucp_worker_h worker;
  if ((status = ucp_worker_create(ctx->ctx, &worker_params, &worker))) {
    fprintf(stderr, "failed to create client worker: %s\n",
            ucs_status_string(status));
    goto err;
  }

  if ((status = bn_client_init_tasks(worker, ctx->client_thread_idx,
                                     ctx->server_addr)) != UCS_OK) {
    goto err_ep;
  }

  _Bool finished = 0;
  while (!finished) {
    finished = 1;
    for (size_t j = 0; j < client_threads[ctx->client_thread_idx].task_count;
         ++j) {
      bn_client_task_state_t *task_state =
          &client_threads[ctx->client_thread_idx].tasks[j];
      if (task_state->finished) {
        continue;
      }
      finished = 0;
      ucs_status_ptr_t status_ptr;
      switch (task_state->state_flag) {
      case BN_CLIENT_DID_RECV:
      case BN_CLIENT_DID_SEND:
      case BN_CLIENT_DID_SEND_FIN:
      case BN_CLIENT_DID_RECV_TAG:
        break;
      case BN_CLIENT_WILL_RECV_TAG:
        status_ptr = ucp_stream_recv_nbx(task_state->ep, task_state->buffer, 1,
                            &task_state->stream_received_size,
                            &task_state->stream_recv_params);
        if (status_ptr == NULL) {
          task_state->state_flag = BN_CLIENT_WILL_SEND;
        }
        else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "client recv tag from stream: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task_state->finished = 1;
          ucp_request_free(status_ptr);
        }
        else {
          task_state->state_flag = BN_CLIENT_DID_RECV_TAG;
        }
        break;

      // 終了処理
      case BN_CLIENT_WILL_SEND_FIN:
        memcpy(task_state->buffer, "FIN", 4);
        status_ptr = ucp_tag_send_nbx(task_state->ep, task_state->buffer,
                                      task_state->buffer_size, task_state->tag,
                                      &task_state->send_params);
        if (status_ptr == NULL) {
          task_state->state_flag = BN_CLIENT_WILL_RECV;
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "client tag_send_nbx (FIN): %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task_state->finished = 1;
          ucp_request_free(status_ptr);
        } else {
          task_state->state_flag = BN_CLIENT_DID_SEND_FIN;
        }
        break;
      case BN_CLIENT_WILL_SEND:
        status_ptr = ucp_tag_send_nbx(task_state->ep, task_state->buffer,
                                      task_state->buffer_size, task_state->tag,
                                      &task_state->send_params);
        if (status_ptr == NULL) {
          task_state->state_flag = BN_CLIENT_WILL_RECV;
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "client tag_send_nbx: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task_state->finished = 1;
          ucp_request_free(status_ptr);
        } else {
          task_state->state_flag = BN_CLIENT_DID_SEND;
        }
        break;
      case BN_CLIENT_WILL_RECV:
        status_ptr = ucp_tag_recv_nbx(worker, task_state->buffer,
                                      task_state->buffer_size, task_state->tag, 0,
                                      &task_state->recv_params);
        if (status_ptr == NULL) {
          if (++task_state->count > endpoint_pingpong_max) {
            fprintf(stderr, "send FIN\n");
            task_state->state_flag = BN_CLIENT_WILL_SEND_FIN;
          } else {
            task_state->state_flag = BN_CLIENT_WILL_SEND;
          }
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "client tag_recv_nbx: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task_state->finished = 1;
          ucp_request_free(status_ptr);
        } else {
          task_state->state_flag = BN_CLIENT_DID_RECV;
        }
        break;
      }
    }
    if (finished) {
      fprintf(stderr, "client finished\n");
      break;
    }
    ucp_worker_progress(worker);
  }
  bn_client_cleanup_tasks(worker, ctx->client_thread_idx);
err_ep:
  ucp_worker_destroy(worker);
  client_threads[ctx->client_thread_idx].finished = 1;
  free(arg);
err:
  return NULL;
}

// ただmallocした領域に情報詰めてpthreadを立ち上げるだけ
static void bn_spawn_client_worker(ucp_context_h ctx,
                                   struct sockaddr *server_addr,
                                   size_t client_thread_idx) {
  pthread_t thread;
  bn_client_thread_ctx_t *thread_ctx = malloc(sizeof(bn_client_thread_ctx_t));
  thread_ctx->client_thread_idx = client_thread_idx;
  thread_ctx->ctx = ctx;
  thread_ctx->server_addr = server_addr;

  pthread_create(&thread, 0, bn_client_worker_main, thread_ctx);
}

static ucs_status_t bn_run_client(ucp_context_h ctx,
                                  struct sockaddr *server_addr,
                                  size_t client_thread_count,
                                  size_t client_task_count,
                                  size_t buffer_size) {
  client_threads =
      malloc(sizeof(bn_client_thread_state_t) * client_thread_count);
  // 初期化
  for (size_t i = 0; i < client_thread_count; ++i) {
    client_threads[i].tasks =
        malloc(sizeof(bn_client_task_state_t) * client_task_count);
    client_threads[i].task_count = client_task_count;
    client_threads[i].finished = 0;
    // init all task states
    for (size_t j = 0; j < client_task_count; ++j) {
      client_threads[i].tasks[j].ep = NULL;
      client_threads[i].tasks[j].buffer_size = buffer_size;
      client_threads[i].tasks[j].buffer = NULL;
      client_threads[i].tasks[j].state_flag = BN_CLIENT_WILL_RECV_TAG;
      client_threads[i].tasks[j].count = 0;
    }
    bn_spawn_client_worker(ctx, server_addr, i);
  }

  _Bool yet_finished = 1;
  while (yet_finished) {
    // 一つでも終了していないスレッドがあれば終了しない
    yet_finished = 0;
    for (size_t i = 0; i < client_thread_count; ++i) {
      if (!client_threads[i].finished) {
        yet_finished = 1;
      }
    }
    // 適当...
    // このスレッドは眠ってても大丈夫なはず(各スレッド独立のworkerが実際の処理をするので)
    sleep(1);
  }

  fprintf(stderr, "all client finished\n");

  // 確保したリソースの削除
  for (size_t i = 0; i < client_thread_count; ++i) {
    free(client_threads[i].tasks);
  }
  return UCS_OK;
}

static size_t server_task_slot_preparation_count = 1024;

typedef enum bn_server_task_state_flag {
  BN_SERVER_WILL_SEND_TAG,
  BN_SERVER_DID_SEND_TAG,
  BN_SERVER_WILL_SEND,
  BN_SERVER_DID_SEND,
  BN_SERVER_WILL_RECV,
  BN_SERVER_DID_RECV,
} bn_server_task_state_flag_t;

typedef struct bn_server_task_state {
  ucp_ep_h ep;
  ucp_conn_request_h conn_req;
  bn_server_task_state_flag_t flag;
  uint8_t *buffer;
  size_t buffer_size;
  _Bool finished;
  _Bool ep_closed;
  uint64_t tag;
} bn_server_task_state_t;

typedef struct bn_server_worker_state {
  ucp_worker_h worker;
  bn_server_task_state_t *tasks;
  size_t task_count;
  _Bool finished;
} bn_server_worker_state_t;

bn_server_worker_state_t *server_threads;
static size_t global_server_thread_count = 0;
static size_t roundrobin = 0;

static void listener_conn_handler(ucp_conn_request_h req, void *arg) {
  UNUSED(arg);
  // conn_reqだけ入れておく。
  // TODOここアトミックにしないとダメそうfalseの場合は0かそれ以外なのでatomicでなくてもよかったが、
  // この場合はメモリ書き込みがAtomicになる保証がないので
  size_t thread_idx = (roundrobin++) % global_server_thread_count;
  for (size_t i = 0; i < server_threads[thread_idx].task_count; ++i) {
    bn_server_task_state_t *task = &server_threads[thread_idx].tasks[i];
    if (task->conn_req == NULL) {
      task->conn_req = req;
      break;
    }
  }
}

typedef struct bn_server_worker_arg {
  ucp_context_h ctx;
  size_t server_thread_idx;
  size_t buffer_size;
} server_thread_arg_t;

static void bn_cb_serer_ep_err(void *arg, ucp_ep_h ep, ucs_status_t status) {
  UNUSED(ep);
  UNUSED(status);
  bn_server_task_state_t *task = arg;
  task->ep_closed = 1;
  task->finished = 1;
}

static void bn_cb_server_recv(void *req, ucs_status_t status,
                              const ucp_tag_recv_info_t *recv_info,
                              void *user_data) {
  UNUSED(recv_info);
  bn_server_task_state_t *task = user_data;
  if (status != UCS_OK) {
    fprintf(stderr, "server_recv_cb: %s\n", ucs_status_string(status));
    task->finished = 1;
  } else {
    task->flag = BN_SERVER_WILL_SEND;
    if (memcmp(task->buffer, "FIN", 3) == 0) {
      task->finished = 1;
    }
  }
  ucp_request_free(req);
}

static void bn_cb_server_send(void *req, ucs_status_t status, void *user_data) {
  bn_server_task_state_t *task = user_data;
  if (status != UCS_OK) {
    fprintf(stderr, "server_recv_cb: %s\n", ucs_status_string(status));
    task->finished = 1;
  } else {
    task->flag = BN_SERVER_WILL_RECV;
  }
  ucp_request_free(req);
}

atomic_long io_counter = ATOMIC_VAR_INIT(0);

atomic_ulong tag = ATOMIC_VAR_INIT(1000);

static void *bn_server_worker_main(void *arg) {
  server_thread_arg_t *thread_arg = arg;
  bn_server_worker_state_t *thread =
      &server_threads[thread_arg->server_thread_idx];

  ucs_status_t status;

  size_t handled_request_count = 0;
  size_t yet_finished = 1;
  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };

  if ((status = ucp_worker_create(thread_arg->ctx, &worker_params,
                                  &thread->worker)) != UCS_OK) {
    goto err;
  }

  while (handled_request_count == 0 || yet_finished) {
    handled_request_count = 0;
    yet_finished = 0;
    for (size_t i = 0; i < thread->task_count; ++i) {
      bn_server_task_state_t *task = &thread->tasks[i];
      if (task->conn_req) {
        ++handled_request_count;
        if (task->ep == NULL) {
          ucp_ep_params_t ep_params = {
              .field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST |
                            UCP_EP_PARAM_FIELD_ERR_HANDLER,
              .err_handler.arg = task,
              .err_handler.cb = bn_cb_serer_ep_err,
              .conn_request = task->conn_req,
          };
          if ((status = ucp_ep_create(thread->worker, &ep_params, &task->ep)) !=
              UCS_OK) {
            fprintf(stderr, "server ep_create: %s\n",
                    ucs_status_string(status));
            thread->finished = 1;
            break;
          }
          task->ep_closed = 0;
          task->flag = BN_SERVER_WILL_SEND_TAG;
          task->buffer_size = thread_arg->buffer_size;
          task->buffer = malloc(thread_arg->buffer_size);
        }
      }

      if (task->finished) {
        continue;
      } else if (task->ep == NULL) {
        break;
      } else {
        yet_finished = 1;
      }

      ucs_status_ptr_t status_ptr;
      ucp_request_param_t recv_params = {
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA,
          .datatype = ucp_dt_make_contig(1),
          .cb.recv = bn_cb_server_recv,
          .user_data = task,
      };

      ucp_request_param_t send_params = {
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA,
          .datatype = ucp_dt_make_contig(1),
          .cb.send = bn_cb_server_send,
          .user_data = task,
      };

      switch (task->flag) {
      case BN_SERVER_DID_RECV:
      case BN_SERVER_DID_SEND:
      case BN_SERVER_DID_SEND_TAG:
        break;
      case BN_SERVER_WILL_SEND_TAG:
        task->tag = atomic_fetch_add(&tag, 1);
        status_ptr = ucp_stream_send_nbx(task->ep, task->buffer, sizeof(uint64_t), &send_params);
        if (status_ptr == NULL) {
          task->flag = BN_SERVER_WILL_RECV;
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "server tag_recv_nbx: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task->finished = 1;
          task->ep_closed = 1;
          ucp_request_free(status_ptr);
        } else {
          task->flag = BN_SERVER_DID_SEND_TAG;
        }
        break;
      case BN_SERVER_WILL_RECV:
        status_ptr = ucp_tag_recv_nbx(thread->worker, task->buffer,
                                      task->buffer_size, task->tag, 0, &recv_params);
        if (status_ptr == NULL) {
          task->flag = BN_SERVER_WILL_SEND;
          if (memcmp(task->buffer, "FIN", 3) == 0) {
            task->finished = 1;
          }
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "server tag_recv_nbx: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task->finished = 1;
          task->ep_closed = 1;
          ucp_request_free(status_ptr);
        } else {
          task->flag = BN_SERVER_DID_RECV;
        }
        atomic_fetch_add(&io_counter, 1);
        break;
      case BN_SERVER_WILL_SEND:
        status_ptr = ucp_tag_send_nbx(task->ep, task->buffer, task->buffer_size,
                                      task->tag, &send_params);
        if (status_ptr == NULL) {
          task->flag = BN_SERVER_WILL_RECV;
        } else if (UCS_PTR_IS_ERR(status_ptr)) {
          fprintf(stderr, "server tag_recv_nbx: %s\n",
                  ucs_status_string(UCS_PTR_STATUS(status_ptr)));
          task->finished = 1;
          task->ep_closed = 1;
          ucp_request_free(status_ptr);
        } else {
          task->flag = BN_SERVER_DID_SEND;
        }
        break;
      }
    }
    ucp_worker_progress(thread->worker);
  }
  fprintf(stderr, "handle_count: %ld, yet_finished: %ld\n",
          handled_request_count, yet_finished);
  for (size_t i = 0; i < thread->task_count; ++i) {
    bn_server_task_state_t *task = &thread->tasks[i];
    if (task->ep && !task->ep_closed) {
      bn_close_ep(thread->worker, task->ep);
      ucp_ep_destroy(task->ep);
    }
  }
  fprintf(stderr, "server_exited\n");
  thread->finished = 1;
  ucp_worker_destroy(thread->worker);
err:
  free(arg);
  return NULL;
}

static ucs_status_t bn_spawn_server_worker(ucp_context_h ctx,
                                           size_t server_thread_idx,
                                           size_t buffer_size) {
  server_thread_arg_t *arg = malloc(sizeof(server_thread_arg_t));
  arg->server_thread_idx = server_thread_idx;
  arg->ctx = ctx;
  arg->buffer_size = buffer_size;
  pthread_t thread;
  pthread_create(&thread, 0, bn_server_worker_main, arg);
  return UCS_OK;
}

void *io_count_summarizer(void *arg) {
  UNUSED(arg);
  for (;;) {
    sleep(1);
    uint64_t counter = atomic_exchange(&io_counter, 0);
    fprintf(stderr, "iops=%ld\n", counter);
  }
  return NULL;
}

static ucs_status_t bn_run_server(ucp_context_h ctx,
                                  struct sockaddr server_addr,
                                  size_t server_thread_count,
                                  size_t buffer_size) {

  server_threads =
      malloc(sizeof(bn_server_worker_state_t) * server_thread_count);
  global_server_thread_count = server_thread_count;

  ucs_status_t status;
  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };
  ucp_worker_h worker;

  pthread_t thread;
  pthread_create(&thread, 0, io_count_summarizer, NULL);

  if ((status = ucp_worker_create(ctx, &worker_params, &worker)) != UCS_OK) {
    fprintf(stderr, "init ctx: (%s)\n", ucs_status_string(status));
    goto err;
  }

  for (size_t i = 0; i < server_thread_count; ++i) {
    server_threads[i].finished = 0;
    server_threads[i].tasks = malloc(sizeof(bn_server_task_state_t) *
                                     server_task_slot_preparation_count);
    server_threads[i].task_count = server_task_slot_preparation_count;
    for (size_t j = 0; j < server_task_slot_preparation_count; ++j) {
      server_threads[i].tasks[j].finished = 0;
      server_threads[i].tasks[j].ep = NULL;
      server_threads[i].tasks[j].conn_req = NULL;
    }
    bn_spawn_server_worker(ctx, i, buffer_size);
  }

  ucp_listener_h listener;
  ucp_listener_params_t listener_params = {
      .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                    UCP_LISTENER_PARAM_FIELD_CONN_HANDLER,
      .sockaddr.addr = &server_addr,
      .sockaddr.addrlen = sizeof(struct sockaddr),
      .conn_handler.arg = NULL,
      .conn_handler.cb = listener_conn_handler,
  };
  if ((status = ucp_listener_create(worker, &listener_params, &listener)) !=
      UCS_OK) {
    fprintf(stderr, "create listener (%s)\n", ucs_status_string(status));
    goto err_listener;
  }
  fprintf(stderr, "waiting connection...\n");
  _Bool yet_finished = 1;
  while (yet_finished) {
    yet_finished = 0;
    for (size_t i = 0; i < server_thread_count; ++i) {
      if (!server_threads[i].finished) {
        yet_finished = 1;
      }
    }
    ucp_worker_progress(worker);
  }
  ucp_listener_destroy(listener);
err_listener:
  ucp_worker_destroy(worker);
err:
  return status;
}

static ucs_status_t bn_init_ucx(ucp_context_h *ctx, ucp_worker_h *worker) {
  ucp_params_t params = {
      .field_mask =
          UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED,
      .features = UCP_FEATURE_RMA | UCP_FEATURE_TAG | UCP_FEATURE_STREAM |
                  UCP_FEATURE_WAKEUP,
      .mt_workers_shared = 1,
  };

  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };

  ucs_status_t status;
  ucp_config_t *config;

  if ((status = ucp_config_read(NULL, NULL, &config)) != UCS_OK) {
    fprintf(stderr, "read config: (%s)\n", ucs_status_string(status));
    goto err;
  }

  if ((status = ucp_init_version(1, 14, &params, config, ctx)) != UCS_OK) {
    fprintf(stderr, "init ctx: (%s)\n", ucs_status_string(status));
    goto err;
  }

  if ((status = ucp_worker_create(*ctx, &worker_params, worker)) != UCS_OK) {
    fprintf(stderr, "init ctx: (%s)\n", ucs_status_string(status));
    goto err_worker;
  }

  return UCS_OK;

err_worker:
  ucp_config_release(config);
  ucp_cleanup(*ctx);
err:
  return status;
}

int main(int argc, char *argv[]) {
  int nprocs, rank;
  struct sockaddr server_addr;
  MPI_Status status;
  ucp_context_h *ctx = malloc(sizeof(ucp_context_h));
  ucp_worker_h worker;

  char *server_thread_count_s = getenv("SERVER_THREAD_COUNT");
  if (server_thread_count_s == NULL) {
    fprintf(stderr, "SERVER_THREAD_COUNT required\n");
    return -1;
  }
  int server_thread_count = strtol(server_thread_count_s, NULL, 10);
  if (errno != 0) {
    fprintf(stderr, "invalid SERVER_THREAD_COUNT\n");
    return -1;
  }

  char *client_thread_count_s = getenv("CLIENT_THREAD_COUNT");
  if (client_thread_count_s == NULL) {
    fprintf(stderr, "CLIENT_THREAD_COUNT required\n");
    return -1;
  }
  int client_thread_count = strtol(client_thread_count_s, NULL, 10);
  if (errno != 0) {
    fprintf(stderr, "invalid CLIENT_THREAD_COUNT\n");
    return -1;
  }

  char *client_task_count_s = getenv("CLIENT_TASK_COUNT");
  if (client_task_count_s == NULL) {
    fprintf(stderr, "CLIENT_TASK_COUNT required\n");
    return -1;
  }
  int client_task_count = strtol(client_task_count_s, NULL, 10);
  if (errno != 0) {
    fprintf(stderr, "invalid CLIENT_TASK_COUNT\n");
    return -1;
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("rank: %d, size: %d\n", rank, nprocs);

  if (rank == 0) {
    // とりあえずIPv4だけ考える
    bn_get_my_addr(&server_addr);

    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    MPI_Send(&server_addr, sizeof(server_addr), MPI_BYTE, 1, 99,
             MPI_COMM_WORLD);
    printf("sent server addr: %s\n", inet_ntoa(i->sin_addr));
    if (bn_init_ucx(ctx, &worker) != UCS_OK) {
      return 1;
    }
    if (bn_run_server(*ctx, server_addr, server_thread_count,
                      sizeof(test_message)) != UCS_OK) {
      return 1;
    }
  } else {
    // とりあえずIPv4だけ考える
    MPI_Recv(&server_addr, sizeof(struct sockaddr), MPI_BYTE, 0, 99,
             MPI_COMM_WORLD, &status);
    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    printf("got server addr: %s\n", inet_ntoa(i->sin_addr));
    if (bn_init_ucx(ctx, &worker) != UCS_OK) {
      return 1;
    }
    sleep(1);
    if (bn_run_client(*ctx, &server_addr, client_thread_count,
                      client_task_count, sizeof(test_message)) != UCS_OK) {
      return 1;
    }
  }

  ucp_worker_destroy(worker);
  ucp_cleanup(*ctx);

  return MPI_Finalize();
}
