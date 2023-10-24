#include <alloca.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <mpi.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

static uint16_t listen_port = 10082;
static _Bool global_close = 0;

int get_my_addr(struct sockaddr *if_addr_buf) {
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

static ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request) {
  ucs_status_t status;
  /* if operation was completed immediately */
  if (request == NULL) {
    return UCS_OK;
  }
  if (UCS_PTR_IS_ERR(request)) {
    return UCS_PTR_STATUS(request);
  }
  while ((status = ucp_request_check_status(request)) == UCS_INPROGRESS) {
    ucp_worker_progress(ucp_worker);
  }
  ucp_request_free(request);
  return status;
}

static ucs_status_t close_ep(ucp_worker_h worker, ucp_ep_h ep) {
  ucp_request_param_t params = {
      .op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS,
      .flags = UCP_EP_CLOSE_MODE_FLUSH,
  };
  ucs_status_ptr_t close_req = ucp_ep_close_nbx(ep, &params);
  ucs_status_t status;
  if ((status = request_wait(worker, close_req)) != UCS_OK) {
    fprintf(stderr, "failed to close ep (%s)\n", ucs_status_string(status));
  };
  return status;
}

static void common_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
  UNUSED(ep);
  printf("%s\n error handling callback was invoked with status %d (%s)\n",
         (char *)arg, status, ucs_status_string(status));
}

typedef struct conn_req_ctx {
  ucp_context_h ctx;
} conn_req_ctx_t;

static void server_recv_callback(void *req, ucs_status_t status,
                                 const ucp_tag_recv_info_t *tag_recv_info,
                                 void *user_data) {
  UNUSED(req);
  UNUSED(status);
  UNUSED(tag_recv_info);
  _Bool *completed = user_data;
  *completed = 1;
}

static void client_send_callback(void *req, ucs_status_t status,
                                 void *user_data) {
  UNUSED(req);
  UNUSED(status);
  _Bool *completed = user_data;
  *completed = 1;
  fprintf(stderr, "sent data from client to server\n");
}

const char test_message[] = "Hello World!";

static ucs_status_t client_server_do_work(ucp_worker_h worker, ucp_ep_h ep,
                                          _Bool is_server) {
  ucs_status_t status;
  _Bool completed = 0;
  ucp_dt_iov_t *iov = alloca(1 * sizeof(ucp_dt_iov_t));
  memset(iov, 0, 1 * sizeof(ucp_dt_iov_t));
  void *buffer = malloc(sizeof(test_message));
  if (is_server) {
    fprintf(stderr, "server\n");
    ucp_request_param_t recv_req_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA |
                        UCP_OP_ATTR_FIELD_DATATYPE,
        .user_data = &completed,
        .cb.recv = server_recv_callback,
        .datatype = ucp_dt_make_contig(1),
    };
    ucp_request_param_t send_req_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA |
                        UCP_OP_ATTR_FIELD_DATATYPE,
        .user_data = &completed,
        .cb.send = client_send_callback,
        .datatype = ucp_dt_make_contig(1),
    };
    for (size_t i = 0; i < 50; ++i) {
      for (size_t j = 0; j < 100000; ++j) {
        ucs_status_ptr_t recv_req = ucp_tag_recv_nbx(
            worker, buffer, sizeof(test_message), 99, 0, &recv_req_params);
        if ((status = request_wait(worker, recv_req)) != UCS_OK) {
          fprintf(stderr, "failed to close ep (%s)\n",
                  ucs_status_string(status));
        };
        ucs_status_ptr_t send_req = ucp_tag_send_nbx(
            ep, buffer, sizeof(test_message), 99, &send_req_params);
        if ((status = request_wait(worker, send_req)) != UCS_OK) {
          fprintf(stderr, "send tag nbx %s\n", ucs_status_string(status));
          return status;
        }
      }
      ucp_ep_print_info(ep, stderr);
    }
    global_close = 1;
  } else {
    fprintf(stderr, "client\n");
    memcpy(buffer, test_message, sizeof(test_message));
    ucp_request_param_t send_req_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA |
                        UCP_OP_ATTR_FIELD_DATATYPE,
        .user_data = &completed,
        .cb.send = client_send_callback,
        .datatype = ucp_dt_make_contig(1),
    };
    ucp_request_param_t recv_req_params = {
        .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                        UCP_OP_ATTR_FIELD_USER_DATA |
                        UCP_OP_ATTR_FIELD_DATATYPE,
        .user_data = &completed,
        .cb.recv = server_recv_callback,
        .datatype = ucp_dt_make_contig(1),
    };
    for (size_t i = 0; i < 50; ++i) {
      struct timespec before, after;
      clock_gettime(CLOCK_REALTIME, &before);
      for (size_t j = 0; j < 100000; ++j) {
        ucs_status_ptr_t send_req = ucp_tag_send_nbx(
            ep, buffer, sizeof(test_message), 99, &send_req_params);
        if ((status = request_wait(worker, send_req)) != UCS_OK) {
          fprintf(stderr, "send tag nbx %s\n", ucs_status_string(status));
          return status;
        }
        ucs_status_ptr_t recv_req = ucp_tag_recv_nbx(
            worker, buffer, sizeof(test_message), 99, 0, &recv_req_params);
        if ((status = request_wait(worker, recv_req)) != UCS_OK) {
          fprintf(stderr, "failed to close ep (%s)\n",
                  ucs_status_string(status));
        };
      }
      clock_gettime(CLOCK_REALTIME, &after);
      uint64_t elapsed_nsecs = (after.tv_sec - before.tv_sec) * 1000000000 +
                               (after.tv_nsec - before.tv_nsec);
      double elapsed_ns = elapsed_nsecs;
      double iops = 100000.0 / elapsed_ns * 1000.0 * 1000.0 * 1000.0;
      fprintf(stderr, "iops=%f\n", iops);
      // ucp_ep_print_info(ep, stderr);
    }
    fprintf(stderr, "sent completed\n");
  }
  return UCS_OK;
}

static ucs_status_t run_client(ucp_worker_h worker,
                               struct sockaddr *server_addr) {
  ucs_status_t status;
  ucp_ep_params_t ep_params = {
      .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR |
                    UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE,
      .err_mode = UCP_ERR_HANDLING_MODE_PEER,
      .flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
      .sockaddr.addr = server_addr,
      .sockaddr.addrlen = sizeof(struct sockaddr),
  };
  ucp_ep_h ep;
  size_t retry_count = 0;
  do {
    fprintf(stderr, "retry %ld\n", retry_count);
    status = ucp_ep_create(worker, &ep_params, &ep);
    ++retry_count;
  } while (status != UCS_OK && retry_count < 10);
  // ucp_ep_print_info(ep, stderr);
  if (status != UCS_OK) {
    fprintf(stderr, "client ep creation (%s)\n", ucs_status_string(status));
    goto err;
  }
  if ((status = client_server_do_work(worker, ep, 0)) != UCS_OK) {
    goto err_ep;
  }
err_ep:
  close_ep(worker, ep);
  ucp_ep_destroy(ep);
err:
  return status;
}

typedef struct worker_thread_param {
  ucp_context_h ctx;
  ucp_conn_request_h req;
} worker_thread_param_t;

void *spawn_worker(void *params) {
  worker_thread_param_t *wt_params = params;

  ucp_ep_params_t ep_params = {
      .field_mask =
          UCP_EP_PARAM_FIELD_CONN_REQUEST | UCP_EP_PARAM_FIELD_ERR_HANDLER,
      .err_handler.arg = "conn_handler",
      .err_handler.cb = common_err_cb,
      .conn_request = wt_params->req,
  };

  ucs_status_t status;
  ucp_worker_h worker;
  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };
  fprintf(stderr, "worker_spawned\n");
  ucp_worker_create(wt_params->ctx, &worker_params, &worker);
  ucp_worker_print_info(worker, stderr);
  fprintf(stderr, "worker created\n");
  ucp_ep_h ep;
  if ((status = ucp_ep_create(worker, &ep_params, &ep)) != UCS_OK) {
    fprintf(stderr, "create ep (%s)\n", ucs_status_string(status));
  }
  ucp_ep_print_info(ep, stderr);

  status = client_server_do_work(worker, ep, 1);
  ucp_ep_destroy(ep);
  ucp_worker_destroy(worker);
  return NULL;
}

static void conn_handler(ucp_conn_request_h req, void *arg) {
  conn_req_ctx_t *ctx = arg;
  fprintf(stderr, "conn_handler\n");

  worker_thread_param_t *wt_param = malloc(sizeof(worker_thread_param_t));
  wt_param->ctx = ctx->ctx;
  wt_param->req = req;

  pthread_t thread;
  pthread_create(&thread, NULL, spawn_worker, wt_param);
}

static ucs_status_t run_server(conn_req_ctx_t *conn_req_ctx,
                               ucp_worker_h worker,
                               struct sockaddr server_addr) {
  ucp_listener_h listener;
  ucp_listener_params_t listener_params = {
      .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                    UCP_LISTENER_PARAM_FIELD_CONN_HANDLER,
      .sockaddr.addr = &server_addr,
      .sockaddr.addrlen = sizeof(struct sockaddr),
      .conn_handler.arg = conn_req_ctx,
      .conn_handler.cb = conn_handler,
  };
  ucs_status_t status;
  if ((status = ucp_listener_create(worker, &listener_params, &listener)) !=
      UCS_OK) {
    fprintf(stderr, "create listener (%s)\n", ucs_status_string(status));
    goto err;
  }
  fprintf(stderr, "waiting connection...\n");
  while (!global_close) {
    ucp_worker_progress(worker);
  }
  ucp_listener_destroy(listener);
err:
  return status;
}

static ucs_status_t init_ucx(ucp_context_h *ctx, ucp_worker_h *worker) {
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

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("rank: %d, size: %d\n", rank, nprocs);

  if (rank == 0) {
    // とりあえずIPv4だけ考える
    get_my_addr(&server_addr);

    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    MPI_Send(&server_addr, sizeof(server_addr), MPI_BYTE, 1, 99,
             MPI_COMM_WORLD);
    printf("sent server addr: %s\n", inet_ntoa(i->sin_addr));
    if (init_ucx(ctx, &worker) != UCS_OK) {
      return 1;
    }
    conn_req_ctx_t *conn_req_ctx = malloc(sizeof(conn_req_ctx_t));
    conn_req_ctx->ctx = *ctx;
    if (run_server(conn_req_ctx, worker, server_addr) != UCS_OK) {
      free(conn_req_ctx);
      return 1;
    }
    free(conn_req_ctx);
  } else {
    // とりあえずIPv4だけ考える
    MPI_Recv(&server_addr, sizeof(struct sockaddr), MPI_BYTE, 0, 99,
             MPI_COMM_WORLD, &status);
    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    printf("got server addr: %s\n", inet_ntoa(i->sin_addr));
    if (init_ucx(ctx, &worker) != UCS_OK) {
      return 1;
    }
    sleep(3);
    if (run_client(worker, &server_addr) != UCS_OK) {
      return 1;
    }
  }

  ucp_worker_destroy(worker);
  ucp_cleanup(*ctx);

  return MPI_Finalize();
}
