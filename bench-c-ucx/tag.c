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

char test_message[] = "Hello World!";

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

_Bool read_env_var_as_int(char *name, int *var) {
  char *server_thread_count_s = getenv(name);
  if (server_thread_count_s == NULL) {
    fprintf(stderr, "%s required\n", name);
    return 0;
  }
  *var = strtol(server_thread_count_s, NULL, 10);
  if (errno != 0) {
    fprintf(stderr, "invalid %s\n", name);
    return 0;
  }
  return 1;
}

typedef struct bn_conn_req_stack {
  ucp_conn_request_h *arr;
  int64_t current;
  int64_t len;
  atomic_flag lock;
} bn_conn_req_stack_t;

void bn_conn_req_stack_push(bn_conn_req_stack_t *stack,
                            ucp_conn_request_h data) {
  while (
      atomic_flag_test_and_set_explicit(&stack->lock, memory_order_acquire)) {
    __builtin_ia32_pause();
  }

  if (stack->current + 1 == stack->len) {
    ucp_conn_request_h *new_arr =
        malloc(sizeof(ucp_conn_request_h) * stack->len * 2);
    memcpy(new_arr, stack->arr, sizeof(ucp_conn_request_h) * stack->len);
    stack->len *= 2;
    stack->arr = new_arr;
  }

  stack->arr[++stack->current] = data;

  atomic_flag_clear_explicit(&stack->lock, memory_order_release);
}

_Bool bn_conn_req_stack_pop(bn_conn_req_stack_t *stack,
                            ucp_conn_request_h *data) {
  while (
      atomic_flag_test_and_set_explicit(&stack->lock, memory_order_acquire)) {
    __builtin_ia32_pause();
  }

  if (stack->current <= 0) {
    return 0;
  }

  memcpy(data, stack->arr[stack->current--], sizeof(ucp_conn_request_h) * 1);

  atomic_flag_clear_explicit(&stack->lock, memory_order_release);
  return 1;
}

typedef struct bn_listener_context {
  bn_conn_req_stack_t *stack;
  size_t thread_count;
  size_t current_thread_idx;
} bn_listener_context_t;

static bn_listener_context_t create_listener_context(size_t server_count) {
  bn_listener_context_t ctx = {
      .stack = malloc(sizeof(bn_listener_context_t) * server_count),
      .thread_count = server_count,
      .current_thread_idx = 0,
  };
  for (size_t i = 0; i < server_count; ++i) {
    atomic_flag flag = ATOMIC_FLAG_INIT;
    ctx.stack[i].arr = malloc(sizeof(ucp_conn_request_h) * 16);
    ctx.stack[i].current = 0;
    ctx.stack[i].len = 16;
    ctx.stack[i].lock = flag;
  }
  return ctx;
}

static void free_listener_context(bn_listener_context_t ctx) {
  for (size_t i = 0; i < ctx.thread_count; ++i) {
    free(ctx.stack[i].arr);
  }
  free(ctx.stack);
}

static void bn_conn_handler(ucp_conn_request_h conn_req, void *raw_ctx) {
  bn_listener_context_t *ctx = raw_ctx;
  bn_conn_req_stack_push(&ctx->stack[ctx->current_thread_idx], conn_req);
  ctx->current_thread_idx = (ctx->current_thread_idx + 1) % ctx->thread_count;
}

typedef struct bn_server_worker_arg {
  size_t thread_idx;
  atomic_bool finished;
  bn_conn_req_stack_t *stack;
  ucp_context_h ctx;
  size_t required_ep_closing_count;
  size_t buffer_size;
} bn_server_worker_arg_t;

typedef enum bn_server_task_state_flag {
  BN_SERVER_START_INIT,
  BN_SERVER_FINISH_INIT,
  BN_SERVER_START_RECV,
  BN_SERVER_FINISH_RECV,
  BN_SERVER_START_SEND,
  BN_SERVER_FINISH_SEND,
} bn_server_task_state_flag_t;

typedef struct bn_server_task_state {
  ucp_worker_h worker;
  ucp_ep_h ep;
  void *buffer;
  size_t buffer_size;
  ucp_request_param_t init_stream_send_params;
  ucp_request_param_t recv_params;
  ucp_request_param_t send_params;
  uint64_t tag;
  size_t *close_count;
  size_t *wait_count;
  bn_server_task_state_flag_t flag;
} bn_server_task_state_t;

static void bn_server_ep_close_cb(void *req, ucs_status_t status,
                                  void *raw_task) {
  bn_server_task_state_t *task = raw_task;
  *task->wait_count = *task->wait_count - 1;
  if (status != UCS_OK) {
    fprintf(stderr, "server: cb of ep_close_nbx: %s\n",
            ucs_status_string(status));
  }
  free(task->buffer);
  free(task);
  ucp_request_free(req);
}

static void bn_server_ep_err_handler(void *raw_ctx, ucp_ep_h ep,
                                     ucs_status_t status) {
  UNUSED(ep);
  bn_server_task_state_t *task = raw_ctx;
  *task->close_count = *task->close_count + 1;
  ucp_request_param_t params = {
      .cb.send = bn_server_ep_close_cb,
      .user_data = task,
  };
  ucs_status_ptr_t status_ptr;
  status_ptr = ucp_ep_close_nbx(task->ep, &params);
  if (status_ptr == NULL) {
    free(task->buffer);
    free(task);
    return;
  } else if (UCS_PTR_IS_ERR(status)) {
    fprintf(stderr, "server: ep_close_nbx: %s\n",
            ucs_status_string(UCS_PTR_STATUS(status_ptr)));
    ucp_request_free(status_ptr);
    return;
  } else {
    *task->wait_count = *task->wait_count + 1;
  }
}


static void bn_server_task_coroutine(bn_server_task_state_t *task);

static void bn_server_initial_send_cb(void *req, ucs_status_t status,
                                      void *raw_task) {
  bn_server_task_state_t *task = raw_task;
  if (status == UCS_OK) {
    bn_server_task_coroutine(task);
  }
  else {
    fprintf(stderr, "server: init failed: %s\n", ucs_status_string(status));
    bn_server_ep_err_handler(task, task->ep, status);
  }
  ucp_request_free(req);
}

static void bn_server_recv_cb(void *req, ucs_status_t status,
                              const ucp_tag_recv_info_t *recv_info,
                              void *raw_task) {
                              UNUSED(recv_info);
  bn_server_task_state_t *task = raw_task;
  if (status == UCS_OK) {
    bn_server_task_coroutine(task);
  }
  else {
    fprintf(stderr, "server: recv failed: %s\n", ucs_status_string(status));
    bn_server_ep_err_handler(task, task->ep, status);
  }
  ucp_request_free(req);
}

static void bn_server_send_cb(void *req, ucs_status_t status, void *raw_task) {
  bn_server_task_state_t *task = raw_task;
  if (status == UCS_OK) {
    bn_server_task_coroutine(task);
  }
  else {
    fprintf(stderr, "server: send failed: %s\n", ucs_status_string(status));
    bn_server_ep_err_handler(task, task->ep, status);
  }
  ucp_request_free(req);
}

static void bn_server_task_coroutine(bn_server_task_state_t *task) {
  for (;;) {
    ucs_status_ptr_t status_ptr = NULL;
    switch (task->flag) {
    case BN_SERVER_START_INIT:
      status_ptr =
          ucp_stream_send_nbx(task->ep, task->buffer, task->buffer_size,
                              &task->init_stream_send_params);
      task->flag = BN_SERVER_FINISH_INIT;
      break;

    case BN_SERVER_FINISH_INIT:
      task->flag = BN_SERVER_START_RECV;
      break;

    case BN_SERVER_START_RECV:
      status_ptr =
          ucp_tag_recv_nbx(task->worker, task->buffer, task->buffer_size,
                           task->tag, 0, &task->recv_params);
      task->flag = BN_SERVER_FINISH_RECV;
      break;

    case BN_SERVER_FINISH_RECV:
      task->flag = BN_SERVER_START_SEND;
      break;

    case BN_SERVER_START_SEND:
      status_ptr = ucp_tag_send_nbx(task->ep, task->buffer, task->buffer_size,
                                    task->tag, &task->send_params);
      task->flag = BN_SERVER_FINISH_SEND;
      break;

    case BN_SERVER_FINISH_SEND:
      task->flag = BN_SERVER_START_RECV;
      break;
    }

    if (UCS_PTR_IS_ERR(status_ptr)) {
      fprintf(stderr, "server: send initial data: %s\n",
              ucs_status_string(UCS_PTR_STATUS(status_ptr)));
      bn_server_ep_err_handler(task, task->ep, UCS_PTR_STATUS(status_ptr));
      ucp_request_free(status_ptr);
      return;
    } else if (status_ptr != NULL) {
      // PENDING
      return;
    }
  }
}

static void *bn_server_worker_main(void *raw_arg) {
  bn_server_worker_arg_t *arg = raw_arg;
  ucp_worker_h worker;

  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };

  size_t closed_ep_count = 0;
  size_t wait_count = 0;
  uint64_t next_tag = 0;

  ucp_worker_create(arg->ctx, &worker_params, &worker);

  while (closed_ep_count < arg->required_ep_closing_count || wait_count > 0) {
    ucp_conn_request_h req;
    while (bn_conn_req_stack_pop(arg->stack, &req)) {
      bn_server_task_state_t *task = malloc(sizeof(bn_server_task_state_t));
      task->buffer = malloc(arg->buffer_size);
      task->buffer_size = arg->buffer_size;
      task->close_count = &closed_ep_count;
      task->wait_count = &wait_count;
      task->worker = worker;
      task->tag = next_tag++;
      task->flag = BN_SERVER_START_INIT;

      ucp_request_param_t initial_stream_send_params = {
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA,
          .datatype = ucp_dt_make_contig(1),
          .user_data = task,
          .cb.send = bn_server_initial_send_cb,
      };
      task->init_stream_send_params = initial_stream_send_params;

      ucp_request_param_t recv_params = {
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA,
          .datatype = ucp_dt_make_contig(1),
          .user_data = task,
          .cb.recv = bn_server_recv_cb,
      };
      task->recv_params = recv_params;

      ucp_request_param_t send_params = {
          .op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA,
          .datatype = ucp_dt_make_contig(1),
          .user_data = task,
          .cb.send = bn_server_send_cb,
      };
      task->recv_params = send_params;

      ucp_ep_params_t ep_params = {
          .field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST |
                        UCP_EP_PARAM_FIELD_ERR_HANDLER |
                        UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                        UCP_EP_PARAM_FIELD_USER_DATA,
          .conn_request = req,
          .user_data = task,
          .err_mode = UCP_ERR_HANDLING_MODE_PEER,
          .err_handler.arg = task,
          .err_handler.cb = bn_server_ep_err_handler,
      };
      ucp_ep_create(worker, &ep_params, &task->ep);
      bn_server_task_coroutine(task);
    }
    ucp_worker_progress(worker);
  }
  atomic_store(&arg->finished, 1);
  ucp_worker_destroy(worker);
  return NULL;
}

static ucs_status_t bn_server_run(ucp_context_h ctx, struct sockaddr *addr,
                                  size_t server_thread_count,
                                  size_t buffer_size) {
  ucp_worker_h worker;
  ucp_worker_params_t worker_params = {
      .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
      .thread_mode = UCS_THREAD_MODE_SINGLE,
  };
  ucp_worker_create(ctx, &worker_params, &worker);

  bn_listener_context_t listener_ctx =
      create_listener_context(server_thread_count);
  ucp_listener_h listener;
  ucp_listener_params_t listener_params = {
      .field_mask = UCP_LISTENER_PARAM_FIELD_CONN_HANDLER |
                    UCP_LISTENER_PARAM_FIELD_SOCK_ADDR,
      .sockaddr.addr = addr,
      .sockaddr.addrlen = sizeof(struct sockaddr),
      .conn_handler.cb = bn_conn_handler,
      .conn_handler.arg = &listener_ctx,
  };

  ucp_listener_create(worker, &listener_params, &listener);
  bn_server_worker_arg_t *args =
      malloc(sizeof(bn_server_worker_arg_t) * server_thread_count);
  for (size_t i = 0; i < server_thread_count; ++i) {
    atomic_bool flag = ATOMIC_VAR_INIT(0);

    args[i].thread_idx = i;
    args[i].finished = flag;
    args[i].stack = &listener_ctx.stack[i];
    args[i].ctx = ctx;
    // TODO: fix
    args[i].required_ep_closing_count = 1;
    args[i].buffer_size = buffer_size;

    pthread_t thread;
    pthread_create(&thread, 0, bn_server_worker_main, &args[i]);
  }

  _Bool all_finished = 0;

  while (!all_finished) {
    all_finished = 1;
    for (size_t i = 0; i < server_thread_count; ++i) {
      if (!atomic_load(&args[i].finished)) {
        all_finished = 0;
      }
    }
  }

  free_listener_context(listener_ctx);
  ucp_listener_destroy(listener);
  ucp_worker_destroy(worker);

  free(args);

  return UCS_OK;
}

static ucs_status_t bn_client_run(ucp_context_h ctx, struct sockaddr *addr,
                                  size_t client_thread_count,
                                  size_t client_task_count,
                                  size_t buffer_size) {
  return UCS_OK;
}

int main(int argc, char *argv[]) {
  int nprocs, rank;
  struct sockaddr server_addr;
  MPI_Status status;
  ucp_context_h ctx;
  ucp_config_t *config;

  int server_thread_count, client_thread_count, client_task_count;

  if (!read_env_var_as_int("SERVER_THREAD_COUNT", &server_thread_count)) {
    return -1;
  }
  if (!read_env_var_as_int("CLIENT_THREAD_COUNT", &client_thread_count)) {
    return -1;
  }
  if (!read_env_var_as_int("CLIENT_TASK_COUNT", &client_task_count)) {
    return -1;
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("rank: %d, size: %d\n", rank, nprocs);

  ucp_params_t params = {
      .field_mask =
          UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED,
      .features = UCP_FEATURE_TAG | UCP_FEATURE_STREAM,
      .mt_workers_shared = 1,
  };

  if (ucp_config_read(NULL, NULL, &config) != UCS_OK) {
    goto err;
  }
  if (ucp_init(&params, config, &ctx) != UCS_OK) {
    goto err;
  }

  if (rank == 0) {
    // とりあえずIPv4だけ考える
    bn_get_my_addr(&server_addr);

    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    MPI_Send(&server_addr, sizeof(server_addr), MPI_BYTE, 1, 99,
             MPI_COMM_WORLD);
    printf("sent server addr: %s\n", inet_ntoa(i->sin_addr));
    if (bn_server_run(ctx, &server_addr, server_thread_count,
                      sizeof(test_message)) != UCS_OK) {
      goto clean_ucx;
    }
  } else {
    // とりあえずIPv4だけ考える
    MPI_Recv(&server_addr, sizeof(struct sockaddr), MPI_BYTE, 0, 99,
             MPI_COMM_WORLD, &status);
    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    printf("got server addr: %s\n", inet_ntoa(i->sin_addr));
    if (bn_client_run(ctx, &server_addr, client_thread_count, client_task_count,
                      sizeof(test_message)) != UCS_OK) {
      goto clean_ucx;
    }
  }

clean_ucx:
  ucp_cleanup(ctx);

err:
  return MPI_Finalize();
}
