#include <arpa/inet.h>
#include <ifaddrs.h>
#include <mpi.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/config/types.h>
#include <ucs/sys/compiler_def.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>
#include <uct/api/uct.h>
#include <unistd.h>

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
      freeifaddrs(ifaddr);
      return 0;
    }
  }

  freeifaddrs(ifaddr);
  return -1;
}

/*
static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
  //unimplemented
}

static ucs_status_t connect_to_server(ucp_worker_h worker, struct sockaddr
*server_addr, ucp_ep_h *client_ep_in) { ucp_ep_params_t ep_params = {
      .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR |
                    UCP_EP_PARAM_FIELD_ERR_HANDLER |
                    UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE,
      .err_mode = UCP_ERR_HANDLING_MODE_PEER,
      .err_handler.cb = err_cb,
      .err_handler.arg = NULL,
      .flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER,
      .sockaddr.addr = server_addr,
      .sockaddr.addrlen = sizeof(struct sockaddr),
  };
  ucs_status_t status = ucp_ep_create(worker, &ep_params, client_ep_in);
  if (status != UCS_OK) {
    fprintf( stderr, "Failed to create ep (%s)\n", ucs_status_string(status));
  }
  return status;
}*/

enum send_recv_type {
  CLIENT_SERVER_SEND_RECV_STREAM = UCS_BIT(0),
  CLIENT_SERVER_SEND_RECV_TAG = UCS_BIT(1),
  CLIENT_SERVER_SEND_RECV_AM = UCS_BIT(2),
  CLIENT_SERVER_SEND_RECV_DEFAULT = CLIENT_SERVER_SEND_RECV_STREAM
};

static ucs_status_t init_worker(ucp_context_h ctx, ucp_worker_h *worker_in) {
  ucs_status_t status;
  ucp_worker_params_t params;
  memset(&params, 0, sizeof(ucp_worker_params_t));
  params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  params.thread_mode = UCS_THREAD_MODE_SINGLE;
  if ((status = ucp_worker_create(ctx, &params, worker_in))) {
    fprintf(stderr, "ucp_worker_create: (%s)\n", ucs_status_string(status));
    return status;
  }
  return UCS_OK;
}

static ucs_status_t init_context(ucp_context_h *ctx_in, ucp_worker_h *worker_in,
                                 enum send_recv_type send_recv_type) {
  ucp_params_t params;
  ucs_status_t status;
  ucp_config_t *config;
  memset(&params, 0, sizeof(ucp_params_t));

  if ((status = ucp_config_read(NULL, NULL, &config))) {
    fprintf(stderr, "ucp_read_config: (%s)\n", ucs_status_string(status));
    return status;
  }
  ucp_config_print(config, stderr, "UCP Configuration",
                   UCS_CONFIG_PRINT_CONFIG | UCS_CONFIG_PRINT_HEADER);

  params.field_mask =
      UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
  params.mt_workers_shared = 1;

  if (send_recv_type == CLIENT_SERVER_SEND_RECV_STREAM) {
    params.features = UCP_FEATURE_STREAM;
  } else if (send_recv_type == CLIENT_SERVER_SEND_RECV_TAG) {
    params.features = UCP_FEATURE_TAG;
  } else {
    params.features = UCP_FEATURE_AM;
  }
  if ((status = ucp_init_version(1, 14, &params, config, ctx_in)) != UCS_OK) {
    fprintf(stderr, "ucp_init: (%s)\n", ucs_status_string(status));
    goto err;
  }
  if ((status = init_worker(*ctx_in, worker_in)) != UCS_OK) {
    fprintf(stderr, "init_worker: (%s)\n", ucs_status_string(status));
    goto err_worker;
  }
  return UCS_OK;
err_worker:
  ucp_cleanup(*ctx_in);
err:
  return status;
}

int main(int argc, char *argv[]) {
  int nprocs, rank;
  struct sockaddr server_addr;
  MPI_Status status;
  ucp_context_h ctx;
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
    if (init_context(&ctx, &worker, CLIENT_SERVER_SEND_RECV_TAG) != UCS_OK) {
      fprintf(stderr, "init_context\n");
      exit(-1);
    }
  } else {
    // とりあえずIPv4だけ考える
    MPI_Recv(&server_addr, sizeof(struct sockaddr), MPI_BYTE, 0, 99,
             MPI_COMM_WORLD, &status);
    struct sockaddr_in *i = (struct sockaddr_in *)&server_addr;
    printf("got server addr: %s\n", inet_ntoa(i->sin_addr));
    if (init_context(&ctx, &worker, CLIENT_SERVER_SEND_RECV_TAG) != UCS_OK) {
      fprintf(stderr, "init_context\n");
      exit(-1);
    }
  }

  ucp_worker_destroy(worker);
  ucp_cleanup(ctx);

  return MPI_Finalize();
}
