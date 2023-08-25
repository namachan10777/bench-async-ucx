#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);

  int myrank, num_procs;
  char message[256];
  MPI_Status status;
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  printf("my rank: %d, n_procs: %d\n", myrank, num_procs);
  if (myrank == 0) {
    strcpy(message, "Hello, there");
    MPI_Send(message, strlen(message) + 1, MPI_CHAR, 1, 99, MPI_COMM_WORLD);
    printf("rank%d sent: message: %s\n", myrank, message);
  } else {
    MPI_Recv(message, sizeof(message), MPI_CHAR, 0, 99, MPI_COMM_WORLD, &status);
    printf("rank%d received: %s\n", myrank, message);
  }

  MPI_Finalize();
  return EXIT_SUCCESS;
}
