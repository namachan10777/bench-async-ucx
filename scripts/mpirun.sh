module "load openmpi/${NQSV_MPI_VER}"
mpirun "${NQSV_MPIOPTS}" -np 2 -npernode 1 "../target/release/bin/${BIN}"