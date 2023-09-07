#!/bin/bash
#PBS -b 2
#PBS -q gpu
#PBS -T openmpi
#PBS -A NBB
#PBS -v NQSV_MPI_VER=4.1.5/gcc9.4.0-cuda11.8.0

BIN=tag

module load use.own
module load "openmpi/${NQSV_MPI_VER}"
module load "ucx/1.13.1/cuda11.8.0"

RUST_LOG=Info
SERVER_THREAD_COUNT=1
CLIENT_THREAD_COUNT=1
CLIENT_TASK_COUNT=256

mpirun ${NQSV_MPIOPTS} \
    -x RUST_LOG \
    -x SERVER_THREAD_COUNT \
    -x CLIENT_THREAD_COUNT \
    -x CLIENT_TASK_COUNT \
    -np 2 \
    -npernode 1 \
    "/work/0/NBB/mnakano/bench-async-ucx/target/release/${BIN}"
