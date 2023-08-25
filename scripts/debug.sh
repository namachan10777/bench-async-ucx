#!/bin/sh

qlogin \
    -b 2 \
    -q debug \
    -T openmpi \
    -A NBB \
    -v NQSV_MPI_VER=4.1.5/gcc9.4.0-cuda11.8.0