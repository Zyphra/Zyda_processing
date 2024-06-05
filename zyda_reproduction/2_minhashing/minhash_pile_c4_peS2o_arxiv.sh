#!/bin/bash

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/pile-uncopyrighted \
    --save-path $DATA_BASE/minhash/pile-uncopyrighted \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key transformed_text

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/c4-en \
    --save-path $DATA_BASE/minhash/c4-en \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key transformed_text

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/peS2o \
    --save-path $DATA_BASE/minhash/peS2o \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key transformed_text

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/arxiv \
    --save-path $DATA_BASE/minhash/arxiv \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key transformed_text
