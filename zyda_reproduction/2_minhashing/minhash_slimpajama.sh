#!/bin/bash

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/slimpajama \
    --save-path $DATA_BASE/minhash/slimpajama \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key transformed_text
