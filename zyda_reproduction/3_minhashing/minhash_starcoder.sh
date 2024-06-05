#!/bin/bash

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/starcoder-languages \
    --save-path $DATA_BASE/minhash/starcoder-languages \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key content

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/starcoder-github-issues-filtered-structured \
    --save-path $DATA_BASE/minhash/starcoder-github-issues-filtered-structured \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key content

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/starcoder-jupyter-structured-clean-dedup \
    --save-path $DATA_BASE/minhash/starcoder-jupyter-structured-clean-dedup \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key content

python $REPO_BASE/zyda/src/lsh_minhash/compute_minhash.py \
    --load-path $DATA_BASE/processed/starcoder-git-commits-cleaned \
    --save-path $DATA_BASE/minhash/starcoder-git-commits-cleaned \
    --num-proc $NUM_PROC \
    --width 13 \
    --num-perm 128 \
    --key content
