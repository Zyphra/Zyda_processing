#!/bin/bash

python $REPO_BASE/zyda/src/preprocessing/preprocess.py \
    --hf-path $DATA_BASE/SlimPajama-HF \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name slimpajama \
    --save-path $DATA_BASE/processed/slimpajama
