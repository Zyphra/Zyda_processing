#!/bin/bash

python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess_and_filter.py \
    --hf-path $DATA_BASE/raw/slimpajama \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name slimpajama \
    --save-path $DATA_BASE/processed/slimpajama
