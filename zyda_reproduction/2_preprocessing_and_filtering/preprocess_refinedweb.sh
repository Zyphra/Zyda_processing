#!/bin/bash

NAME=refinedweb
python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess.py \
    --hf-path $DATA_BASE/raw/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME
