#!/bin/bash

NAME=refinedweb
python $REPO_BASE/zyda/src/preprocessing/preprocess.py \
    --hf-path $DATA_BASE/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME
