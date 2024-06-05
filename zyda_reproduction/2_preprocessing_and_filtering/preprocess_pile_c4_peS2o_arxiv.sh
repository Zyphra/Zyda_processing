#!/bin/bash

NAME=pile-uncopyrighted
python $REPO_BASE/zyda/src/preprocessing/preprocess_and_filter.py \
    --hf-path $DATA_BASE/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=c4-en
python $REPO_BASE/zyda/src/preprocessing/preprocess_and_filter.py \
    --hf-path $DATA_BASE/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=peS2o
python $REPO_BASE/zyda/src/preprocessing/preprocess_and_filter.py \
    --hf-path $DATA_BASE/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=arxiv
python $REPO_BASE/zyda/src/preprocessing/preprocess_and_filter.py \
    --hf-path $DATA_BASE/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key text \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME
