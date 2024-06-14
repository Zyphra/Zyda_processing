#!/bin/bash

NAME=starcoder-languages
python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess_and_filter.py \
    --hf-path $DATA_BASE/raw/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key content \
    --keep-key \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=starcoder-github-issues-filtered-structured
python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess_and_filter.py \
    --hf-path $DATA_BASE/raw/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key content \
    --keep-key \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=starcoder-jupyter-structured-clean-dedup
python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess_and_filter.py \
    --hf-path $DATA_BASE/raw/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key content \
    --keep-key \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME

NAME=starcoder-git-commits-cleaned
python $REPO_BASE/zyda/preprocessing_and_filtering/preprocess_and_filter.py \
    --hf-path $DATA_BASE/raw/$NAME \
    --load-from-disk \
    --num-proc $NUM_PROC \
    --key content \
    --keep-key \
    --name $NAME \
    --save-path $DATA_BASE/processed/$NAME
