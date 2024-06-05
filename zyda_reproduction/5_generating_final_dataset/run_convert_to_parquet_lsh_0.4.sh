#!/bin/bash
IN=$DATA_BASE/zyda_0.4/jsonl
OUT=$DATA_BASE/zyda_0.4/jsonl

DATASET=refinedweb
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 8192
fi

DATASET=arxiv
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 64
fi

DATASET=peS2o
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 512
fi

DATASET=c4-en
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 2048
fi

DATASET=pile-uncopyrighted
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 1024
fi

DATASET=slimpajama
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 2048
fi

DATASET=starcoder-languages
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 2048
fi

DATASET=starcoder-git-commits-cleaned
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 128
fi

DATASET=starcoder-jupyter-structured-clean-dedup
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 32
fi

DATASET=starcoder-github-issues-filtered-structured
if test -d $IN/$DATASET; then
    echo Processing $DATASET
    mkdir -p $OUT/$DATASET
    python $REPO_BASE/scripts/5_generating_final_dataset/convert_jsonl_to_parquet.py \
        --input-folder $IN/$DATASET \
        --output-folder $OUT/$DATASET \
        --num-proc $NUM_PROC \
        --partitions 256
fi
