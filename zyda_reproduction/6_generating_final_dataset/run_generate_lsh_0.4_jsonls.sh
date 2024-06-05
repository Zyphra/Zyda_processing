#!/bin/bash

python $REPO_BASE/zyda_reproduction/6_generating_final_dataset/generate_final_jsonls.py \
    --input-indices $DATA_BASE/lsh_0.4/dupes/output/dupes.pickle \
    --out-folder $DATA_BASE/zyda_0.4-final/jsonl \
    --jsonl-partitions 48
