#!/bin/bash

python $REPO_BASE/scripts/5_generating_final_dataset/generate_final_jsonls.py \
    --input-indices $DATA_BASE/dupes/lsh_0.4/dupes.pickle \
    --out-folder $DATA_BASE/zyda_0.4-HF/jsonl \
    --jsonl-partitions 48
