#!/bin/bash

INPUT_DIR=$DATA_BASE/lsh_0.4/dupes
CC_PICKLE=$DATA_BASE/lsh_0.4/dupes/output/cc.pickle
DUPES_PICKLE=$DATA_BASE/lsh_0.4/dupes/output/dupes.pickle

WORKERS=2

python $REPO_BASE/zyda/connected_components/generate_connected_components.py \
    --input-dir $INPUT_DIR \
    --out-file $CC_PICKLE \
    --workers $WORKERS

python $REPO_BASE/zyda/connected_components/generate_indices_to_remove.py \
    --input-file $CC_PICKLE \
    --out-file $DUPES_PICKLE \
    --ranking \
        starcoder-languages \
        starcoder-github-issues-filtered-structured \
        starcoder-jupyter-structured-clean-dedup \
        starcoder-git-commits-cleaned \
        refinedweb \
        peS2o \
        arxiv \
        c4-en \
        pile-uncopyrighted \
        slimpajama    
