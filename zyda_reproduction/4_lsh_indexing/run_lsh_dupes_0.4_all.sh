#!/bin/bash
LSH_OUT_DIR=$DATA_BASE/lsh_0.4
BANDS=32
RANGE=4

# Running on one machine will be slow.
# Use --nodes and --node-rank for ditributing a job among multiple compute nodes.
# We split our job across 8 nodes with 2TB of RAM. Peak RAM usge was a bit more than 1TB.

python $REPO_BASE/zyda/lsh_minhash/build_lsh_index.py \
    --load-path \
        $DATA_BASE/minhash/pile-uncopyrighted \
        $DATA_BASE/minhash/c4-en \
        $DATA_BASE/minhash/peS2o \
        $DATA_BASE/minhash/arxiv \
        $DATA_BASE/minhash/refinedweb \
        $DATA_BASE/minhash/slimpajama \
        $DATA_BASE/minhash/starcoder-languages \
        $DATA_BASE/minhash/starcoder-git-commits-cleaned \
        $DATA_BASE/minhash/starcoder-github-issues-filtered-structured \
        $DATA_BASE/minhash/starcoder-jupyter-structured-clean-dedup \
    --dupes-out $LSH_OUT_DIR/dupes/all_pairs.txt \
    --lsh-out $LSH_OUT_DIR/lsh_index.pickle \
    --range $RANGE \
    --bands $BANDS \
    --bands-parallel 2 \
    --reader-processes 12
