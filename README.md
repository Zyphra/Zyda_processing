# Zyda
This repository contains the source code of `zyda` package - powerful Python-based software for filtering and deduplicating text datasets for a subsequent use for LLM training. The core dataset objects are based on HuggingFace's `datasets` package, making it straightforward to work with datasets hosted on HuggingFace. In addition, default HuggingFace format is based on `pyarrow`, which allows fast indexing and easy parallel processing of the datasets.

It was used for producing Zyda dataset: https://huggingface.co/datasets/Zyphra/Zyda

Technical report: https://arxiv.org/abs/2406.01981

We release step-by-step instructions on how to reproduce Zyda using this package.


## Installation
```
git clone https://github.com/Zyphra/Zyda_processing.git
cd Zyda_processing
pip install -e .
```

## Repository structure
Folder `zyda` contains source code for the package. It consists of the following subfolders:
- `preprocessing_and_filtering` - code for preprocessing and filtering the datasets
- `lsh_minhash` - code for computing minhash signatures and building LSH index
- `connected_components` - code for finding connected components in the graph of duplicates and for identifying which document to keep in every component
- `utils` - commonly reused code

Folder `zyda_reproduction` contains scripts for reproducing Zyda datasets arranged in a step-by-step manner.

## How to reproduce Zyda dataset
All the scripts necessary for Zyda reproduction are in `zyda_reproduction` folder. Below are step-by-step instructions on how to run them.
Before running the scripts, please, set the following environment variables:
- `DATA_BASE` - base location in your filesystem to save results of processing steps
- `REPO_BASE` - location of `Zyda_processing` repository
- `NUM_PROC` - number of parallel processes to use at various stages of processing 
- [optional] `HF_HOME` - location of HuggingFace cache

### 1. Downloading component datasets
Scripts for downloading component datasets are in `zyda_reproduction/1_downloading`.

All the scripts save downloaded datasets into separate folders in HuggingFace format in `$DATA_BASE/raw/<component name>` folders.

Downloading most components from HuggingFace is straighforward: run `download_arxiv_pile_peS2o_c4_refinedweb.py` and `download_refinedweb.py` scripts.

However, we had to apply special handling to SlimPajama and StarCoder:
1. Clone their HuggingFace repositories locally
2. Set `SLIMPAJAMA_REPO_PATH` and `STARCODER_REPO_PATH` environmental variable with paths to local SlimPajama and StarCoders repositories respectively
3. Run scripts `process_repo_slimpajama.py` and `process_repo_starcoder.py` for generating raw versions of the datasets in HuggingFace format.

### 2. Preprocessing and filtering
Scripts for preprocessing and filtering are in `zyda_reproduction/2_preprocessing_and_filtering`.

Run all the bash scripts in this folder.

This stage performs the following:
1. Generation of filtering features
2. Transformation of the text
3. Filtering of the documents (default filtering parameters can be found in `zyda/utils/filtering.py`)
4. Splitting of resultant datasets to shards, and saving them in `$DATA_BASE/processed/<component name>/shard_<id>` folders in HuggingFace format

### 3. Computing minhashes
Scripts for computing minhash signatures are in `zyda_reproduction/3_minhashing`.

Run all the bash scripts in this folder.

This stage performs the following:
1. Normalizes text of each document and splits it into words
2. Generates 13-grams based on words
3. Computes minhash signatures with of the size of 128
4. Saves results in `$DATA_BASE/minhash/<component name>` folders in HuggingFace format (it only saves columns necessary for indexing along with minhashes)

### 4. Building LSH index
Script for building LSH index is at `zyda_reproduction/4_lsh_indexing/run_lsh_dupes_0.4_all.sh`.

For Zyda we used 40% Jaccard similarity threshold when building our LSH index. Optimal split of minhash signatures can be computed using `zyda/lsh_minhash/compute_optimal_params.py`, which for our threshold and signature size gave us 32 bands with a range of 4.

This is the most time-consuming and memory-intensive stage. We split it in a parallel job distrubuted among 8 nodes of our HPC cluster, each with 92 physical cores and 2TB of RAM. It took approximately 2 days with a peak RAM consumption of 1.5TB.

We stripped away our distributed configuration in the script `run_lsh_dupes_0.4_all.sh`, basically assuming it will be run on one node. To limit RAM consumption we allow only 2 minhash bands to be processed in parallel by specifying `--bands-parallel 2` flag. On one compute node, bands are be split into 16 groups of size 2, and such groups are processed sequentially.

The resultant LSH index is saved in `$DATA_BASE/lsh_0.4/lsh_index-<band index>.pickle` files. We also save all the identified duplicate pairs in `$DATA_BASE/lsh_0.4/dupes/all_pairs-<band index>.txt` files.

### 5. Clustering duplicates using connected components and generating indices of documents to remove
Script for clustering duplicates using connected components and generating indices of documents to remove is at `zyda_reproduction/5_clustering/run_cc_lsh_0.4_dupes.sh`.

This stage performs clustering of identified duplicated documnt by identifying connected components in a graph, where nodes are documents and edges - duplicate pairs. Graph processing is implemented in `zyda/connected_components/generate_connected_components.py`.
1. It first performs processing of all duplicate pairs text files and generates a set that is saved to `$DATA_BASE/lsh_0.4/dupes/output/cc-set-final.txt`
2. It uses `networkit` package for building a graph and finding connecting components in it. It saves graph at `$DATA_BASE/lsh_0.4/dupes/output/cc-graph.graph`, document-to-node mapper at `$DATA_BASE/lsh_0.4/dupes/output/cc-mapper.pickle`, and connected components with node-to-document reverse mapper at `$DATA_BASE/lsh_0.4/dupes/output/cc.pickle`.

And finally we generate indices of duplicate documents to remove by sorting every document in a cluster according to a ranking and keeping only the highest ranked one. This is implemented in `zyda/connected_components/generate_indices_to_remove.py`. Resultant dict with mapping of datasets names to indices to remove is saved in `$DATA_BASE/lsh_0.4/dupes/output/dupes.pickle`. We decided to use the following ranking:
1. starcoder components
2. refinedweb
3. peS2o
4. arxiv
5. c4-en
6. pile-uncopyrighted
7. slimpajama

This stage took roughly half a day to run. 


### 6. Generating final dataset
Scripts for generating final dataset are in `zyda_reproduction/6_generating_final_dataset`.

Bash script `zyda_reproduction/6_generating_final_dataset/run_generate_lsh_0.4_jsonls.sh` generates final jsonl files of our dataset:
1. Load processed and filtered local HuggingFace datasets from stage 2
2. Remove documents using indices from the previous stage
3. Save resultant datasets in jsonl partitions to `$DATA_BASE/zyda_0.4-final/jsonl`

If you want to generate parquet files, you can convert jsonl's by running `zyda_reproduction/6_generating_final_dataset/run_convert_to_parquet_lsh_0.4.sh`, which saves them to `$DATA_BASE/zyda_0.4-final/parquet`. Files generated by this script were uploaded to Zyda's HuggingFace dataset repository.

## Citation 
To cite our work please use:

```
@misc{tokpanov2024zyda,
      title={Zyda: A 1.3T Dataset for Open Language Modeling}, 
      author={Yury Tokpanov and Beren Millidge and Paolo Glorioso and Jonathan Pilault and Adam Ibrahim and James Whittington and Quentin Anthony},
      year={2024},
      eprint={2406.01981},
      archivePrefix={arXiv},
      primaryClass={cs.CL}
}
```

## Acknowledgements
We would like to acknolwedge SlimPajama's team for publicly releasing their codebase with detailed instructions and explanations: [huggingface link](https://huggingface.co/datasets/cerebras/SlimPajama-627B), [github link](https://github.com/Cerebras/modelzoo/tree/Release_2.2.1/src/cerebras/modelzoo/data_preparation/nlp/slimpajama). We used their code as a starting point for LSH minhash deduplication. We made significant changes to optimize parallel performance and enable distributed deduplication jobs on our HPC cluster.


## License
[Apache License 2.0](./LICENSE)
