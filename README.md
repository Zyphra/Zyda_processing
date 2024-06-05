# Zyda
This repository contains source code of `zyda` package - powerful Python-based software for filtering and deduplicating text datasets for a subsequent use for LLM training.
It was used for producing Zyda dataset: https://huggingface.co/datasets/Zyphra/Zyda
We release step-by-step instructions on how to reproduce Zyda using this package.

## Installation
```
git clone https://github.com/Zyphra/Zyda_processing.git
cd Zyda_processing
pip install -e .
```

## Repository structure
Folder `zyda` contains source code for the package. It consists of the following subfolders:
- `preprocessing` - code for preprocessing and filtering the datasets
- `lsh_minhash` - code for computing minhash signatures and building LSH index
- `connected_components` - code for finding connected components in the graph of duplicates and for identifying which document to keep in every component
- `utils` - commonly reused code

Folder `zyda_reproduction` contains scripts for reproducing Zyda datasets arranged in a step-by-step manner.

## How to reproduce Zyda
### Downloading components datasets
### Preprocessing and filtering
### Computing minhashes
### Building LSH index
### Clustering duplicates using connected components
### Generating indices of documents to remove
### Generating final dataset