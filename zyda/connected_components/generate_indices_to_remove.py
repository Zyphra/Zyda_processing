# Copyright 2024 Zyphra Technologies.
# Based on SlimPajama codebase: https://github.com/Cerebras/modelzoo/blob/main/src/cerebras/modelzoo/data_preparation/nlp/slimpajama/dedup/generate_duplicates_dict.py
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import pickle
from collections import defaultdict

import tqdm

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)


def generate_duplicates(args):
    print()
    
    if args.ranking:
        ranking = {}
        for i, dataset in enumerate(args.ranking):
            ranking[dataset] = i
        logging.info(f"Ranking of datasets: {ranking}")            
    
    # load pickled components and other artifacts
    logging.info(f'Loading {args.input_file}')
    with open(args.input_file, "rb") as fin:
        components, n_components, reverse_mapper = pickle.load(fin)

    logging.info("Processing connected components...")
    duplicates = defaultdict(set)
    n_duplicate_docs = 0
    for component in tqdm.tqdm(components, unit="components", unit_scale=True):
        if args.ranking:
            component.sort(key=lambda x: ranking[reverse_mapper[x].split("@")[0]])
        for j in range(1, len(component)):
            doc = reverse_mapper[component[j]]
            file_name, shard, shard_index, global_index = doc.split("@")
            duplicates[file_name].add((int(shard), int(shard_index), int(global_index)))
            n_duplicate_docs += 1

    logging.info(f"Total number of duplicate documents that will be removed: {n_duplicate_docs}")
    logging.info("Duplicates to remove per dataset:")
    for ds_name, dupes in duplicates.items():
        logging.info(f"   {ds_name}: {len(dupes)}")
    
    logging.info(f"Saving to {args.out_file}...")
    with open(args.out_file, "wb") as fout:
        pickle.dump(duplicates, fout, protocol=5)
    logging.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", type=str, required=True, help="Input pickle file with connected components")
    parser.add_argument("--out-file", type=str, required=True, help="Output pickle file to save indices of duplicates to remove")
    parser.add_argument("--ranking", nargs="+", type=str, help="Ranking of datasets for choosing a single document from a component")
    
    args = parser.parse_args()
    generate_duplicates(args)
