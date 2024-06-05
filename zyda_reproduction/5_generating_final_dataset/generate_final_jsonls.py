# Copyright 2024 Zyphra Technologies.
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
import os
import tqdm
import random
import gc
import json
import multiprocessing as mp

from zyda.utils.common import ComboDataset, ensure_directory_exists
from zyda.utils.filtering import FILTERING_FEATURES

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)

KEY = "transformed_text"
NEW_KEY = "text"
DISCARD_KEYS = ["shard", "shard_index"]
DATASETS_NAMES = [
    "pile-uncopyrighted",
    "c4-en",
    "peS2o",
    "arxiv",
    "refinedweb",
    "slimpajama",
    "starcoder-languages",
    "starcoder-github-issues-filtered-structured",
    "starcoder-jupyter-structured-clean-dedup",
    "starcoder-git-commits-cleaned",
]
DATA_BASE = os.environ.get("DATA_BASE")
PATHS = {name: os.path.join(DATA_BASE, name) for name in DATASETS_NAMES}
DATASETS = {name: ComboDataset(path) for name, path in PATHS.items() if os.path.exists(path)}


def save_shard_to_jsonl(data, out_folder, text_key, ds_name, total, idx):
    shard = data.shard(num_shards=total, index=idx, contiguous=True)
    jsonl_name = f"{ds_name}_{idx}.jsonl"
    save_path = os.path.join(out_folder, jsonl_name)
    ensure_directory_exists(save_path)
    with open(save_path, 'w') as f:
        for row in tqdm.tqdm(shard, desc=jsonl_name, unit="docs", unit_scale=True, position=idx):
            row_dict = {
                "filtering_features": {},
                "source_other": {},
            }
            for key, val in row.items():
                if key in DISCARD_KEYS:
                    continue
                elif key in FILTERING_FEATURES:
                    row_dict["filtering_features"][key] = str(val)
                elif key == text_key:
                    row_dict[NEW_KEY] = str(val)
                elif key == "dataset_name":
                    row_dict["source"] = str(val)
                elif key == "global_index":
                    row_dict["source_index"] = str(val)
                elif key != KEY:  # need this check for starcoder
                    row_dict["source_other"][key] = str(val)
            f.write(json.dumps(row_dict) + "\n")


def generate_datasets(args):

    logging.info(f"Loading duplicates indices from {args.input_indices}...")
    with open(args.input_indices, "rb") as f:
        duplicates = pickle.load(f)
    gc.collect()

    for ds_name, dataset in DATASETS.items():

        if ds_name not in duplicates:
            logging.info(f"Dataset {ds_name} is not in duplicates dict, so setting dupes to empty set")
            dupes = set()
        else:
            dupes = duplicates[ds_name]

        logging.info(f"Processing {ds_name}")
        logging.info(f"Number of duplicates to remove: {len(dupes)}")

        dupe_inds = set()       
        for dupe in tqdm.tqdm(dupes, desc="Processing dupe indices", unit="inds", unit_scale=True):
            _, _, global_index = dupe
            dupe_inds.add(global_index)
        num_dupes_removed = len(dupe_inds)

        if len(dupe_inds) > 0:
            select_inds = []
            for i in tqdm.tqdm(range(len(dataset.ds)), desc="Generating indices to select", unit="inds", unit_scale=True):
                if args.check_indices and random.random() < 1e-4:
                    assert i == dataset.ds[i]["global_index"]
                if i not in dupe_inds:
                    select_inds.append(i)
            del dupe_inds
            gc.collect()

            logging.info(f"Selecting {len(select_inds)} rows")
            filtered_ds = dataset.ds.select(select_inds)
            del select_inds
            gc.collect()
        else:
            filtered_ds = dataset.ds

        logging.info(f"Removed {len(dataset.ds) - len(filtered_ds)} rows overall")
        logging.info(f"Length of the final dataset: {len(filtered_ds)} rows ({100 * len(filtered_ds) / len(dataset.ds):.2f}% of original {len(dataset.ds)} rows)")
        assert len(dataset.ds) - len(filtered_ds) >= num_dupes_removed

        text_key = KEY
        if "starcoder" in ds_name:
            text_key = "content"
            logging.info(f"Starcoder detected: using {text_key} as a key")
        
        out_folder = os.path.join(args.out_folder, "jsonls", ds_name)            
        partitions = args.jsonl_partitions
        processes = []
        logging.info(f"Saving {partitions} jsonls to {out_folder}")
        for process_id in range(partitions):
            p = mp.Process(target=save_shard_to_jsonl, args=(filtered_ds, out_folder, text_key, ds_name, partitions, process_id))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
        print("\n" * partitions)

        logging.info(f"Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-indices", type=str, required=True, help="Input pickle file with connected components.")
    parser.add_argument("--out-folder", type=str, required=True, help="Output folder for saving jsonl files.")
    parser.add_argument("--jsonl-partitions", type=int, default=48, help="Number of jsonl partitions. Note: one process will be used per partition.")

    args = parser.parse_args()
    
    generate_datasets(args)
