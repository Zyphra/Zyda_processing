# Copyright 2024 Zyphra Technologies.
# Based on SlimPajama codebase: https://github.com/Cerebras/modelzoo/blob/main/src/cerebras/modelzoo/data_preparation/nlp/slimpajama/dedup/to_hash.py
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

import os
import argparse
import datasets
from datasketch import MinHash
from collections import defaultdict
from zyda.utils.text import get_features

import nltk
nltk.download('punkt')

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)

COLUMNS_TO_SAVE = ["dataset_name", "shard", "shard_index", "global_index"]


def to_minhash(
    batch,
    key: str = "transformed_text",
    width: int = 13,
    num_perm: int = 128,
):
    output = defaultdict(list)
    for text in batch[key]:
        m = MinHash(num_perm=num_perm)
        m.update_batch(map(lambda x: x.encode('utf8'), get_features(text, width)))
        output["seed"].append(m.seed)
        output["hashvalues"].append(m.hashvalues)
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--load-path', type=str, required=True, help='Path to folder with preprocessed shards')
    parser.add_argument('--save-path', type=str, required=True, help='Path to folder to where we save ')
    parser.add_argument('--num-proc', type=int, default=1, help='Number of processes')
    parser.add_argument('--key', type=str, default='transformed_text', help='Key to use for minhashing')
    parser.add_argument('--width', type=int, default=13, help='n-grams size for minhashes')
    parser.add_argument('--num-perm', type=int, default=128, help='Number of permutation for computing minhashes')
    parser.add_argument('--from-scratch', action='store_true', help='If specified, will forcefully do every shard regardless of previous progress')
    args = parser.parse_args()

    shards_dirs = sorted(os.listdir(args.load_path))
    logging.info(f"Found {len(shards_dirs)} shards")
    for i, shard_dir in enumerate(shards_dirs):
        load_path = os.path.join(args.load_path, shard_dir)
        save_path = os.path.join(args.save_path, shard_dir)
        print()
        logging.info(f"Processing shard {i + 1} / {len(shards_dirs)} from {load_path}")
        if os.path.exists(save_path) and not args.from_scratch:
            logging.info(f"Already processed!")
            continue
        
        shard = datasets.load_from_disk(load_path)
        logging.info(f"Cache cleaned: {shard.cleanup_cache_files()}")

        shard_minhash = shard.map(
            lambda batch: to_minhash(batch, key=args.key, width=args.width, num_perm=args.num_perm),
            batched=True,
            num_proc=args.num_proc,
            remove_columns=[col for col in shard.column_names if col not in COLUMNS_TO_SAVE]
        )
        logging.info(f"Saving minhash to: {save_path}")
        shard_minhash.save_to_disk(save_path, max_shard_size="8GB")
        logging.info(f"Cache cleaned: {shard.cleanup_cache_files()}")
