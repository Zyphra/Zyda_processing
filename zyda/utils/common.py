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

import os
import datasets
import tqdm

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)


class ComboDataset:
    def __init__(self, path: str):
        self.path = path
        
        shards_dirs = sorted(os.listdir(path))
        logging.info(f"\nLoading {len(shards_dirs)} shards from {path}")
        self.shards_dirs = shards_dirs

        shards = []
        for shard_dir in tqdm.tqdm(shards_dirs):
            load_path = os.path.join(path, shard_dir)
            shards.append(datasets.load_from_disk(load_path))
        self.shards = shards

        ds = datasets.concatenate_datasets(shards)
        self.ds = ds
        logging.info(ds)


def ensure_directory_exists(filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok = True)
