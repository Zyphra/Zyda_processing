# Copyright 2024 Zyphra Technologies.
# Based on SlimPajama codebase: https://github.com/Cerebras/modelzoo/blob/main/src/cerebras/modelzoo/data_preparation/nlp/slimpajama/dedup/generate_duplicate_pairs.py
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
import queue
import time
import os
import more_itertools
from typing import List
from tqdm import tqdm
from collections import defaultdict
from multiprocessing import Process, Queue
from datasketch.lean_minhash import LeanMinHash
from zyda.utils.common import ensure_directory_exists

import datasets

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)


def _H(hs):
    return bytes(hs.byteswap().data)


def get_hashes_band(
    shards: List[datasets.Dataset],
    doc_queue: Queue,
    i: int,
    r: int,
    log_interval: int = 0,
):
    for shard in shards:
        for j, item in enumerate(shard):
            if log_interval and j % log_interval == 0:
                logging.debug(f"Band {i}: read {j} records")
            key = f"{item['dataset_name']}@{item['shard']}@{item['shard_index']}@{item['global_index']}"
            minhash = LeanMinHash(seed=item['seed'], hashvalues=item['hashvalues'])
            H = _H(minhash.hashvalues[i * r : (i + 1) * r])
            doc_queue.put((key, H))


def lsh_process(
    dupes_out: str,
    lsh_in: str,
    lsh_out: str,
    doc_queue: Queue,
    queue_idx: int,
    band_idx: int,
    log_interval: int = 1_000_000,
    n_docs: int = 0, 
    check_only: bool = False,
):
    lsh_dict = defaultdict(str)
    if lsh_in:
        lsh_in_band = lsh_in.replace(".pickle", f"-{band_idx}.pickle")
        if os.path.exists(lsh_in_band):
            with open(lsh_in_band, "rb") as f:
                logging.info(f"Band {band_idx}: loading LSH index from {lsh_in_band}")
                lsh_dict = pickle.load(f)
                logging.info(f"Band {band_idx}: loaded LSH index from {lsh_in_band}")
        elif check_only:
            raise FileExistsError()
        else:
            logging.info(f"Band {band_idx}: did not find existing LSH index at {lsh_in_band}, so creating a new one")
    
    ensure_directory_exists(dupes_out)
    with open(dupes_out.replace(".txt", f"-{band_idx}.txt"), "w") as f:
        i = 0
        start_time = time.time()
        t0 = start_time
        if n_docs:
            pbar = tqdm(desc=f"Band {band_idx}", total=n_docs, unit_scale=True, position=queue_idx, dynamic_ncols=True)
        while True:
            try:
                key, H = doc_queue.get(timeout=30)
                cand = lsh_dict.get(H, "None")
                if cand != "None":
                    f.write(f'{key} :: {cand}\n')
                elif not check_only:
                    lsh_dict[H] = key
                i += 1
                if n_docs:
                    pbar.update(1)
                elif i % log_interval == 0:
                    speed = log_interval / (time.time() - t0)
                    t0 = time.time()                
                    logging.info(
                        f"Band {band_idx}: Processed {i / 1_000_000:.1f}M in {time.time() - start_time:.1f}s; "
                        f"{speed / 1_000:.1f}kdocs/sec. Index size: {len(lsh_dict) / i * 100:.2f}%. "
                        f"Doc queue size: {doc_queue.qsize()}"
                    )
            except queue.Empty:
                break
        if n_docs:
            pbar.close()

        if not check_only:
            lsh_out_band = lsh_out.replace(".pickle", f"-{band_idx}.pickle")
            ensure_directory_exists(lsh_out_band)
            with open(lsh_out_band, "wb") as f:
                logging.info(f"Band {band_idx}: saving LSH index to {lsh_out_band}")
                pickle.dump(lsh_dict, f, protocol=5)
                logging.info(f"Band {band_idx}: saved LSH index to {lsh_out_band}")
        logging.info(f"Band {band_idx}: Total number of documents: {i}")


def generate_pairs(args):
    print()

    bands_inds = range(args.bands)
    bands_splits = [list(x) for x in more_itertools.divide(args.node, bands_inds)]
    if args.node_rank > -1:
        bands_splits = [bands_splits[args.node_rank]]
    if args.bands_parallel > 0:
        bands_splits_flattened = [band for bands in bands_splits for band in bands]
        bands_splits = [list(x) for x in more_itertools.chunked(bands_splits_flattened, args.bands_parallel)]
    logging.info(f"Bands splits: {bands_splits}")

    num_queues = max([len(x) for x in bands_splits])
    doc_queues = [Queue(1_000_000) for _ in range(num_queues)]    

    reader_shards = [[] for _ in range(args.reader_processes)]
    total_length = 0
    for arg_load_path in args.load_path:        
        mh_dirs = sorted(os.listdir(arg_load_path))
        logging.info(f'Loading {len(mh_dirs)} minhash shards from {arg_load_path}')
        mh_shards = []
        for mh_dir in tqdm(mh_dirs):
            load_path = os.path.join(arg_load_path, mh_dir)
            mh_shards.append(datasets.load_from_disk(load_path))
        logging.info('Concatenating into a single dataset')
        mh_ds = datasets.concatenate_datasets(mh_shards)
        total_length += len(mh_ds)
        logging.info(f'Splitting into {args.reader_processes} shards')
        for i in range(args.reader_processes):
            shard = mh_ds.shard(num_shards=args.reader_processes, index=i, contiguous=True)
            reader_shards[i].append(shard)
        
    t0 = time.time()
    for bands_split in bands_splits:
        logging.info('-' * 120)
        logging.info(f"Processing bands: {bands_split}")
        logging.info('-' * 120)
        processes = []
        for q_i, band_i in enumerate(bands_split):
            for process_id in range(args.reader_processes):
                p = Process(
                    target=get_hashes_band,
                    args=(reader_shards[process_id], doc_queues[q_i], band_i, args.range, args.log_interval),
                )
                processes.append(p)
                p.start()

            p = Process(
                target=lsh_process,
                args=(args.dupes_out, args.lsh_in, args.lsh_out, doc_queues[q_i], q_i, band_i, args.log_interval, total_length, args.check_only),
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

    logging.info('-' * 120)
    logging.info(f'Done processing LSH index in {time.time() - t0:.1f}s.')
    logging.info('-' * 120)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-path", nargs="+", type=str, required=True, help="Path to a folder with shards from minhashing step")
    parser.add_argument("--check-only", action="store_true", help="Only check existing LSH index")
    parser.add_argument("--dupes-out", type=str, required=True, help="Output text file with duplicates")
    parser.add_argument("--lsh-in", type=str, help="Pickle file with LSH index to load")
    parser.add_argument("--lsh-out", type=str, required=True, help="Output pickle file with LSH index")
    parser.add_argument("--range", type=int, required=True, help="Range of LSH index")
    parser.add_argument("--bands", type=int, required=True, help="Number of bands of LSH index")
    parser.add_argument("--node", type=int, default=1, help="Number of splits of LSH bands")
    parser.add_argument("--node-rank", type=int, default=-1, help="Rank of the split")
    parser.add_argument("--bands-parallel", type=int, default=-1, help="Number of bands to be processed in parallel")
    parser.add_argument("--reader-processes", type=int, default=1, help="Number of reader processes to populate document queues")
    parser.add_argument("--log-interval", type=int, default=100_000, help="Interval of logging/updating progress bar")
    args = parser.parse_args()

    generate_pairs(args)
