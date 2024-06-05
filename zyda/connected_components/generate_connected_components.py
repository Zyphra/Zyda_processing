# Copyright 2024 Zyphra Technologies.
# Based on SlimPajama codebase: https://github.com/Cerebras/modelzoo/blob/main/src/cerebras/modelzoo/data_preparation/nlp/slimpajama/dedup/generate_connected_components.py
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

from typing import Tuple, List, Dict
from glob import glob
import argparse
import pickle
import os
import networkit as nk
import tqdm
import subprocess
import multiprocessing as mp
import gc

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)

from zyda.utils.common import ensure_directory_exists


def construct_graph(set_of_duplicate_pairs: set) -> Tuple[nk.Graph, Dict[str, int]]:
    G = nk.Graph()
    mapper = {}
    for pair in tqdm.tqdm(set_of_duplicate_pairs, desc="Bulding graph", unit="dupes", unit_scale=True):
        node1_name, node2_name = pair
        if node1_name not in mapper:
            mapper[node1_name] = G.addNode()
        if node2_name not in mapper:
            mapper[node2_name] = G.addNode()
        G.addEdge(mapper[node1_name], mapper[node2_name])
    return G, mapper


def find_connected_components(G):
    cc = nk.components.ConnectedComponents(G)
    cc.run()
    return cc.getComponents(), cc.numberOfComponents()


def count_file_lines(fname: str) -> int:
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, 
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])


def process_files(args: Tuple[int, str, bool, List[str]]) -> str:
    set_of_duplicate_pairs = set()
    pid = args[0]
    save_path = args[1]
    from_scratch = args[2]
    files = args[3]

    # save_path = save_path.replace(".pickle", f"-set-{pid}.pickle")
    save_path = save_path.replace(".pickle", f"-set-{pid}.txt")
    if os.path.exists(save_path) and not from_scratch:
        logging.info(f"Found {save_path}")
        return save_path, None

    for file in files:
        with open(file, "r") as f:
            for line in tqdm.tqdm(f, desc=file, unit="dupes", unit_scale=True, position=pid):
                pair = tuple(line.strip().split(" :: "))
                if pair[0] != pair[1]:
                    set_of_duplicate_pairs.add(pair)
    
    desc = f"Saving set to {save_path}"
    ensure_directory_exists(save_path)
    with open(save_path, "w") as f:
        for pair in tqdm.tqdm(set_of_duplicate_pairs, desc=desc, unit="dupes", unit_scale=True, position=pid):
            f.write(f"{pair[0]} :: {pair[1]}\n")
    return save_path, len(set_of_duplicate_pairs)


def get_set_of_duplicate_pairs(set_save_path: str, from_scratch: bool) -> set:
    set_of_duplicate_pairs = set()
    if not from_scratch and os.path.exists(set_save_path):
        # Set of duplicates exists, load it
        logging.info(f"Counting lines in {set_save_path}")
        total_lines = count_file_lines(set_save_path)
        logging.info(f"Constructing set of duplicates from {set_save_path}")
        with open(set_save_path, "r") as f:
            for line in tqdm.tqdm(f, total=total_lines, unit="docs", unit_scale=True):
                pair = tuple(line.strip().split(" :: "))
                if pair[0] != pair[1]:
                    set_of_duplicate_pairs.add(pair)
    else:
        # Need to generate a set of duplicates
        all_files = sorted(glob(f"{args.input_dir}/*.txt"))
        workers_files = [[] for _ in range(args.workers)]
        for i, file in enumerate(all_files):
            workers_files[i % args.workers].append(file)

        workers_args = []
        for i, files in enumerate(workers_files):
            workers_args.append((i, args.out_file, args.from_scratch, files))

        with mp.Pool(processes=args.workers) as p:
            sets_files = p.map(process_files, workers_args)
        
        logging.info("Processing sets from workers")
        logging.info("Constructing final set")
        for file, total_pairs in sets_files:
            with open(file, "r") as f:
                for line in tqdm.tqdm(f, total=total_pairs, desc=file, unit="dupes", unit_scale=True):
                    pair = tuple(line.strip().split(" :: "))
                    if pair[0] != pair[1]:
                        set_of_duplicate_pairs.add(pair)

        desc = f"Saving final set to {set_save_path}..."
        ensure_directory_exists(set_save_path)
        with open(set_save_path, "w") as f:
            for pair in tqdm.tqdm(set_of_duplicate_pairs, desc=desc, unit="dupes", unit_scale=True):
                f.write(f"{pair[0]} :: {pair[1]}\n")
    
    return set_of_duplicate_pairs


def generate_connected_components_mp(args):
    print()
    nk.setNumberOfThreads(args.nk_threads)

    graph_save_path = args.out_file.replace(".pickle", f"-graph.graph")
    mapper_save_path = args.out_file.replace(".pickle", f"-mapper.pickle")
    set_save_path = args.out_file.replace(".pickle", f"-set-final.txt")
    
    if not args.from_scratch and os.path.exists(graph_save_path) and os.path.exists(mapper_save_path):
        # Graph and mapper exist, so load them
        logging.info(f"Loading a graph from {graph_save_path}")
        G = nk.graphio.readGraph(graph_save_path, nk.Format.METIS)

        logging.info(f"Loading a mapper from {mapper_save_path}")
        with open(mapper_save_path, "rb") as f:
            mapper = pickle.load(f)

    else:
        logging.info("Processing text files with duplicates...")
        set_of_duplicate_pairs = get_set_of_duplicate_pairs(set_save_path, args.from_scratch)
        logging.info(f"Length of the set of duplicates: {len(set_of_duplicate_pairs)}")

        # Generate a graph using id's as nodes and a pair of ids as an edge
        logging.info("Building graph...")
        G, mapper = construct_graph(set_of_duplicate_pairs)
        del set_of_duplicate_pairs
        gc.collect()
        
        logging.info(f"Saving graph to {graph_save_path}")
        nk.graphio.writeGraph(G, graph_save_path, nk.Format.METIS)
        
        logging.info(f"Saving mapper to {mapper_save_path}")
        ensure_directory_exists(mapper_save_path)
        with open(mapper_save_path, "wb") as f:
            pickle.dump(mapper, f, protocol=5)
    
    logging.info("Finding connected components...")
    components, n_components = find_connected_components(G)
    del G
    gc.collect()
    logging.info(f"Number of connected components: {n_components}")

    logging.info("Building reverse mapper...")
    reverse_mapper = {value: key for key, value in tqdm.tqdm(mapper.items(), unit="docs", unit_scale=True)}
    del mapper
    gc.collect()

    # dump pickled cc on disk and load if needed
    logging.info(f"Saving connected components to {args.out_file}...")
    ensure_directory_exists(args.out_file)
    with open(args.out_file, "wb") as fout:
        pickle.dump((components, n_components, reverse_mapper), fout, protocol=5)
    logging.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True, help="Input directory containing text files with duplicate pairs")
    parser.add_argument(
        "--out-file", type=str, required=True, 
        help="Output pickle file to save connected components. Prefix will be used for saving intermediate things as well (sets of duplicates, graph)"
    )
    parser.add_argument("--workers", type=int, default=1, help="Number of workers for processing text files with duplicate pairs")
    parser.add_argument("--nk-threads", type=int, default=96, help="Number of threads for graph processing")
    parser.add_argument("--from-scratch", action="store_true", help="Start from scratch ignoring any intermediate files")
    args = parser.parse_args()
    generate_connected_components_mp(args)
