import argparse
import os
import more_itertools
import multiprocessing as mp
import datasets
from zyda.utils.common import ensure_directory_exists

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)


def save_partitions_to_parquet(data, out_folder, total, indices):
    for idx in indices:
        shard = data.shard(num_shards=total, index=idx, contiguous=True)
        parquet_name = f"train-{idx:05d}-of-{total:05d}.parquet"
        save_path = os.path.join(out_folder, parquet_name)
        ensure_directory_exists(save_path)
        logging.info(f"Saving shard {idx}")
        shard.to_parquet(save_path) 


def convert_to_parquet(args):
    files = os.listdir(args.input_folder)
    sorted_files = sorted(files, key = lambda x: x.split("_")[1].split(".")[0])
    logging.info(f"Found {len(sorted_files)} files: {sorted_files}")
    
    files_paths = [os.path.join(args.input_folder, x) for x in sorted_files]
    ds = datasets.Dataset.from_json(files_paths, num_proc=args.num_proc)
    ds.cleanup_cache_files()
    
    logging.info("Removing index column")
    ds = ds.remove_columns("source_index")

    logging.info("Converting columns")
    ds = ds.map(
        lambda row: {
            "source_other": str(row["source_other"]),
            "filtering_features": str(row["filtering_features"]),
        }, 
        num_proc=48,
    )
    logging.info("Shuffling the dataset")
    ds = ds.shuffle()
    
    processes = []
    partitions = args.partitions
    out_folder = args.output_folder
    logging.info(f"Saving {partitions} parquets to {out_folder}")
    indices = [list(x) for x in more_itertools.divide(args.num_proc, range(partitions))]
    for process_id in range(args.num_proc):
        p = mp.Process(target=save_partitions_to_parquet, args=(ds, out_folder, partitions, indices[process_id]))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    logging.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-folder", type=str, required=True, help="Input folder with jsonl files.")
    parser.add_argument("--output-folder", type=str, required=True, help="Input folder for saving parquet files.")
    parser.add_argument('--num-proc', type=int, default=1, help="Number of processes for saving.")
    parser.add_argument("--partitions", type=int, default=1, help="Number of parquet partitions. Partitions will be distributed among saving processes.")

    args = parser.parse_args()
    
    convert_to_parquet(args)
