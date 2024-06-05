import os
import datasets
from tqdm import tqdm
import zstandard as zstd
import io

# Generating Slimpajama jsonl file directly from the repo, since loading Slimpajama from HF repository has a bug
# that results in doubling the dataset.

REPO_PATH = os.environ.get("SLIMPAJAMA_REPO_PATH")
DATA_BASE = os.environ.get("DATA_BASE")
OUTPUT_JSONL = os.path.join(REPO_PATH, "SlimPajama.jsonl")

def list_files_in_directory(directory):
    file_list = []
    for root, directories, files in os.walk(directory):
        for filename in files:
            file_list.append(os.path.join(root, filename))
    return file_list


def combine_jsonl_files(input_folder, output_file):

    jsonl_files = sorted([f for f in list_files_in_directory(input_folder) if f.endswith(".jsonl")])
    zst_files = sorted([f for f in list_files_in_directory(input_folder) if f.endswith(".jsonl.zst")])

    with tqdm(total=len(jsonl_files) + len(zst_files), desc='Combining JSONL files') as pbar:
        with open(output_file, 'w') as out_f:
            for filename in jsonl_files:
                input_file = os.path.join(input_folder, filename)
                with open(input_file, 'r') as f:
                    for line in f:
                        out_f.write(line)
                pbar.update(1)

            for filename in zst_files:
                input_file = os.path.join(input_folder, filename)
                with open(input_file, 'rb') as f:
                    decompressor = zstd.ZstdDecompressor()
                    text_stream = io.TextIOWrapper(decompressor.stream_reader(f), encoding='utf-8')
                    for line in text_stream:
                        out_f.write(line)
                pbar.update(1)

combine_jsonl_files(
    input_folder=os.path.join(REPO_PATH, "train"),
    output_file=OUTPUT_JSONL,
)

data = datasets.Dataset.from_json(OUTPUT_JSONL, split='train')

print(data)

print('Saving the dataset...')
data.save_to_disk(os.path.joing(DATA_BASE, "raw/slimpajama"))
