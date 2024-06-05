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

from collections import defaultdict
from typing import Dict, List
import os
import argparse
import re
import json
import datasets
import transformers
from zyda.utils.text import get_normalized_words
from zyda.utils.filtering import filter

import nltk
nltk.download('punkt')

import logging
logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)

TOKENIZERS = {
    "neox": transformers.AutoTokenizer.from_pretrained("EleutherAI/gpt-neox-20b"),
}

def read_json_file(fname):
    with open(fname, "r") as f:
        result_dict = json.loads(f.read()) 
    return result_dict["words"]

WORD_LISTS = {
    "profanity_word_list.json": read_json_file("profanity_word_list.json"),
    "sexual_word_list.json": read_json_file("sexual_word_list.json"),
    "zh_pornsignals.json": read_json_file("zh_pornsignals.json"),
    "cursed_substrings.json": read_json_file("cursed_substrings.json"),
}

PATTERNS = ["xml", "<?xml version=", "lorem ipsum", "https://", "<", ">", "\":", "www."]

REPEATING_CHARACTER_THRESHOLD = 10
CHARS_FOR_TRANSFORM = {
    "-": REPEATING_CHARACTER_THRESHOLD,
    " ": 40,
    "_": REPEATING_CHARACTER_THRESHOLD,
    "/": REPEATING_CHARACTER_THRESHOLD,
    r"\\": REPEATING_CHARACTER_THRESHOLD,
    "\n": REPEATING_CHARACTER_THRESHOLD,
    "\t": 20,
    "\r": REPEATING_CHARACTER_THRESHOLD,
    r"\.": REPEATING_CHARACTER_THRESHOLD,
    ",": REPEATING_CHARACTER_THRESHOLD,
    ":": REPEATING_CHARACTER_THRESHOLD,
    r"\?": REPEATING_CHARACTER_THRESHOLD,
    "\xa0": REPEATING_CHARACTER_THRESHOLD,
}


REGEX_MEAN_WORD_LENGTH = re.compile(r'\s|-|/|\\|\.')
def mean_word_length(text: str):
    if text:
        words = REGEX_MEAN_WORD_LENGTH.split(text)
        return sum(len(word) for word in words) / len(words)
    return 0.0


REGEX_NON_ALPHANUMERIC = re.compile(r'[^\w\s]')
def fraction_non_alphanumeric(text: str):
    if text:
        return len(REGEX_NON_ALPHANUMERIC.findall(text)) / len(text)
    return 0.0


REGEX_COUNT_NUMERICS = re.compile(r'\d')
def fraction_numerical(text: str):
    if text:
        return len(REGEX_COUNT_NUMERICS.findall(text)) / len(text)
    return 0.0


def count_substrings(text: str, allowed_num_repeats: int = 7):
    substrings = re.findall(r'(\w)\1{%d,}' % (allowed_num_repeats - 1), text)
    return len(substrings)


def count_pattern(text: str, pattern: str):
    return len(re.findall(pattern, text))


def count_word_list(text: str, word_list: str):
    return sum([count_pattern(text, word) for word in word_list])

REGEX_EMAIL = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
REGEX_PHONE_NUMBER = re.compile(r'(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9]{1,2})\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9]{1,2})')
def count_PII_items(input_string):
    num_email = len(REGEX_EMAIL.findall(input_string))
    num_phone_number = len(REGEX_PHONE_NUMBER.findall(input_string))
    return num_email  + num_phone_number


def transform(
    text: str,
    chars_with_thresholds: dict =CHARS_FOR_TRANSFORM,
) -> str:
    new_text = text
    for char, threshold in chars_with_thresholds.items():
        pattern = char * threshold + '+'
        char = '?' if char == r'\?' else char
        char = '.' if char == r'\.' else char
        new_text = re.sub(pattern, char, new_text)
    return new_text


def preprocess(
    batch,
    indices,
    key: str,
    name: str,
    shard: int,
    offset: int,
    patterns: list = PATTERNS,
    word_lists: dict = WORD_LISTS,
) -> Dict[str, List]:
    texts = batch[key]
    features = defaultdict(list)
    for ind, text in zip(indices, texts):
        features["dataset_name"].append(name)
        features["shard"].append(shard)
        features["shard_index"].append(ind)
        features["global_index"].append(ind + offset)
        features["mean_word_length"].append(mean_word_length(text))
        features["fraction_non_alphanumeric"].append(fraction_non_alphanumeric(text))
        features["fraction_numerical"].append(fraction_numerical(text))
        features["pii_count"].append(count_PII_items(text))
        
        pattern_counts = {}
        for pattern in patterns:
            pattern_counts[pattern] = count_pattern(text, pattern)
        features["pattern_counts"].append(pattern_counts)
        
        word_list_counts = {}
        for word_list_key, word_list in word_lists.items():
            word_list_counts[word_list_key] = count_word_list(text, word_list)
        features["word_list_counts"].append(word_list_counts)

        feature_key = "pii_count"
        if feature_key not in batch.keys():
            features[feature_key].append(count_PII_items(text))

        feature_key = "n_tokens_neox"
        if feature_key not in batch.keys():
            tokenized = TOKENIZERS["neox"].encode(text)
            features[feature_key].append(len(tokenized))

        feature_key = "n_words"
        if feature_key not in batch.keys():
            words = get_normalized_words(text)
            features[feature_key].append(len(words))

        transformed_text = transform(text)
        features["transformed_text"].append(transformed_text)
        features["substrings_counts"].append(count_substrings(transformed_text))

    return features 


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--hf-path', type=str, required=True, help='Path of HF dataset')
    parser.add_argument('--hf-dir', type=str, default=None, help='Dir in HF dataset')
    parser.add_argument('--name', type=str, required=True, help='Descriptor for identifying the dataset')
    parser.add_argument('--load-from-disk', action='store_true', help='Use datasets.load_from_disk() to load the dataset')
    parser.add_argument('--num-proc', type=int, default=1, help='Number of processes for HF processing')
    parser.add_argument('--num-records-per-shard', type=int, default=20_000_000, help='Approximate number of records per shard')
    parser.add_argument('--key', type=str, default='text', help='Key to extract')
    parser.add_argument('--keep-key', action='store_true', help='If specified, key column will be saved in shards')
    parser.add_argument('--save-path', type=str, required=True, help='Folder to save processed HF dataset to')
    parser.add_argument('--from-scratch', action='store_true', help='If specified, will forcefully do every shard regardless of previous progress')

    args = parser.parse_args()
    print()

    logging.info(f"Loading {args.hf_path}, dir={args.hf_dir}")
    if args.load_from_disk:
        dataset = datasets.load_from_disk(args.hf_path)
    else:
        dataset = datasets.load_dataset(args.hf_path, args.hf_dir, num_proc=args.num_proc, split='train', trust_remote_code=True)
    
    logging.info(f"Loaded dataset:\n{dataset}")
    logging.info(f"Cache cleaned: {dataset.cleanup_cache_files()}")

    num_shards = 1
    if args.num_records_per_shard:
        num_shards = 1 + len(dataset) // args.num_records_per_shard
    
    offset = 0
    for i in range(num_shards):
        print()
        logging.info(f"Processing shard {i + 1} / {num_shards}. Current offset = {offset}.")
        save_path = f"{args.save_path}/shard_{i:02d}"
        if os.path.exists(save_path) and not args.from_scratch:
            logging.info(f"Already processed!")
            offset += len(datasets.load_from_disk(save_path))
            continue

        ds_shard = dataset.shard(num_shards=num_shards, index=i, contiguous=True)
        logging.info(f"Cache cleaned: {ds_shard.cleanup_cache_files()}")

        ds_shard_post = ds_shard.map(
            lambda batch, indices: preprocess(batch, indices, shard=i, offset=offset, key=args.key, name=args.name),
            batched=True,
            with_indices=True,
            remove_columns=None if args.keep_key else args.key,
            num_proc=args.num_proc,
        )

        if "starcoder" in args.name:
            logging.info(f"Starcoder detected: skipping filtering")
        else:
            ds_shard_post = ds_shard_post.filter(lambda row: filter(row), num_proc=args.num_proc)

        offset += len(ds_shard_post)
        ds_shard_post.save_to_disk(save_path, max_shard_size="8GB")
        logging.info(f"Cache cleaned: {ds_shard.cleanup_cache_files()}")
