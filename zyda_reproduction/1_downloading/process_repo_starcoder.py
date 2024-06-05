import os
import time
from collections import defaultdict
import datasets

NUM_PROC = os.environ.get("NUM_PROC", 1)
REPO_PATH = os.environ.get("STARCODER_REPO_PATH")
DATA_BASE = os.environ.get("DATA_BASE")

all_dirs = sorted([item for item in os.listdir(REPO_PATH) if os.path.isdir(os.path.join(REPO_PATH, item)) and ".git" not in item])
print(all_dirs)

has_different_features = set(
    [
        'jupyter-scripts-dedup-filtered',
        'jupyter-structured-clean-dedup',
        'github-issues-filtered-structured',
        'git-commits-cleaned',
    ]
)

type2data = defaultdict(list)
for key in all_dirs:
    print(f"\nLoading {key}")
    t0 = time.time()
    data_raw = datasets.load_dataset(
        os.path.join(REPO_PATH, key),
        num_proc=NUM_PROC,
        split='train'
    )
    data_with_dir = data_raw.map(lambda row: {"dir": key}, num_proc=NUM_PROC)
    
    new_features = data_with_dir.features.copy()
    for feature in new_features:
        new_features[feature] = datasets.Value(dtype="string")
    data_casted = data_with_dir.cast(new_features, num_proc=NUM_PROC)
    
    if key in has_different_features:
        type2data[key].append(data_casted)
    else:
        type2data['languages'].append(data_casted)

print()
for key, datas in type2data.items():
    print(f"{key}: {sum([len(data) for data in datas])}")

for key, datas in type2data.items():
    print(f"\nSaving {key}")
    conc_dataset = datasets.concatenate_datasets(datas)
    conc_dataset.save_to_disk(os.path.join(DATA_BASE, f"raw/starcoder-{key}"))
