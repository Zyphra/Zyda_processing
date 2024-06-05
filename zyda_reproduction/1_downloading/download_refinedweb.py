import os
import datasets

DATA_BASE = os.environ.get("DATA_BASE")
NUM_PROC = int(os.environ.get("NUM_PROC", 1))

data = datasets.load_dataset(
    "tiiuae/falcon-refinedweb",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    ),
    num_proc=NUM_PROC,
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/refinedweb"))
