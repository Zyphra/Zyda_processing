import os
import datasets

DATA_BASE = os.environ.get("DATA_BASE")

data = datasets.load_dataset(
    "tiiuae/falcon-refinedweb",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "refinedweb"))
