import os
import datasets

DATA_BASE = os.environ.get("DATA_BASE")

data = datasets.load_dataset(
    "ArtifactAI/arxiv_s2orc_parsed",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/arxiv"))

data = datasets.load_dataset(
    "monology/pile-uncopyrighted",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/pile-uncopyrighted"))

data = datasets.load_dataset(
    "allenai/peS2o",
    split="train",
    trust_remote_code=True,
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/peS2o"))

data = datasets.load_dataset(
    "allenai/c4", "en",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/c4-en"))

data = datasets.load_dataset(
    "tiiuae/falcon-refinedweb",
    split="train",
    download_config=datasets.DownloadConfig(
        num_proc=8,
        resume_download=True,
    )
)
print(data)
data.save_to_disk(os.path.join(DATA_BASE, "raw/refinedweb"))
