import setuptools

NAME = "zyda"
AUTHOR = "Zyphra Technologies"
VERSION = "0.0.1"
DESCRIPTION = "Processing of LLM datasets at a large scale"

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

# Setting up
setuptools.setup(
    name=NAME, 
    version=VERSION,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Zyphra/Zyda_processing",  
    packages=setuptools.find_packages(
        exclude=(
            "dist",
            "zyda.egg-info",
        )        
    ),
    install_requires=[
        "datasketch==1.5.9",
        "networkit==10.1",
        "nltk==3.8.1",
        "numpy==1.24.3",
        "regex==2023.6.3",
        "scipy==1.10.1",
        "tqdm==4.65.0",
        "ftfy==6.1.1",
        "more-itertools==9.1.0",
        "Levenshtein==0.25.1",
        "zstandard==0.22.0",
        "transformers==4.41.2",
        "datasets==2.18.0",
    ],
    python_requires=">=3.10",
        
)