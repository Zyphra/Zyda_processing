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

import string
import ftfy
import re
import nltk
nltk.download('punkt')

# Inspired by: https://github.com/Cerebras/modelzoo/blob/0bb30b6e681e792f3ba1804835d3f966a7ec9611/src/cerebras/modelzoo/data_preparation/nlp/slimpajama/dedup/to_hash.py#L32
def get_normalized_words(s: str):
    # normalize string
    s = ftfy.fix_text(s, normalization="NFC")
    # lower cased
    s = s.lower()
    # remove punctuation
    s = s.translate(str.maketrans("", "", string.punctuation))
    # remove consecutive spaces, newlines, tabs in the middle and in the beginning / end
    s = re.sub(r"\s+", " ", s.strip())
    # return words
    return nltk.word_tokenize(s)


def get_features(s: str, width: int):
    return map(lambda x: " ".join(x), nltk.ngrams(get_normalized_words(s), width))
