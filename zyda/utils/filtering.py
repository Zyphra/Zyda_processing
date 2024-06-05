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

from typing import Dict, Optional

FILTERING_FEATURES = ["mean_word_length", "fraction_non_alphanumeric", "fraction_numerical", "pii_count", "pattern_counts", "word_list_counts", "substrings_counts"]

MIN_LENGTH = 100
MIN_MEAN_WORD_LENGTH = 3
MAX_MEAN_WORD_LENGTH = 12
MAX_FRACTION_NON_ALPHANUMERIC = 0.3
MAX_FRACTION_NUMERICAL = 0.2
MAX_CURSED_FRACTION = 0.01
MAX_NUM_REPEATED_SUBSTRINGS = 50
PATTERNS_WITH_MAX_COUNTS = {
    "xml": 10,
    "<?xml version=": 1,
    "lorem ipsum": 2,
}
PATTERNS_WITH_MAX_FRACTIONS = {
    "https://": 0.002,
    "<": 0.03,
    "\":": 0.002,
    "www.": 0.01
}
WORD_LISTS_WITH_MAX_COUNTS = {
    "zh_pornsignals.json": 1,
    "sexual_word_list.json": 10,
}
WORD_LISTS_WITH_MAX_FRACTIONS = {
    "sexual_word_list.json": 0.001,
    "profanity_word_list.json": 0.0025,
    "cursed_substrings.json": 0.01,
}

def filter(
    row,
    key: str = "transformed_text",
    dupe_inds: Optional[set] = None,
    min_length: int = MIN_LENGTH,
    min_mean_word_length: int = MIN_MEAN_WORD_LENGTH,
    max_mean_word_length: int = MAX_MEAN_WORD_LENGTH,
    max_fraction_non_alphanumeric: float = MAX_FRACTION_NON_ALPHANUMERIC,
    max_fraction_numerical: float = MAX_FRACTION_NUMERICAL,
    max_repeated_substrings: int = MAX_NUM_REPEATED_SUBSTRINGS,
    patterns_with_max_counts: Dict[str, int] = PATTERNS_WITH_MAX_COUNTS,
    patterns_with_max_fractions: Dict[str, float] = PATTERNS_WITH_MAX_FRACTIONS,
    word_lists_with_max_counts: Dict[str, int] = WORD_LISTS_WITH_MAX_COUNTS,
    word_lists_with_max_fractions: Dict[str, float] = WORD_LISTS_WITH_MAX_FRACTIONS,
) -> bool:
    if len(row[key]) < min_length:
        return False
    
    if row["mean_word_length"] < min_mean_word_length:
        return False
    
    if row["mean_word_length"] > max_mean_word_length:
        return False
    
    if row["fraction_non_alphanumeric"] > max_fraction_non_alphanumeric:
        return False
    
    if row["fraction_numerical"] > max_fraction_numerical:
        return False
    
    if row["substrings_counts"] > max_repeated_substrings:
        return False

    for pattern, max_count in patterns_with_max_counts.items():
        if row["pattern_counts"][pattern] > max_count:
            return False
        
    for pattern, max_fraction in patterns_with_max_fractions.items():
        if row["pattern_counts"][pattern] / len(row[key]) > max_fraction:
            return False
        
    for word_list, max_count in word_lists_with_max_counts.items():
        if row["word_list_counts"][word_list] > max_count:
            return False

    for word_list, max_fraction in word_lists_with_max_fractions.items():
        if row["word_list_counts"][word_list] / len(row[key]) > max_fraction:
            return False

    if dupe_inds is not None and row["global_index"] in dupe_inds:
        return False

    return True
