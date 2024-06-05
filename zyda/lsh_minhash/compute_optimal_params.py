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

import argparse

from datasketch.lsh import _false_negative_probability, _false_positive_probability, _optimal_param

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-perm', type=int, default=128, help='Number of permutations in minhash')
    parser.add_argument('--threshold', type=float, default=0.4, help='Jaccard similarity threshold to declare a pair duplicate')
    parser.add_argument('--fn-weight', type=float, default=0.5, help='Weight of false negatives when determining optimal parameters')
    parser.add_argument('--fp-weight', type=float, default=0.5, help='Weight of false positives when determining optimal parameters')
    args = parser.parse_args()

    b, r = _optimal_param(
        threshold=args.threshold, 
        num_perm=args.num_perm,
        false_negative_weight=args.fn_weight, 
        false_positive_weight=args.fp_weight,
    )
    
    print(f"\nOptimal LSH index parameters: b = {b}, r = {r}")
    print(f"FN probability = {_false_negative_probability(threshold=args.threshold, b=b, r=r) * 100:.2f}%")
    print(f"FP probability = {_false_positive_probability(threshold=args.threshold, b=b, r=r) * 100:.2f}%")
