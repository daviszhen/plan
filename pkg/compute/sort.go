// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

const (
	//size <= this, insert sort
	insertion_sort_threshold = 24

	//partitions size > this, use ninther to choice pivot
	ninther_threshold = 128

	//
	partial_insertion_sort_limit = 8

	block_size = 64

	cacheline_size = 64
)

func log2(diff int) int {
	log := 0
	for {
		diff >>= 1
		if diff <= 0 {
			break
		}
		log++
	}
	return log
}
