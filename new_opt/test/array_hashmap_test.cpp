// Copyright 2026 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <iostream>
#include <vector>

#include "hashmap.h"

namespace {

void Check(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "check failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

struct Tracked {
  explicit Tracked(int value) : value(value) {}

  ~Tracked() { ++destroyed; }

  bool operator==(const Tracked &other) const { return value == other.value; }

  int value;
  static int destroyed;
};

int Tracked::destroyed = 0;

int32_t CompareTracked(const void *lhs, const void *rhs) {
  const Tracked *left = *static_cast<Tracked *const *>(lhs);
  const Tracked *right = *static_cast<Tracked *const *>(rhs);
  if (left->value < right->value) {
    return -1;
  }
  if (left->value > right->value) {
    return 1;
  }
  return 0;
}

using OwnedTrackedArray =
    opt::CDynamicPtrArray<Tracked, opt::CleanupDelete>;
using BorrowedIntArray = opt::CDynamicPtrArray<int, opt::CleanupNULL>;

void CheckTrackedValues(const OwnedTrackedArray *array,
                        std::initializer_list<int> expected) {
  Check(array->Size() == expected.size(), "array size");
  uint32_t index = 0;
  for (int expected_value : expected) {
    Check((*array)[index]->value == expected_value, "array value order");
    ++index;
  }
}

void TestDynamicPtrArrayMutation() {
  Tracked::destroyed = 0;
  OwnedTrackedArray *array = new OwnedTrackedArray(2, 10);
  Check(array->RefCount() == 1, "array starts with one reference");
  array->AddRef();
  Check(array->RefCount() == 2, "array reference count increments");
  array->Release();

  Tracked *first = new Tracked(3);
  Tracked *second = new Tracked(1);
  Tracked *third = new Tracked(2);
  array->Append(first);
  array->Append(second);
  array->Append(third);
  Check(array->Size() == 3, "append updates array size");
  Check((*array)[0] == first, "subscript returns stored pointer");

  Tracked probe(1);
  Check(array->Find(&probe) == second, "find compares pointed-to values");
  Check(array->IndexOf(&probe) == 1, "index of finds pointed-to value");
  Tracked missing(99);
  Check(array->Find(&missing) == nullptr, "find returns null when absent");
  Check(array->IndexOf(&missing) == UINT32_MAX,
        "index of returns UINT32_MAX when absent");

  array->Swap(0, 2);
  CheckTrackedValues(array, {2, 1, 3});
  Tracked *replacement = new Tracked(4);
  array->Replace(1, replacement);
  Check(Tracked::destroyed == 1, "replace cleans up the old pointer");
  CheckTrackedValues(array, {2, 4, 3});

  array->Sort(CompareTracked);
  CheckTrackedValues(array, {2, 3, 4});
  array->Sort();
  Check(array->IsSorted(), "default sort produces pointer-sorted array");

  array->Clear();
  Check(array->Size() == 0, "clear removes all elements");
  Check(Tracked::destroyed == 4, "clear cleans every remaining pointer");
  Check(array->RemoveLast() == nullptr, "remove last on empty array returns null");

  Tracked *last = new Tracked(5);
  array->Append(last);
  Check(array->RemoveLast() == last, "remove last returns ownership");
  Check(array->Size() == 0, "remove last updates size");
  delete last;
  Check(Tracked::destroyed == 5, "caller can clean removed pointer");

  array->Release();
  Check(Tracked::destroyed == 5, "array release does not double-clean pointers");
}

void TestDynamicPtrArrayComposition() {
  int one = 1;
  int two = 2;
  int three = 3;
  int four = 4;
  int missing = 99;

  BorrowedIntArray *left = new BorrowedIntArray();
  BorrowedIntArray *right = new BorrowedIntArray();
  left->Append(&one);
  left->Append(&two);
  right->Append(&three);
  right->Append(&four);

  left->AppendArray(right);
  Check(left->Size() == 4, "append array flattens the source array");
  Check(right->Size() == 2, "append array leaves source unchanged");
  Check((*left)[0] == &one && (*left)[3] == &four,
        "append array preserves pointer order");

  BorrowedIntArray *expected = new BorrowedIntArray();
  expected->Append(&one);
  expected->Append(&two);
  expected->Append(&three);
  expected->Append(&four);
  Check(left->Equals(expected), "equals compares array contents");

  BorrowedIntArray *subsequence = new BorrowedIntArray();
  subsequence->Append(&two);
  subsequence->Append(&four);
  opt::ULongPtrArray *indexes = left->IndexesOfSubsequence(subsequence);
  Check(indexes != nullptr, "subsequence indexes are returned");
  Check(indexes->Size() == 2 && *(*indexes)[0] == 1 && *(*indexes)[1] == 3,
        "subsequence indexes use first appearances");

  BorrowedIntArray *reduced = left->CreateReducedArray(indexes);
  Check(reduced->Size() == 2 && (*reduced)[0] == &two && (*reduced)[1] == &four,
        "reduced array selects indexed pointers");

  BorrowedIntArray *not_found = new BorrowedIntArray();
  not_found->Append(&missing);
  Check(left->IndexesOfSubsequence(not_found) == nullptr,
        "subsequence returns null when an element is absent");

  indexes->Release();
  reduced->Release();
  not_found->Release();
  subsequence->Release();
  expected->Release();
  right->Release();
  left->Release();
}

struct TrackedValue {
  explicit TrackedValue(int value) : value(value) {}

  ~TrackedValue() { ++destroyed; }

  int value;
  static int destroyed;
};

int TrackedValue::destroyed = 0;

uint32_t HashInt(const int *key) {
  return static_cast<uint32_t>(*key);
}

bool EqualInt(const int *lhs, const int *rhs) { return *lhs == *rhs; }

using TestHashMap = opt::CHashMap<int, TrackedValue, HashInt, EqualInt,
                                  opt::CleanupDelete, opt::CleanupDelete>;
using TestHashMapIter =
    opt::CHashMapIter<int, TrackedValue, HashInt, EqualInt,
                      opt::CleanupDelete, opt::CleanupDelete>;

struct MapEntry {
  int key;
  int value;
};

std::vector<MapEntry> CollectMapEntries(TestHashMap *map) {
  std::vector<MapEntry> entries;
  TestHashMapIter iterator(map);
  while (iterator.Advance()) {
    const int *key = iterator.Key();
    const TrackedValue *value = iterator.Value();
    Check(key != nullptr && value != nullptr, "map iterator returns entries");
    entries.push_back({*key, value->value});
  }
  return entries;
}

bool HasMapEntry(const std::vector<MapEntry> &entries, int key, int value) {
  for (const MapEntry &entry : entries) {
    if (entry.key == key && entry.value == value) {
      return true;
    }
  }
  return false;
}

void TestHashMapInsertReplaceDeleteAndIterate() {
  TrackedValue::destroyed = 0;
  TestHashMap *map = new TestHashMap(3);
  Check(map->RefCount() == 1, "map starts with one reference");
  map->AddRef();
  map->Release();

  int *key_one = new int(1);
  int *key_four = new int(4); // Same chain as key one.
  int *key_two = new int(2);
  Check(map->Insert(key_one, new TrackedValue(10)),
        "insert adds first map entry");
  Check(map->Insert(key_four, new TrackedValue(40)),
        "insert adds colliding map entry");
  Check(map->Insert(key_two, new TrackedValue(20)),
        "insert adds second-chain entry");
  Check(map->Size() == 3, "map size tracks inserts");

  int probe_one = 1;
  int probe_four = 4;
  int probe_missing = 99;
  Check(map->Find(&probe_one)->value == 10, "find returns value by key");
  Check(map->Find(&probe_four)->value == 40,
        "find works through a collision chain");
  Check(map->Find(&probe_missing) == nullptr,
        "find returns null for a missing key");

  int *duplicate_key = new int(1);
  TrackedValue *duplicate_value = new TrackedValue(100);
  int destroyed_before_duplicate = TrackedValue::destroyed;
  Check(!map->Insert(duplicate_key, duplicate_value),
        "duplicate key is rejected");
  Check(map->Size() == 3, "duplicate insert does not change size");
  delete duplicate_key;
  delete duplicate_value;
  Check(TrackedValue::destroyed == destroyed_before_duplicate + 1,
        "caller retains ownership after rejected insert");

  TrackedValue *replacement = new TrackedValue(11);
  Check(map->Replace(&probe_one, replacement), "replace updates an existing value");
  Check(map->Find(&probe_one)->value == 11, "replace is visible through find");
  Check(TrackedValue::destroyed == destroyed_before_duplicate + 2,
        "replace cleans up the previous value");

  TrackedValue *discarded = new TrackedValue(999);
  Check(!map->Replace(&probe_missing, discarded),
        "replace returns false for a missing key");
  delete discarded;

  std::vector<MapEntry> initial_entries = CollectMapEntries(map);
  Check(initial_entries.size() == 3, "iterator visits each inserted key");
  Check(HasMapEntry(initial_entries, 1, 11) &&
            HasMapEntry(initial_entries, 4, 40) &&
            HasMapEntry(initial_entries, 2, 20),
        "iterator returns key/value pairs");

	Check(map->Delete(&probe_four), "delete removes an existing key");
	Check(map->Find(&probe_four) == nullptr, "deleted key cannot be found");
	Check(!map->Delete(&probe_missing), "delete returns false for missing key");

	Check(map->Insert(new int(4), new TrackedValue(400)),
	        "a deleted key can be inserted again");
	Check(map->Delete(&probe_one), "delete removes a replaced value");

	map->Release();
  Check(TrackedValue::destroyed == 7,
        "map releases replaced, deleted, rejected, and live values exactly once");
}

} // namespace

int main() {
  TestDynamicPtrArrayMutation();
  TestDynamicPtrArrayComposition();
  TestHashMapInsertReplaceDeleteAndIterate();
  return EXIT_SUCCESS;
}
