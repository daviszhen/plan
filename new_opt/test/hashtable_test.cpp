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

#include <cstddef>
#include <cstdlib>
#include <initializer_list>
#include <iostream>
#include <vector>

#include "hashtable.h"

namespace {

struct Entry {
  Entry(int key, int value) : key(key), value(value), link() {}

  int key;
  int value;
  opt::SLink link;
};

using HashTable = opt::SyncHashtable<Entry, int>;

void Check(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "check failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

uint32_t HashKey(const int &key) {
  return static_cast<uint32_t>(key);
}

bool EqualKey(const int &lhs, const int &rhs) { return lhs == rhs; }

void InitTable(HashTable &table, const int &invalid_key) {
  table.Init(static_cast<uint32_t>(3),
             static_cast<uint32_t>(offsetof(Entry, link)),
             static_cast<uint32_t>(offsetof(Entry, key)), &invalid_key,
             HashKey, EqualKey);
}

void CheckKeys(const std::vector<int> &actual,
               std::initializer_list<int> expected) {
  Check(actual.size() == expected.size(), "iteration count");

  auto actual_it = actual.begin();
  for (int expected_key : expected) {
    Check(*actual_it == expected_key, "iteration order");
    ++actual_it;
  }
}

std::vector<int> CollectKeys(opt::SyncHashtableIter<Entry, int> &iterator) {
  std::vector<int> keys;
  while (iterator.Advance()) {
    opt::SyncHashtableAccessByIter<Entry, int> access(iterator);
    Entry *entry = access.Value();
    Check(entry != nullptr, "iterator points at a valid entry");
    keys.push_back(entry->key);
  }
  return keys;
}

void TestInsertFindAndCollisions() {
  const int invalid_key = -1;
  HashTable table;
  InitTable(table, invalid_key);

  Entry key_one(1, 10);
  Entry key_four(4, 40);
  Entry duplicate_key_one(1, 11);
  Entry key_two(2, 20);
  Entry key_zero(0, 0);

  table.Insert(&key_one);
  table.Insert(&key_four);          // Same bucket as key_one.
  table.Insert(&duplicate_key_one); // Duplicate keys are allowed.
  table.Insert(&key_two);
  table.Insert(&key_zero);

  Check(table.Size() == 5, "direct inserts update size");

  int key = 1;
  opt::SyncHashtableAccessByKey<Entry, int> access(table, key);
  Check(access.Find() == &duplicate_key_one,
        "find returns the newest matching entry");
  Check(access.Next(&duplicate_key_one) == &key_one,
        "next skips non-matching entries in a collision chain");
  Check(access.Next(&key_one) == nullptr,
        "next terminates after the last matching entry");

  int colliding_key = 4;
  opt::SyncHashtableAccessByKey<Entry, int> collision_access(table,
                                                              colliding_key);
  Check(collision_access.Find() == &key_four,
        "find locates an entry in a collision chain");

  int missing_key = 5;
  opt::SyncHashtableAccessByKey<Entry, int> missing_access(table, missing_key);
  Check(missing_access.Find() == nullptr, "find returns null for a missing key");

  // Inserting through a key accessor is valid when the new key maps to the
  // accessor's bucket, even when it is different from the lookup key.
  Entry key_seven(7, 70);
  access.Insert(&key_seven); // 7 and 1 both map to bucket 1.
  Check(table.Size() == 6, "accessor insert updates size");

  int inserted_key = 7;
  opt::SyncHashtableAccessByKey<Entry, int> inserted_access(table,
                                                             inserted_key);
  Check(inserted_access.Find() == &key_seven,
        "accessor insert makes the new key searchable");
}

void TestIterationOrderAndRewind() {
  const int invalid_key = -1;
  HashTable table;
  InitTable(table, invalid_key);

  Entry key_one(1, 10);
  Entry key_four(4, 40);
  Entry duplicate_key_one(1, 11);
  Entry key_seven(7, 70);
  Entry key_two(2, 20);
  Entry key_zero(0, 0);

  table.Insert(&key_one);
  table.Insert(&key_four);
  table.Insert(&duplicate_key_one);
  table.Insert(&key_seven);
  table.Insert(&key_two);
  table.Insert(&key_zero);

  opt::SyncHashtableIter<Entry, int> iterator(table);
  CheckKeys(CollectKeys(iterator), {0, 7, 1, 4, 1, 2});
  Check(table.Size() == 6, "iteration does not change size");

  iterator.Rewind();
  CheckKeys(CollectKeys(iterator), {0, 7, 1, 4, 1, 2});
  Check(table.Size() == 6, "rewind does not change size");
}

int destroyed_entries = 0;

void DestroyEntry(Entry *entry) {
  ++destroyed_entries;
  delete entry;
}

void TestDestroyEntriesAndEmptyIteration() {
  const int invalid_key = -1;
  HashTable table;
  InitTable(table, invalid_key);

  opt::SyncHashtableIter<Entry, int> empty_iterator(table);
  Check(!empty_iterator.Advance(), "empty table has no iterator entries");
  empty_iterator.Rewind();
  Check(!empty_iterator.Advance(), "rewound empty table remains empty");

  destroyed_entries = 0;
  table.Insert(new Entry(0, 0));
  table.Insert(new Entry(1, 10));
  table.Insert(new Entry(2, 20));
  table.Insert(new Entry(4, 40));
  Check(table.Size() == 4, "heap entries are inserted");

  table.DestroyEntries(DestroyEntry);
  Check(destroyed_entries == 4, "destroy callback runs for every entry");
  Check(table.Size() == 0, "destroying entries empties the table");

  table.DestroyEntries(DestroyEntry);
  Check(destroyed_entries == 4,
        "destroying an already empty table does nothing");
}

void TestCleanupResetsTable() {
  const int invalid_key = -1;
  HashTable table;
  InitTable(table, invalid_key);

  Entry entry(0, 0);
  table.Insert(&entry);
  Check(table.Size() == 1, "entry exists before cleanup");

  table.Cleanup();
  Check(table.Size() == 0, "cleanup resets the entry count");
}

} // namespace

int main() {
  TestInsertFindAndCollisions();
  TestIterationOrderAndRewind();
  TestDestroyEntriesAndEmptyIteration();
  TestCleanupResetsTable();
  return EXIT_SUCCESS;
}
