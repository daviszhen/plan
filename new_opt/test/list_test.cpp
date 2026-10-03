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

#include "list.h"

namespace {

struct Node {
  explicit Node(int value) : value(value) {}

  int value;
  opt::SLink link;
};

void Check(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "check failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

void CheckOrder(opt::List<Node> &list,
                std::initializer_list<Node *> expected) {
  auto it = expected.begin();
  Node *current = list.First();
  Node *previous = nullptr;
  for (; it != expected.end(); ++it) {
    Check(current == *it, "forward order");
    Check(list.Prev(current) == previous, "previous link");
    previous = current;
    current = list.Next(current);
  }
  Check(current == nullptr, "forward traversal terminates");
  Check(previous == list.Last(), "last element");
}

void TestListInsertAndRemove() {
  opt::List<Node> list;
  list.Init(static_cast<uint32_t>(offsetof(Node, link)));
  Check(list.IsEmpty(), "new list is empty");
  Check(list.Size() == 0, "new list has zero size");
  Check(list.First() == nullptr && list.Last() == nullptr,
        "new list has no endpoints");

  Node front(0);
  Node first(1);
  Node middle(2);
  Node second(3);
  Node tail(4);

  list.Append(&first);
  list.Append(&second);
  list.Prepend(&front);
  list.Append(&tail, &second);
  list.Prepend(&middle, &first);

  Check(list.Size() == 5, "insertions update size");
  CheckOrder(list, {&front, &middle, &first, &second, &tail});

  list.Remove(&first);
  Check(list.Size() == 4, "remove updates size");
  Check(first.link._next == nullptr && first.link._prev == nullptr,
        "removed node is isolated");
  CheckOrder(list, {&front, &middle, &second, &tail});

  Check(list.RemoveHead() == &front, "remove head returns head");
  Check(list.RemoveTail() == &tail, "remove tail returns tail");
  CheckOrder(list, {&middle, &second});
  Check(list.RemoveTail() == &second, "remove last tail");
  Check(list.RemoveHead() == &middle, "remove last head");
  Check(list.IsEmpty() && list.Size() == 0, "removing all nodes empties list");
  Check(list.First() == nullptr && list.Last() == nullptr,
        "empty list has no endpoints");
}

void TestSyncListLifo() {
  opt::SyncList<Node> stack;
  stack.Init(static_cast<uint32_t>(offsetof(Node, link)));
  Node one(1);
  Node two(2);
  Node three(3);
  void *previous_marker = &one;
  one.link._prev = previous_marker;

  Check(stack.IsEmpty(), "new sync list is empty");
  stack.Push(&one);
  stack.Push(&two);
  stack.Push(&three);
  Check(!stack.IsEmpty(), "push updates stack");
  Check(stack.PtFirst() == &three, "stack head is most recent push");
  Check(stack.Next(&three) == &two, "stack next link");
  Check(stack.Next(&two) == &one, "stack traversal reaches oldest node");
  Check(stack.Next(&one) == nullptr, "stack traversal terminates");
  Check(one.link._prev == previous_marker,
        "push leaves the previous link untouched");

  Check(stack.Pop() == &three, "pop is LIFO");
  Check(three.link._next == nullptr, "pop clears the popped next link");
  Check(stack.Pop() == &two, "second pop is LIFO");
  Check(two.link._next == nullptr, "second pop clears the popped next link");
  Check(stack.Pop() == &one, "third pop is LIFO");
  Check(one.link._next == nullptr && one.link._prev == previous_marker,
        "pop leaves previous link untouched");
  Check(stack.Pop() == nullptr, "empty pop returns null");
  Check(stack.IsEmpty(), "pop empties stack");

  // As in CSyncList, a popped node can be reused once its next link is clear.
  stack.Push(&two);
  Check(stack.Pop() == &two, "popped node can be reused");
}

} // namespace

int main() {
  TestListInsertAndRemove();
  TestSyncListLifo();
  return EXIT_SUCCESS;
}
