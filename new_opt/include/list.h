#ifndef __LIST_H__
#define __LIST_H__

#include "link.h"
#include "utils.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
namespace opt {

template <typename T> class SyncList;

template <typename T> class List {
private:
  friend class SyncList<T>;

  uint32_t _offset; // offset of link element
  uint32_t _size;
  T *_head;
  T *_tail;

  List(const List &) = delete;

  // get SLink part from pointer
  SLink &Link(const void *ptr) const {
    assert(ALIGNED_64(ptr));
    assert(ALIGNED_64(_offset));
    assert(_offset != UINT32_MAX);

    SLink &link = *(SLink *)(((uint8_t *)ptr) + _offset);
    return link;
  }

public:
  List() : _offset(UINT32_MAX), _size(0), _head(nullptr), _tail(nullptr) {}

  void Init(uint32_t offset) {
    assert(ALIGNED_64(offset));
    _offset = offset;
  }

  void Prepend(T *elem) {
    assert(elem != nullptr);
    if (_head == nullptr) {
      assert(nullptr == _tail);
      assert(0 == _size);
      SLink &link = Link(elem);
      link._next = link._prev = nullptr;
      _head = _tail = elem;
      _size++;
    } else {
      Prepend(elem, _head);
    }
    assert(0 != _size);
  }

  void Append(T *elem) {
    assert(nullptr != elem);
    if (_tail == nullptr) {
      Prepend(elem);
    } else {
      Append(elem, _tail);
    }
    assert(0 != _size);
  }

  void Prepend(T *elem, T *next_elem) {
    assert(nullptr != elem);
    assert(nullptr != next_elem);
    T *prev = static_cast<T *>(Link(next_elem)._prev);

    // setup elem
    SLink &link = Link(elem);
    link._next = next_elem;
    link._prev = prev;

    // setup next_elem
    Link(next_elem)._prev = elem;

    // setup prev
    if (prev != nullptr) {
      Link(prev)._next = elem;
    } else {
      _head = elem;
    }
    _size++;
  }

  void Append(T *elem, T *prev_elem) {
    assert(nullptr != elem);
    assert(nullptr != prev_elem);
    T *next = static_cast<T *>(Link(prev_elem)._next);

    // setup current elem
    SLink &sl = Link(elem);
    sl._prev = prev_elem;
    sl._next = next;

    // setup prev_elem
    Link(prev_elem)._next = elem;

    // setup next
    if (nullptr != next) {
      Link(next)._prev = elem;
    } else {
      _tail = elem;
    }

    _size++;
  }

  // unlink elem itself from the list
  void Remove(T *elem) {
    assert(nullptr != elem);
    assert(nullptr != _head);
    assert(nullptr != _tail);

    SLink &link = Link(elem);

    // link next.prev = elem.prev
    if (link._next) {
      Link(link._next)._prev = link._prev;
    } else {
      assert(_tail == elem);
      // elem is last. tail is elem.prev now
      _tail = static_cast<T *>(link._prev);
    }

    // link prev.next = elem.next
    if (link._prev) {
      Link(link._prev)._next = link._next;
    } else {
      assert(_head == elem);
      // elem is head. head is link.next now
      _head = static_cast<T *>(link._next);
    }

    link._prev = nullptr;
    link._next = nullptr;

    _size--;
  }

  T *RemoveHead() {
    T *head = _head;
    Remove(_head);
    return head;
  }

  T *RemoveTail() {
    T *tail = _tail;
    Remove(_tail);
    return tail;
  }

  // just head
  T *First() const { return _head; }

  // just tail
  T *Last() const { return _tail; }

  // elem.next. elem must be in the list.
  T *Next(const T *pt) const {
    assert(nullptr != pt);
    assert(nullptr != _head);
    assert(nullptr != _tail);
    SLink &sl = Link(pt);
    return static_cast<T *>(sl._next);
  }

  // elem.prev. elem must be in the list.
  T *Prev(const T *pt) const {
    assert(nullptr != pt);
    assert(nullptr != _head);
    assert(nullptr != _tail);
    SLink &sl = Link(pt);
    return static_cast<T *>(sl._prev);
  }

  // lookup a given element in the list
  OPT_RESULT
  Find(T *elem) const {
    assert(NULL != elem);

    // iterate until found
    T *t = First();
    while (t) {
      if (elem == t) {
        return OPT_OK;
      }
      t = Next(t);
    }
    return OPT_NOT_FOUND;
  }

  uint32_t Size() const { return _size; }

  bool IsEmpty() const { return nullptr == First(); }
};

// not thread safe
// stack
template <typename T> class SyncList {
private:
  List<T> _list;
  SyncList(const SyncList &) = delete;

public:
  SyncList() {}
  ~SyncList() {}

  void Init(uint32_t offset) { _list.Init(offset); }

  // pust element at head
  void Push(T *elem) {
    assert(nullptr != elem);
    assert(_list.First() != elem);

    SLink &link = _list.Link(elem);

    assert(nullptr == link._next);

    T *head = _list.First();

    assert(elem != head && "Element is already inserted");

    // elem.next = head
    link._next = head;

    // set element as head
    assert(_list._head == head);
    _list._head = elem;
  }

  // remove head
  T *Pop() {
    T *old_head = nullptr;

    // get current head
    old_head = _list.First();
    if (nullptr != old_head) {
      // second element becomes the new head
      SLink &link = _list.Link(old_head);
      T *new_head = static_cast<T *>(link._next);

      assert(_list._head == old_head);
      // reset head to next elem
      _list._head = new_head;
      // reset link
      link._next = nullptr;
    }

    return old_head;
  }

  // get first element
  T *PtFirst() {
    _list._tail = _list._head;
    return _list.First();
  }

  // get next element
  T *Next(T *elem) {
    _list._tail = _list._head;
    return _list.Next(elem);
  }

  // check if list is empty
  bool IsEmpty() const { return nullptr == _list.First(); }
};
} // namespace opt

#endif
