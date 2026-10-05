#ifndef __HASH_TABLE_H__
#define __HASH_TABLE_H__

#include "list.h"
#include <cstdint>

namespace opt {

// prototypes
template <class T, class K> class SyncHashtableAccessorBase;

template <class T, class K> class SyncHashtableAccessByKey;

template <class T, class K> class SyncHashtableIter;

template <class T, class K> class SyncHashtableAccessByIter;

template <class T, class K> class SyncHashtable {
  // accessor and iterator classes are friends
  friend class SyncHashtableAccessorBase<T, K>;
  friend class SyncHashtableAccessByKey<T, K>;
  friend class SyncHashtableAccessByIter<T, K>;
  friend class SyncHashtableIter<T, K>;

private:
  // hash bucket is a list of entries
  struct SBucket {
  private:
    // no copy ctor
    SBucket(const SBucket &);

  public:
    // ctor
    SBucket() {};

    // hash chain
    List<T> m_chain;
  };

  // range of buckets
  SBucket *m_buckets;

  // number of ht buckets
  uint32_t m_nbuckets;

  // number of ht entries
  uintptr_t m_size;

  // offset of key. before the value
  uint32_t m_key_offset;

  // invalid key - needed for iteration
  const K *m_invalid_key;

  // pointer to hashing function
  uint32_t (*m_hashfn)(const K &);

  // pointer to key equality function
  bool (*m_eqfn)(const K &, const K &);

  // function to compute bucket index for key
  uint32_t GetBucketIndex(const K &key) const {
    assert(IsValid(key) && "Invalid key is inaccessible");

    return m_hashfn(key) % m_nbuckets;
  }

  // function to get bucket by index
  SBucket &GetBucket(const uint32_t index) const {
    assert(index < m_nbuckets && "Invalid bucket index");

    return m_buckets[index];
  }

  // extract key out of type
  K &Key(T *value) const {
    assert(UINT32_MAX != m_key_offset && "Key offset not initialized.");

    // move to the value
    K &k = *(K *)((uint8_t *)value + m_key_offset);

    return k;
  }

  // key validity check
  bool IsValid(const K &key) const { return !m_eqfn(key, *m_invalid_key); }

public:
  // type definition of function used to cleanup element
  typedef void (*DestroyEntryFuncPtr)(T *);

  // ctor
  SyncHashtable<T, K>()
      : m_buckets(nullptr), m_nbuckets(0), m_size(0), m_key_offset(UINT32_MAX),
        m_invalid_key(nullptr), m_hashfn(nullptr), m_eqfn(nullptr) {}

  // dtor
  // deallocates hashtable internals, does not destroy
  // client objects
  ~SyncHashtable<T, K>() { Cleanup(); }

  // Initialization of hashtable
  void Init(uint32_t size, uint32_t link_offset, uint32_t key_offset,
            const K *invalid_key, uint32_t (*func_hash)(const K &),
            bool (*func_equal)(const K &, const K &)) {
    assert(NULL == m_buckets);
    assert(0 == m_nbuckets);
    assert(NULL != invalid_key);
    assert(NULL != func_hash);
    assert(NULL != func_equal);

    m_nbuckets = size;
    m_key_offset = key_offset;
    m_invalid_key = invalid_key;
    m_hashfn = func_hash;
    m_eqfn = func_equal;

    m_buckets = new SBucket[m_nbuckets];
    for (uint32_t i = 0; i < m_nbuckets; i++) {
      m_buckets[i].m_chain.Init(link_offset);
    }
  }

  // dealloc bucket range and reset members
  void Cleanup() {
    delete[] m_buckets;
    m_buckets = NULL;

    m_nbuckets = 0;
    m_size = 0;
    m_key_offset = UINT32_MAX;
    m_invalid_key = nullptr;
    m_hashfn = nullptr;
    m_eqfn = nullptr;
  }

  // iterate over all entries and call destroy function on each entry
  void DestroyEntries(DestroyEntryFuncPtr pfunc_destroy) {
    T *value = NULL;
    SyncHashtableIter<T, K> it(*this);

    // since removing an entry will automatically advance iter's
    // position, we need to make sure that advance iter is called
    // only when we do not have an entry to delete
    while (NULL != value || it.Advance()) {
      if (NULL != value) {
        pfunc_destroy(value);
      }

      {
        SyncHashtableAccessByIter<T, K> acc(it);
        if (NULL != (value = acc.Value())) {
          acc.Remove(value);
        }
      }
    }
  }

  // insert function;
  void Insert(T *value) {
    K &key = Key(value);

    assert(IsValid(key));

    // determine target bucket
    SBucket &bucket = GetBucket(GetBucketIndex(key));

    // inserting at bucket's head is required by hashtable iteration
    bucket.m_chain.Prepend(value);

    // increase number of entries
    m_size++;
  }

  // return number of entries
  uintptr_t Size() const { return m_size; }

}; // class SyncHashtable

template <class T, class K> class SyncHashtableAccessorBase {
private:
  // shorthand for buckets
  typedef struct SyncHashtable<T, K>::SBucket SBucket;

  // target hashtable
  SyncHashtable<T, K> &m_ht;

  // bucket to operate on
  SBucket &m_bucket;

  // no copy ctor
  SyncHashtableAccessorBase<T, K>(const SyncHashtableAccessorBase<T, K> &) =
      delete;

protected:
  // ctor - protected to restrict instantiation to children
  SyncHashtableAccessorBase<T, K>(SyncHashtable<T, K> &ht, uint32_t bucket_idx)
      : m_ht(ht), m_bucket(m_ht.GetBucket(bucket_idx)) {}

  // dtor
  virtual ~SyncHashtableAccessorBase<T, K>() {}

  // accessor to hashtable
  SyncHashtable<T, K> &GetHashTable() const { return m_ht; }

  // accessor to maintained bucket
  SBucket &GetBucket() const { return m_bucket; }

  // returns the first element in the hash chain
  T *First() const { return m_bucket.m_chain.First(); }

  // finds the element next to the given one
  T *Next(T *value) const {
    assert(NULL != value);

    // make sure element is in this hash chain
    assert(OPT_OK == m_bucket.m_chain.Find(value));

    return m_bucket.m_chain.Next(value);
  }

  // inserts element at the head of hash chain
  void Prepend(T *value) {
    assert(NULL != value);

    m_bucket.m_chain.Prepend(value);

    // increase number of entries
    m_ht.m_size++;
  }

  // adds first element before second element
  void Prepend(T *value, T *ptNext) {
    assert(NULL != value);

    // make sure element is in this hash chain
    assert(OPT_OK == m_bucket.m_chain.Find(ptNext));

    m_bucket.m_chain.Prepend(value, ptNext);

    // increase number of entries
    m_ht.m_size++;
  }

  // adds first element after second element
  void Append(T *value, T *ptPrev) {
    assert(NULL != value);

    // make sure element is in this hash chain
    assert(OPT_OK == m_bucket.m_chain.Find(ptPrev));

    m_bucket.m_chain.Append(value, ptPrev);

    // increase number of entries
    m_ht.m_size++;
  }

public:
  // unlinks element
  void Remove(T *value) {
    // not NULL and is-list-member checks are done in CList
    m_bucket.m_chain.Remove(value);

    // decrease number of entries
    m_ht.m_size--;
  }

}; // class SyncHashtableAccessorBase

template <class T, class K>
class SyncHashtableAccessByKey : public SyncHashtableAccessorBase<T, K> {
private:
  // shorthand for accessor's base class
  typedef class SyncHashtableAccessorBase<T, K> Base;

  // target key
  const K &m_key;

  // no copy ctor
  SyncHashtableAccessByKey<T, K>(const SyncHashtableAccessByKey<T, K> &);

  // finds the first element matching target key starting from
  // the given element
  T *NextMatch(T *value) const {
    T *curr = value;

    while (NULL != curr && !Base::GetHashTable().m_eqfn(
                               Base::GetHashTable().Key(curr), m_key)) {
      curr = Base::Next(curr);
    }

    return curr;
  }

  // returns true if current bucket matches key
  bool CurrentBucketMatchesKey(const K &key) const {
    uint32_t bucket_idx = Base::GetHashTable().GetBucketIndex(key);

    return &(Base::GetHashTable().GetBucket(bucket_idx)) ==
           &(Base::GetBucket());
  }

public:
  // ctor
  SyncHashtableAccessByKey<T, K>(SyncHashtable<T, K> &ht, const K &key)
      : Base(ht, ht.GetBucketIndex(key)), m_key(key) {}

  // dtor
  virtual ~SyncHashtableAccessByKey() {}

  // finds the first bucket's element with a matching key
  T *Find() const { return NextMatch(Base::First()); }

  // finds the next element with a matching key
  T *Next(T *value) const {
    assert(NULL != value);

    return NextMatch(Base::Next(value));
  }

  // insert at head of target bucket's hash chain
  void Insert(T *value) {
    assert(NULL != value);

    K &key = Base::GetHashTable().Key(value);

    // make sure this is a valid key
    assert(Base::GetHashTable().IsValid(key));

    // make sure this is the right bucket
    assert(CurrentBucketMatchesKey(key));

    // inserting at bucket's head is required by hashtable iteration
    Base::Prepend(value);
  }

}; // class SyncHashtableAccessByKey

template <class T, class K>
class SyncHashtableAccessByIter : public SyncHashtableAccessorBase<T, K>
{
	// iterator class is a friend
	friend class SyncHashtableIter<T, K>;

private:
	// shorthand for base class
	typedef class SyncHashtableAccessorBase<T, K> Base;

	// target iterator
	SyncHashtableIter<T, K> &m_iter;

	// no copy ctor
	SyncHashtableAccessByIter<T, K>(const SyncHashtableAccessByIter<T, K> &)=delete;

	// returns the first valid element starting from the given element
	T *
	FirstValid(T *value) const
	{
		assert(NULL != value);

		T *curr = value;
		while (NULL != curr &&
			   !Base::GetHashTable().IsValid(Base::GetHashTable().Key(curr)))
		{
			curr = Base::Next(curr);
		}

		return curr;
	}

public:
	// ctor
	explicit SyncHashtableAccessByIter<T, K>(SyncHashtableIter<T, K> &iter)
		: Base(iter.m_ht, iter.m_bucket_idx), m_iter(iter)
	{
	}

	// returns the element pointed to by iterator
	T *
	Value() const
	{
		assert(m_iter.m_invalid_elem_inserted &&
					"Iterator's advance is not called");

		// advance in the current bucket until finding a valid element;
		// this is needed because the next valid element pointed to by
		// iterator might have been deleted by another client just before
		// using the accessor

		return FirstValid(m_iter.m_invalid_elem);
	}

};	// class SyncHashtableAccessByIter

template <class T, class K>
class SyncHashtableIter
{
	// iterator accessor class is a friend
	friend class SyncHashtableAccessByIter<T, K>;

private:
	// target hashtable
	SyncHashtable<T, K> &m_ht;

	// index of bucket to operate on
	uint32_t m_bucket_idx;

	// a slab of memory to manufacture an invalid element; we enforce
	// memory alignment here to mimic allocation of a class object
	ALIGN_STORAGE uint8_t m_invalid_elem_data[sizeof(T)];

	// a pointer to memory slab to interpret it as invalid element
	T *m_invalid_elem;

	// no copy ctor
	SyncHashtableIter<T, K>(const SyncHashtableIter<T, K> &)=delete;

	// inserts invalid element at the head of current bucket
	void
	InsertInvalidElement()
	{
		m_invalid_elem_inserted = false;
		while (m_bucket_idx < m_ht.m_nbuckets)
		{
			SyncHashtableAccessByIter<T, K> acc(*this);

			T *first = acc.First();
			T *first_valid = NULL;

			if (NULL != first &&
				(NULL != (first_valid = acc.FirstValid(first))))
			{
				// insert invalid element before the found element
				acc.Prepend(m_invalid_elem, first_valid);
				m_invalid_elem_inserted = true;
				break;
			}
			else
			{
				m_bucket_idx++;
			}
		}
	}

	// advances invalid element in current bucket
	void
	AdvanceInvalidElement()
	{
		SyncHashtableAccessByIter<T, K> acc(*this);

		T *value = acc.FirstValid(m_invalid_elem);

		acc.Remove(m_invalid_elem);
		m_invalid_elem_inserted = false;

		// check that we did not find the last element in bucket
		if (NULL != value && NULL != acc.Next(value))
		{
			// insert invalid element after the found element
			acc.Append(m_invalid_elem, value);
			m_invalid_elem_inserted = true;
		}
	}

	// a flag indicating if invalid element is currently in the hash table
	bool m_invalid_elem_inserted;

public:
	// ctor
	explicit SyncHashtableIter<T, K>(SyncHashtable<T, K> &ht)
		: m_ht(ht),
		  m_bucket_idx(0),
		  m_invalid_elem(NULL),
		  m_invalid_elem_inserted(false)
	{
		m_invalid_elem = (T *) m_invalid_elem_data;

		// get a reference to invalid element's key
		K &key = m_ht.Key(m_invalid_elem);

		// copy invalid key to invalid element's memory
		(void) memcpy(&key, m_ht.m_invalid_key,
							sizeof(*(m_ht.m_invalid_key)));
	}

	// dtor
	~SyncHashtableIter<T, K>()
	{
		if (m_invalid_elem_inserted)
		{
			// remove invalid element
			SyncHashtableAccessByIter<T, K> acc(*this);
			acc.Remove(m_invalid_elem);
		}
	}

	// advances iterator
	bool
	Advance()
	{
		assert(m_bucket_idx < m_ht.m_nbuckets &&
					"Advancing an exhausted iterator");

		if (!m_invalid_elem_inserted)
		{
			// first time to call iterator's advance, insert invalid
			// element into the first non-empty bucket
			InsertInvalidElement();
		}
		else
		{
			AdvanceInvalidElement();

			if (!m_invalid_elem_inserted)
			{
				// current bucket is exhausted, insert invalid element
				// in the next unvisited non-empty bucket
				m_bucket_idx++;
				InsertInvalidElement();
			}
		}

		return (m_bucket_idx < m_ht.m_nbuckets);
	}

	// rewinds the iterator to the beginning
	void
	Rewind()
	{
		assert(m_bucket_idx >= m_ht.m_nbuckets &&
					"Rewinding an un-exhausted iterator");
		assert(
			!m_invalid_elem_inserted &&
			"Invalid element from previous iteration exists, cannot rewind");

		m_bucket_idx = 0;
	}

};	// class SyncHashtableIter

} // namespace opt

#endif
