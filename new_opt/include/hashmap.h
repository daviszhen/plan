#ifndef __HASH_MAP_H__
#define __HASH_MAP_H__

#include <cassert>
#include <cstdint>
#include "refcount.h"
#include "array.h"

namespace opt {

template <class K, class T, uint32_t (*HashFn)(const K *),
          bool (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
          void (*DestroyTFn)(T *)>
class CHashMapIter;

template <class K, class T, uint32_t (*HashFn)(const K *),
		  bool (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMap : public CRefCount
{
	// fwd declaration
	friend class CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>;

private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapElem
	//
	//	@doc:
	//		Anchor for key/value pair
	//
	//---------------------------------------------------------------------------
	class CHashMapElem
	{
	private:
		// key/value pair
		K *m_key;
		T *m_value;

		// own objects
		bool m_owns_objects;

		// private copy ctor
		CHashMapElem(const CHashMapElem &)=delete;

	public:
		// ctor
		CHashMapElem(K *key, T *value, bool fOwn)
			: m_key(key), m_value(value), m_owns_objects(fOwn)
		{
			assert(nullptr != key);
		}

		// dtor
		~CHashMapElem()
		{
			// in case of a temporary hashmap element for lookup we do NOT own the
			// objects, otherwise call destroy functions
			if (m_owns_objects)
			{
				DestroyKFn(m_key);
				DestroyTFn(m_value);
			}
		}

		// key accessor
		K *
		Key() const
		{
			return m_key;
		}

		// value accessor
		T *
		Value() const
		{
			return m_value;
		}

		// replace value
		void
		ReplaceValue(T *new_value)
		{
			if (m_owns_objects)
			{
				DestroyTFn(m_value);
			}
			m_value = new_value;
		}

		// equality operator -- map elements are equal if their keys match
		bool
		operator==(const CHashMapElem &elem) const
		{
			return EqFn(m_key, elem.m_key);
		}
	};

	// size
	uint32_t m_num_chains;

	// number of entries
	uint32_t m_size;

	// each hash chain is an array of hashmap elements
	typedef CDynamicPtrArray<CHashMapElem, CleanupDelete> CHashSetElemArray;
	CHashSetElemArray **const m_chains;

	// array for keys
	// We use CleanupNULL because the keys are owned by the hash table
	typedef CDynamicPtrArray<K, CleanupNULL> Keys;
	Keys *const m_keys;

	IntPtrArray *const m_filled_chains;

	// private copy ctor
	CHashMap(const CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> &)=delete;

	// lookup appropriate hash chain in static table, may be nullptr if
	// no elements have been inserted yet
	CHashSetElemArray **
	GetChain(const K *key) const
	{
		assert(nullptr != m_chains);
		return &m_chains[HashFn(key) % m_num_chains];
	}

	// clear elements
	void
	Clear()
	{
		for (uint32_t i = 0; i < m_filled_chains->Size(); i++)
		{
			// release each hash chain
			m_chains[*(*m_filled_chains)[i]]->Release();
		}
		m_size = 0;
		m_filled_chains->Clear();
	}

	// lookup an element by its key
	CHashMapElem *
	Lookup(const K *key) const
	{
		CHashMapElem hme(const_cast<K *>(key), nullptr /*T*/, false /*fOwn*/);
		CHashMapElem *found_hme = nullptr;
		CHashSetElemArray **chain = GetChain(key);
		if (nullptr != *chain)
		{
			found_hme = (*chain)->Find(&hme);
			OPT_ASSERT_IMP(nullptr != found_hme, *found_hme == hme);
		}

		return found_hme;
	}

public:
	// ctor
	CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>(
														 uint32_t num_chains = 127)
		: 
		  m_num_chains(num_chains),
		  m_size(0),
		  m_chains(new CHashSetElemArray * [m_num_chains]),
		  m_keys(new Keys()),
		  m_filled_chains(new IntPtrArray())
	{
		assert(m_num_chains > 0);
		(void) memset(m_chains, 0,
							m_num_chains * sizeof(CHashSetElemArray *));
	}

	// dtor
	~CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>()
	{
		// release all hash chains
		Clear();

		delete [] m_chains;
		m_keys->Release();
		m_filled_chains->Release();
	}

	// insert an element if key is not yet present
	bool
	Insert(K *key, T *value)
	{
		if (nullptr != Find(key))
		{
			return false;
		}

		CHashSetElemArray **chain = GetChain(key);
		if (nullptr == *chain)
		{
			*chain = new CHashSetElemArray();
			int32_t chain_idx = HashFn(key) % m_num_chains;
			m_filled_chains->Append(new int32_t(chain_idx));
		}

		CHashMapElem *elem =
			new CHashMapElem(key, value, true /*fOwn*/);
		(*chain)->Append(elem);

		m_size++;
		m_keys->Append(key);

		return true;
	}

	// lookup a value by its key
	T *
	Find(const K *key) const
	{
		CHashMapElem *elem = Lookup(key);
		if (nullptr != elem)
		{
			return elem->Value();
		}

		return nullptr;
	}

	// replace the value in a map entry with a new given value
	bool
	Replace(const K *key, T *ptNew)
	{
		assert(nullptr != key);

		bool fSuccess = false;
		CHashMapElem *elem = Lookup(key);
		if (nullptr != elem)
		{
			elem->ReplaceValue(ptNew);
			fSuccess = true;
		}

		return fSuccess;
	}

	bool
	Delete(const K *key)
	{
		CHashSetElemArray **chain = GetChain(key);

		if (nullptr != *chain)
		{
			for (uint32_t ul = 0; ul < (*chain)->Size(); ul++)
			{
				if (EqFn((**chain)[ul]->Key(), key))
				{
					// found the entry, now remove it by putting it last,
					// then removing the last element
					(*chain)->Swap(ul, (*chain)->Size() - 1);
					CHashMapElem *to_delete = (*chain)->RemoveLast();
					CleanupDelete(to_delete);
					return true;
				}
			}
		}
		return false;
	}

	// return number of map entries
	uint32_t
	Size() const
	{
		return m_size;
	}

};	// class CHashMap

template <class K, class T, uint32_t (*HashFn)(const K *),
		  bool (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMapIter
{
	// short hand for hashmap type
	typedef CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> TMap;

private:
	// map to iterate
	const TMap *m_map;

	// current hashchain
	uint32_t m_chain_idx;

	// current key
	uint32_t m_key_idx;

	// is initialized?
	bool m_is_initialized;

	// private copy ctor
	CHashMapIter(
		const CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> &);

	// method to return the current element
	const typename TMap::CHashMapElem *
	Get() const
	{
		typename TMap::CHashMapElem *elem = NULL;
		K *k = (*(m_map->m_keys))[m_key_idx - 1];
		elem = m_map->Lookup(k);

		return elem;
	}

public:
	// ctor
	CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>(TMap *ptm)
		: m_map(ptm), m_chain_idx(0), m_key_idx(0)
	{
		assert(NULL != ptm);
	}

	// dtor
	virtual ~CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>()
	{
	}

	// advance iterator to next element
	bool
	Advance()
	{
		if (m_key_idx < m_map->m_keys->Size())
		{
			m_key_idx++;
			return true;
		}

		return false;
	}

	// current key
	const K *
	Key() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (NULL != elem)
		{
			return elem->Key();
		}
		return NULL;
	}

	// current value
	const T *
	Value() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (NULL != elem)
		{
			return elem->Value();
		}
		return NULL;
	}

};	// class CHashMapIter


}
#endif
