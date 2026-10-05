#ifndef __ARRAY_H__
#define __ARRAY_H__

#include <cstdint>
#include <iostream>
#include "refcount.h"
#include "utils.h"

namespace opt {

// forward declaration
template <class T, void (*CleanupFn)(T *)>
class CDynamicPtrArray;

// comparison function signature
typedef int32_t (*CompareFn)(const void *, const void *);

// frequently used destroy functions

// NOOP
template <class T>
inline void
CleanupNULL(T *)
{
}

// plain delete
template <class T>
inline void
CleanupDelete(T *elem)
{
	delete elem;
}

// delete of array
template <class T>
inline void
CleanupDeleteArray(T *elem)
{
	delete [] elem;
}

// release ref-count'd object
template <class T>
inline void
CleanupRelease(T *elem)
{
	(dynamic_cast<CRefCount *>(elem))->Release();
}

// commonly used array types

// arrays of unsigned integers
typedef CDynamicPtrArray<uint32_t, CleanupDelete> ULongPtrArray;
// array of unsigned integer arrays
typedef CDynamicPtrArray<ULongPtrArray, CleanupRelease> ULongPtr2dArray;

// arrays of integers
typedef CDynamicPtrArray<int32_t, CleanupDelete> IntPtrArray;

// array of strings
// typedef CDynamicPtrArray<CWStringBase, CleanupDelete> StringPtrArray;

// arrays of chars
typedef CDynamicPtrArray<int8_t, CleanupDelete> CharPtrArray;

template <class T, void (*CleanupFn)(T *)>
class CDynamicPtrArray : public CRefCount
{
private:
	// currently allocated size
	uint32_t m_capacity;

	// min size
	uint32_t m_min_size;

	// current size
	uint32_t m_size;

	// expansion factor
	uint32_t m_expansion_factor;

	// actual array
	T **m_elems;

	// comparison function for pointers
	static int32_t
	PtrCmp(const void *p1, const void *p2)
	{
		uintptr_t ulp1 = *(uintptr_t *) p1;
		uintptr_t ulp2 = *(uintptr_t *) p2;

		if (ulp1 < ulp2)
		{
			return -1;
		}

		if (ulp1 > ulp2)
		{
			return 1;
		}

		return 0;
	}

	// private copy ctor
	CDynamicPtrArray<T, CleanupFn>(const CDynamicPtrArray<T, CleanupFn> &)=delete;

	// resize function
	void
	Resize(uint32_t new_size)
	{
		assert(new_size > m_capacity &&
					"Invalid call to Resize, cannot shrink array");

		// get new target array
		T **new_elems = new T*[new_size];

		if (m_size > 0)
		{
			assert(nullptr != m_elems);
			memcpy(new_elems, m_elems, sizeof(T *) * m_size);
			delete [] m_elems;
		}

		m_elems = new_elems;
		m_capacity = new_size;
	}

public:
	// ctor
	explicit CDynamicPtrArray<T, CleanupFn>(uint32_t min_size = 4,
											uint32_t expansion_factor = 10)
		:
		  m_capacity(0),
		  m_min_size(std::max((uint32_t) 4, min_size)),
		  m_size(0),
		  m_expansion_factor(std::max((uint32_t) 2, expansion_factor)),
		  m_elems(nullptr)
	{
		assert(nullptr != CleanupFn && "No valid destroy function specified");

		// do not allocate in constructor; defer allocation to first insertion
	}

	// dtor
	~CDynamicPtrArray<T, CleanupFn>()
	{
		Clear();

		delete [] m_elems;
	}

	// clear elements
	void
	Clear()
	{
		for (uint32_t i = 0; i < m_size; i++)
		{
			CleanupFn(m_elems[i]);
		}
		m_size = 0;
	}

	// append element to end of array
	void
	Append(T *elem)
	{
		if (m_size == m_capacity)
		{
			// resize at least by 4 elements or percentage as given by ulExp
			uint32_t new_size =
				(uint32_t)(m_capacity * (1 + (m_expansion_factor / 100.0)));
			uint32_t min_expand_size = m_capacity + 4;

			Resize(std::max(std::max(min_expand_size, new_size), m_min_size));
		}

		assert(m_size < m_capacity);

		m_elems[m_size] = elem;
		++m_size;
	}

	// append array -- flatten it
	void
	AppendArray(const CDynamicPtrArray<T, CleanupFn> *arr)
	{
		assert(nullptr != arr);
		assert(this != arr && "Cannot append array to itself");

		uint32_t total_size = m_size + arr->m_size;
		if (total_size > m_capacity)
		{
			Resize(total_size);
		}

		assert(m_size <= m_capacity);
		OPT_ASSERT_IMP(m_size == m_capacity, 0 == arr->m_size);

		assert(total_size <= m_capacity);

		// at this point old memory is no longer accessible, hence, no self-copy
		if (arr->m_size > 0)
		{
			assert(nullptr != arr->m_elems);
			memcpy(m_elems + m_size, arr->m_elems,
						 arr->m_size * sizeof(T *));
		}

		m_size = total_size;
	}


	// number of elements currently held
	uint32_t
	Size() const
	{
		return m_size;
	}

	// sort array
	void
	Sort(CompareFn compare_func = PtrCmp)
	{
		qsort(m_elems, m_size, sizeof(T *), compare_func);
	}

	// equality check
	bool
	Equals(const CDynamicPtrArray<T, CleanupFn> *arr) const
	{
		bool is_equal = (Size() == arr->Size());

		for (uint32_t i = 0; i < m_size && is_equal; i++)
		{
			is_equal = (m_elems[i] == arr->m_elems[i]);
		}

		return is_equal;
	}

	// lookup object
	T *
	Find(const T *elem) const
	{
		assert(nullptr != elem);

		for (uint32_t i = 0; i < m_size; i++)
		{
			if (*m_elems[i] == *elem)
			{
				return m_elems[i];
			}
		}

		return nullptr;
	}

	// lookup object position
	uint32_t
	IndexOf(const T *elem) const
	{
		assert(nullptr != elem);

		for (uint32_t ul = 0; ul < m_size; ul++)
		{
			if (*m_elems[ul] == *elem)
			{
				return ul;
			}
		}

		return UINT32_MAX;
	}

	// check if array is sorted
	bool
	IsSorted() const
	{
		for (uint32_t i = 1; i < m_size; i++)
		{
			if ((uintptr_t)(m_elems[i - 1]) > (uintptr_t)(m_elems[i]))
			{
				return false;
			}
		}

		return true;
	}

	// accessor for n-th element
	T *
	operator[](uint32_t pos) const
	{
		assert(pos < m_size && "Out of bounds access");
		return (T *) m_elems[pos];
	}

	// replace an element in the array
	void
	Replace(uint32_t pos, T *new_elem)
	{
		assert(pos < m_size && "Out of bounds access");
		CleanupFn(m_elems[pos]);
		m_elems[pos] = new_elem;
	}

	// swap two array entries
	void
	Swap(uint32_t pos1, uint32_t pos2)
	{
		assert(pos1 < m_size && pos2 < m_size &&
					"Swap positions out of bounds");
		T *temp = m_elems[pos1];

		m_elems[pos1] = m_elems[pos2];
		m_elems[pos2] = temp;
	}

	// return the last element of the array and at the same time remove it from the array
	T *
	RemoveLast()
	{
		if (0 == m_size)
		{
			return nullptr;
		}

		return m_elems[--m_size];
	}

	// return the indexes of first appearances of elements of the first array
	// in the second array if the first array is not included in the second,
	// return nullptr
	// equality comparison between elements is via the "==" operator
	ULongPtrArray *
	IndexesOfSubsequence(CDynamicPtrArray<T, CleanupFn> *subsequence)
	{
		assert(nullptr != subsequence);

		uint32_t subsequence_length = subsequence->Size();
		ULongPtrArray *indexes = new ULongPtrArray();

		for (uint32_t ul1 = 0; ul1 < subsequence_length; ul1++)
		{
			T *elem = (*subsequence)[ul1];
			uint32_t index = IndexOf(elem);
			if (UINT32_MAX == index)
			{
				// not found
				indexes->Release();
				return nullptr;
			}

			indexes->Append(new uint32_t(index));
		}
		return indexes;
	}

	// Eliminate members from an array that are not contained in a given list of indexes
	CDynamicPtrArray<T, CleanupFn> *
	CreateReducedArray(ULongPtrArray *indexes_to_choose)
	{
		CDynamicPtrArray<T, CleanupFn> *result =
			new CDynamicPtrArray<T, CleanupFn>( m_min_size,
														  m_expansion_factor);
		uint32_t list_size = indexes_to_choose->Size();

		for (uint32_t i = 0; i < list_size; i++)
		{
			result->Append((*this)[*((*indexes_to_choose)[i])]);
		}
		return result;
	}

};	// class CDynamicPtrArray

}

#endif