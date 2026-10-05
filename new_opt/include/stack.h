#ifndef __STACK_H__
#define __STACK_H__

#include "array.h"

namespace opt {

template <class T>
class CStack
{
private:
	// backing dynamic array store
	CDynamicPtrArray<T, CleanupNULL> *m_dynamic_ptr_array;

	// top of stack index
	uint32_t m_size;

	// copy c'tor - not defined
	CStack(CStack &)=delete;

public:
	// c'tor
	CStack<T>( uint32_t min_size = 4) : m_size(0)
	{
		m_dynamic_ptr_array =
			new CDynamicPtrArray<T, CleanupNULL>(min_size, 10);
	}

	// destructor
	virtual ~CStack()
	{
		m_dynamic_ptr_array->Release();
	}

	// push element onto stack
	void
	Push(T *obj)
	{
		assert(m_dynamic_ptr_array != NULL && "Dynamic array missing");
		assert(m_size <= m_dynamic_ptr_array->Size() &&
					"The top of stack cannot be beyond the underlying array");

		// if the stack was Popped before, reuse that space by replacing the element
		if (m_size < m_dynamic_ptr_array->Size())
		{
			m_dynamic_ptr_array->Replace(m_size, obj);
		}
		else
		{
			m_dynamic_ptr_array->Append(obj);
		}

		m_size++;
	}

	// pop top element
	T *
	Pop()
	{
		assert(!this->IsEmpty() && "Cannot pop from empty stack");

		T *obj = (*m_dynamic_ptr_array)[m_size - 1];
		m_size--;
		return obj;
	}

	// peek at top element
	const T *
	Peek() const
	{
		assert(!this->IsEmpty() && "Cannot peek into empty stack");

		const T *obj = (*m_dynamic_ptr_array)[m_size - 1];

		return obj;
	}

	// is stack empty?
	bool
	IsEmpty() const
	{
		return (m_size == 0);
	}
};

}
#endif