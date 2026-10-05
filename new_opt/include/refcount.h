#ifndef __REF_COUNT_H__
#define __REF_COUNT_H__

#include <cassert>
#include <cstdint>

namespace opt {
class CHeapObject
{
protected:
	CHeapObject() {}
};

class CRefCount : public CHeapObject
{
private:
	// reference counter -- first in class to be in sync with Check()
	uintptr_t m_refs;

	// private copy ctor
	CRefCount(const CRefCount &)=delete;

public:
	// ctor
	CRefCount() : m_refs(1)
	{
	}

	// dtor
	virtual ~CRefCount()
	{
		// enforce strict ref-counting unless we're in a pending exception,
		// e.g., a ctor has thrown
		assert( 0 == m_refs);
	}

	// return ref-count
	uintptr_t
	RefCount() const
	{
		return m_refs;
	}

	// return true if calling object's destructor is allowed
	virtual bool
	Deletable() const
	{
		return true;
	}

	// count up
	void
	AddRef()
	{
		m_refs++;
	}

	// count down
	void
	Release()
	{
		m_refs--;

		if (0 == m_refs)
		{
			if (!Deletable())
			{
				// restore ref-count
				AddRef();

				// deletion is not allowed
				assert(0);
			}

			delete this;
		}
	}

	// safe version of Release -- handles NULL pointers
	static void
	SafeRelease(CRefCount *rc)
	{
		if (nullptr != rc)
		{
			rc->Release();
		}
	}
};	// class CRefCount

}


#endif
