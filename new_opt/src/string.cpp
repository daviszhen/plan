#include "string.h"
#include <cassert>
#include <cstdint>

namespace opt {

const wchar_t CWStringBase::m_empty_wcstr = OPT_WSZ_LIT('\0');

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Copy
//
//	@doc:
//		Creates a deep copy of the string
//
//---------------------------------------------------------------------------
CWStringConst *
CWStringBase::Copy() const
{
	return new CWStringConst(nullptr, GetBuffer());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::IsValid
//
//	@doc:
//		Checks if the string is properly NULL-terminated
//
//---------------------------------------------------------------------------
bool
CWStringBase::IsValid() const
{
	return (Length() == OPT_WSZ_LENGTH(GetBuffer()));
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::operator ==
//
//	@doc:
//		Equality operator on strings
//
//---------------------------------------------------------------------------
bool
CWStringBase::operator==(const CWStringBase &str) const
{
	return Equals(&str);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Length()
//
//	@doc:
//		Returns the length of the string in number of wide characters,
//		not counting the terminating '\0'
//
//---------------------------------------------------------------------------
uint32_t
CWStringBase::Length() const
{
	return m_length;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Equals
//
//	@doc:
//		Checks whether the string is byte-wise equal to another string
//
//---------------------------------------------------------------------------
bool
CWStringBase::Equals(const CWStringBase *str) const
{
	assert(NULL != str);
	return Equals(str->GetBuffer());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Equals
//
//	@doc:
//		Checks whether the string is byte-wise equal to a string literal
//
//---------------------------------------------------------------------------
bool
CWStringBase::Equals(const wchar_t *w_str_buffer) const
{
	assert(NULL != w_str_buffer);
	uint32_t length = OPT_WSZ_LENGTH(w_str_buffer);
	if (Length() == length &&
		0 == wcsncmp(GetBuffer(), w_str_buffer, length))
	{
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::IsEmpty
//
//	@doc:
//		Checks whether the string is empty
//
//---------------------------------------------------------------------------
bool
CWStringBase::IsEmpty() const
{
	return (0 == Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::Find
//
//	@doc:
//		Returns the index of the first occurrence of a character, -1 if not found
//
//---------------------------------------------------------------------------
int32_t
CWStringBase::Find(wchar_t wc) const
{
	const wchar_t *w_str = GetBuffer();
	const uint32_t length = Length();

	for (uint32_t i = 0; i < length; i++)
	{
		if (wc == w_str[i])
		{
			return i;
		}
	}

	return -1;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::HasEscapedCharAt
//
//	@doc:
//		Checks if a character is escaped
//
//---------------------------------------------------------------------------
bool
CWStringBase::HasEscapedCharAt(uint32_t offset) const
{
	assert(!IsEmpty());
	assert(Length() > offset);

	const wchar_t *w_str_buffer = GetBuffer();

	for (uint32_t i = offset; i > 0; i--)
	{
		// check for escape character
		if (OPT_WSZ_LIT('\\') != w_str_buffer[i - 1])
		{
			if (0 == ((offset - i) & uint32_t(1)))
			{
				return false;
			}
			else
			{
				return true;
			}
		}
	}

	// reached beginning of string
	if (0 == (offset & uint32_t(1)))
	{
		return false;
	}
	else
	{
		return true;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::CountOccurrencesOf
//
//	@doc:
//		Count how many times the character appears in string
//
//---------------------------------------------------------------------------
uint32_t
CWStringBase::CountOccurrencesOf(const wchar_t wc) const
{
	uint32_t occurrences = 0;
	uint32_t length = Length();
	const wchar_t *buf = GetBuffer();

	for (uint32_t i = 0; i < length; i++)
	{
		if (wc == buf[i] && !HasEscapedCharAt(i))
		{
			occurrences++;
		}
	}
	return occurrences;
}

CWStringConst::CWStringConst(const wchar_t *w_str_buffer)
    : CWStringBase(OPT_WSZ_LENGTH(w_str_buffer),
                   false // owns_memory
                   ),
      m_w_str_buffer(w_str_buffer) {
  assert(NULL != w_str_buffer);
  assert(IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Initializes a constant string by making a copy of the given
//character buffer. 		The string owns the memory.
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst(void *mp, const wchar_t *w_str_buffer)
    : CWStringBase(OPT_WSZ_LENGTH(w_str_buffer),
                   true // owns_memory
                   ),
      m_w_str_buffer(NULL) {
  assert(NULL != mp);
  assert(NULL != w_str_buffer);

  if (0 == m_length) {
    // string is empty
    m_w_str_buffer = &m_empty_wcstr;
  } else {
    // make a copy of the string
    wchar_t *w_str_temp_buffer = new wchar_t[m_length + 1];
    wcsncpy(w_str_temp_buffer, w_str_buffer, m_length + 1);
    m_w_str_buffer = w_str_temp_buffer;
  }

  assert(IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::CWStringConst
//
//	@doc:
//		Shallow copy constructor.
//
//---------------------------------------------------------------------------
CWStringConst::CWStringConst(const CWStringConst &str)
    : CWStringBase(str.Length(),
                   false // owns_memory
                   ),
      m_w_str_buffer(str.GetBuffer()) {
  assert(NULL != m_w_str_buffer);
  assert(IsValid());
}
//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::~CWStringConst
//
//	@doc:
//		Destroys a constant string. This involves releasing the
//character buffer 		provided the string owns it.
//
//---------------------------------------------------------------------------
CWStringConst::~CWStringConst() {
  if (m_owns_memory && m_w_str_buffer != &m_empty_wcstr) {
    delete[] m_w_str_buffer;
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringConst::GetBuffer
//
//	@doc:
//		Returns the wide character buffer
//
//---------------------------------------------------------------------------
const wchar_t *CWStringConst::GetBuffer() const { return m_w_str_buffer; }


//---------------------------------------------------------------------------
//	@function:
//		CWString::CWString
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWString::CWString(uint32_t length)
	: CWStringBase(length,
				   true	 // owns_memory
				   ),
	  m_w_str_buffer(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::GetBuffer
//
//	@doc:
//		Returns the wide character buffer storing the string
//
//---------------------------------------------------------------------------
const wchar_t *
CWString::GetBuffer() const
{
	return m_w_str_buffer;
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Append
//
//	@doc:
//		Appends a string to the current string
//
//---------------------------------------------------------------------------
void
CWString::Append(const CWStringBase *str)
{
	assert(NULL != str);
	if (0 < str->Length())
	{
		AppendBuffer(str->GetBuffer());
	}
	assert(IsValid());
}
} // namespace opt