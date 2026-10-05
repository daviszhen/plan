#ifndef __STRING_H__
#define __STRING_H__

#include <cstdint>
#include <cwchar>
#include "utils.h"

#define OPT_WSZ_LENGTH(x) wcslen(x)
#define OPT_WSZ_STR_LENGTH(x) OPT_WSZ_LIT(x), OPT_WSZ_LENGTH(OPT_WSZ_LIT(x))

#define WCHAR_EOS OPT_WSZ_LIT('\0')

namespace opt {
class CWStringConst;

class CWStringBase
{
private:
	// private copy ctor
	CWStringBase(const CWStringBase &);

protected:
	// represents end-of-wide-string character
	static const wchar_t m_empty_wcstr;

	// size of the string in number of wchar_t units (not counting the terminating '\0')
	uint32_t m_length;

	// whether string owns its memory and should take care of deallocating it at destruction time
	bool m_owns_memory;

	// checks whether the string is byte-wise equal to a given string literal
	virtual bool Equals(const wchar_t *w_str_buffer) const;

public:
	// ctor
	CWStringBase(uint32_t length, bool owns_memory)
		: m_length(length), m_owns_memory(owns_memory)
	{
	}

	// dtor
	virtual ~CWStringBase()
	{
	}

	// deep copy of the string
	virtual CWStringConst *Copy() const;

	// accessors
	virtual uint32_t Length() const;

	// checks whether the string is byte-wise equal to another string
	virtual bool Equals(const CWStringBase *str) const;

	// checks whether the string contains any characters
	virtual bool IsEmpty() const;

	// checks whether a string is properly null-terminated
	bool IsValid() const;

	// equality operator
	bool operator==(const CWStringBase &str) const;

	// returns the wide character buffer storing the string
	virtual const wchar_t *GetBuffer() const = 0;

	// returns the index of the first occurrence of a character, -1 if not found
	int32_t Find(wchar_t wc) const;

	// checks if a character is escaped
	bool HasEscapedCharAt(uint32_t offset) const;

	// count how many times the character appears in string
	uint32_t CountOccurrencesOf(const wchar_t wc) const;
};

class CWStringConst : public CWStringBase
{
private:
	// null terminated wide character buffer
	const wchar_t *m_w_str_buffer;

public:
	// ctors
	CWStringConst(const wchar_t *w_str_buffer);
    CWStringConst(void *mp, const wchar_t *w_str_buffer);

	// shallow copy ctor
	CWStringConst(const CWStringConst &);

	//dtor
	~CWStringConst();

	// returns the wide character buffer storing the string
	const wchar_t *GetBuffer() const;
};

#define OPT_MAX_FMT_STR_LENGTH (10 * 1024 * 1024)	// 10MB

class CWString : public CWStringBase
{
protected:
	// null-terminated wide character buffer
	wchar_t *m_w_str_buffer;

	// appends the contents of a buffer to the current string
	virtual void AppendBuffer(const wchar_t *w_str_buffer) = 0;

public:
	// ctor
	CWString(uint32_t length);

	// dtor
	virtual ~CWString()
	{
	}

	// returns the wide character buffer storing the string
	const wchar_t *GetBuffer() const;

	// appends a string
	void Append(const CWStringBase *str);

	// appends a formatted string
	virtual void AppendFormat(const wchar_t *format, ...) = 0;

	// appends a string and replaces character with string
	virtual void AppendEscape(const CWStringBase *str, wchar_t wc,
							  const wchar_t *w_str_replace) = 0;

	// appends a null terminated character array
	virtual void AppendCharArray(const int8_t *sz) = 0;

	// appends a null terminated wide character array
	virtual void AppendWideCharArray(const wchar_t *w_str) = 0;

	// resets string
	virtual void Reset() = 0;
};

}

#endif