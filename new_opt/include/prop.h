#ifndef __PROP_H__
#define __PROP_H__

#include "refcount.h"

namespace opt {

class CReqdProp : public CRefCount
{
public:
	// types of required properties
	enum EPropType
	{
		EptRelational,
		EptPlan,

		EptSentinel
	};

private:
	// private copy ctor
	CReqdProp(const CReqdProp &);

public:
	// ctor
	CReqdProp();

	// dtor
	virtual ~CReqdProp();

	// is it a relational property?
	virtual bool
	FRelational() const
	{
		return false;
	}

	// is it a plan property?
	virtual bool
	FPlan() const
	{
		return false;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl,
						 CReqdProp *prpInput, uint32_t child_index,
						 CDrvdPropArray *pdrgpdpCtxt, uint32_t ulOptReq) = 0;

};	// class CReqdProp


}

#endif