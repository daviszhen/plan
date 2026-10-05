#ifndef __META_DATA_H__
#define __META_DATA_H__

#include "refcount.h"

namespace opt {

class IMDInterface : public CRefCount
{
public:
	virtual ~IMDInterface()
	{
	}
};

class IMDCacheObject : public IMDInterface
{
protected:
	// Serialize a list of metadata id elements using pstrTokenList
	// as the root XML element for the list, and each metadata id is
	// serialized in the form of a pstrTokenListItem element.
	// The serialized information looks like this:
	// <strTokenList>
	//		<strTokenListItem .../>...
	// </strTokenList>
	static void SerializeMDIdList(CXMLSerializer *xml_serializer,
								  const IMdIdArray *mdid_array,
								  const CWStringConst *strTokenList,
								  const CWStringConst *strTokenListItem);

public:
	// type of md object
	enum Emdtype
	{
		EmdtRel,
		EmdtInd,
		EmdtFunc,
		EmdtAgg,
		EmdtOp,
		EmdtType,
		EmdtTrigger,
		EmdtCheckConstraint,
		EmdtRelStats,
		EmdtColStats,
		EmdtCastFunc,
		EmdtScCmp
	};

	// md id of cache object
	virtual IMDId *MDId() const = 0;

	// cache object name
	virtual CMDName Mdname() const = 0;

	// object type
	virtual Emdtype MDType() const = 0;

	// serialize object in DXL format
	virtual void Serialize(gpdxl::CXMLSerializer *) const = 0;

	// DXL string representation of cache object
	virtual const CWStringDynamic *GetStrRepr() const = 0;


	// serialize the metadata id information as the attributes of an
	// element with the given name
	virtual void SerializeMDIdAsElem(gpdxl::CXMLSerializer *xml_serializer,
									 const CWStringConst *element_name,
									 const IMDId *mdid) const;

#ifdef GPOS_DEBUG
	virtual void DebugPrint(IOstream &os) const = 0;
#endif
};

typedef CDynamicPtrArray<IMDCacheObject, CleanupRelease> IMDCacheObjectArray;

}

#endif