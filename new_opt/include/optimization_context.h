#ifndef __OPTIMIZATION_H__
#define __OPTIMIZATION_H__

#include "refcount.h"
#include "hashtable.h"
#include "hashmap.h"
#include "job.h"

namespace opt {

// forward declarations
class CGroup;
class CGroupExpression;
class CCostContext;
class COptimizationContext;
class CDrvdPropPlan;

// optimization context pointer definition
typedef COptimizationContext *OPTCTXT_PTR;

// array of optimization contexts
typedef CDynamicPtrArray<COptimizationContext, CleanupRelease>
	COptimizationContextArray;

//---------------------------------------------------------------------------
//	@class:
//		COptimizationContext
//
//	@doc:
//		Optimization context
//
//---------------------------------------------------------------------------
class COptimizationContext : public CRefCount
{
public:
	// states of optimization context
	enum EState
	{
		estUnoptimized,	 // initial state

		estOptimizing,	// ongoing optimization
		estOptimized,	// done optimization

		estSentinel
	};

private:
	// private copy ctor
	COptimizationContext(const COptimizationContext &);

	// unique id within owner group, used for debugging
	uint32_t m_id;

	// back pointer to owner group, used for debugging
	CGroup *m_pgroup;

	// required plan properties
	CReqdPropPlan *m_prpp;

	// required relational properties -- used for stats computation during costing
	CReqdPropRelational *m_prprel;

	// stats of previously optimized expressions
	IStatisticsArray *m_pdrgpstatCtxt;

	// index of search stage where context is generated
	uint32_t m_ulSearchStageIndex;

	// best cost context under the optimization context
	CCostContext *m_pccBest;

	// optimization context state
	EState m_estate;

	// is there a multi-stage Agg plan satisfying required properties
	bool m_fHasMultiStageAggPlan;

	// context's optimization job queue
	JobQueue m_jqOptimization;

	// internal matching function
	bool FMatchSortColumns(const COptimizationContext *poc) const;

	// private dummy ctor; used for creating invalid context
	COptimizationContext()
		: m_mp(NULL),
		  m_id(GPOPT_INVALID_OPTCTXT_ID),
		  m_pgroup(NULL),
		  m_prpp(NULL),
		  m_prprel(NULL),
		  m_pdrgpstatCtxt(NULL),
		  m_ulSearchStageIndex(0),
		  m_pccBest(NULL),
		  m_estate(estUnoptimized),
		  m_fHasMultiStageAggPlan(false){};

	// check if Agg node should be optimized for the given context
	static bool FOptimizeAgg(CMemoryPool *mp, CGroupExpression *pgexprParent,
							 CGroupExpression *pgexprAgg,
							 COptimizationContext *poc, uint32_t ulSearchStages);

	// check if Sort node should be optimized for the given context
	static bool FOptimizeSort(CMemoryPool *mp, CGroupExpression *pgexprParent,
							  CGroupExpression *pgexprSort,
							  COptimizationContext *poc, uint32_t ulSearchStages);

	// check if Motion node should be optimized for the given context
	static bool FOptimizeMotion(CMemoryPool *mp, CGroupExpression *pgexprParent,
								CGroupExpression *pgexprMotion,
								COptimizationContext *poc,
								uint32_t ulSearchStages);

	// check if NL join node should be optimized for the given context
	static bool FOptimizeNLJoin(CMemoryPool *mp, CGroupExpression *pgexprParent,
								CGroupExpression *pgexprMotion,
								COptimizationContext *poc,
								uint32_t ulSearchStages);

public:
	// ctor
	COptimizationContext(
		CMemoryPool *mp, CGroup *pgroup, CReqdPropPlan *prpp,
		CReqdPropRelational *
			prprel,	 // required relational props -- used during stats derivation
		IStatisticsArray
			*stats_ctxt,  // stats of previously optimized expressions
		uint32_t ulSearchStageIndex)
		: m_mp(mp),
		  m_id(GPOPT_INVALID_OPTCTXT_ID),
		  m_pgroup(pgroup),
		  m_prpp(prpp),
		  m_prprel(prprel),
		  m_pdrgpstatCtxt(stats_ctxt),
		  m_ulSearchStageIndex(ulSearchStageIndex),
		  m_pccBest(NULL),
		  m_estate(estUnoptimized),
		  m_fHasMultiStageAggPlan(false)
	{
		GPOS_ASSERT(NULL != pgroup);
		GPOS_ASSERT(NULL != prpp);
		GPOS_ASSERT(NULL != prprel);
		GPOS_ASSERT(NULL != stats_ctxt);
	}

	// dtor
	virtual ~COptimizationContext();

	// best group expression accessor
	CGroupExpression *PgexprBest() const;

	// match optimization contexts
	bool Matches(const COptimizationContext *poc) const;

	// get id
	uint32_t
	Id() const
	{
		return m_id;
	}

	// group accessor
	CGroup *
	Pgroup() const
	{
		return m_pgroup;
	}

	// required plan properties accessor
	CReqdPropPlan *
	Prpp() const
	{
		return m_prpp;
	}

	// required relatoinal properties accessor
	CReqdPropRelational *
	GetReqdRelationalProps() const
	{
		return m_prprel;
	}

	// stats of previously optimized expressions
	IStatisticsArray *
	Pdrgpstat() const
	{
		return m_pdrgpstatCtxt;
	}

	// search stage index accessor
	uint32_t
	UlSearchStageIndex() const
	{
		return m_ulSearchStageIndex;
	}

	// best cost context accessor
	CCostContext *
	PccBest() const
	{
		return m_pccBest;
	}

	// optimization job queue accessor
	CJobQueue *
	PjqOptimization()
	{
		return &m_jqOptimization;
	}

	// state accessor
	EState
	Est() const
	{
		return m_estate;
	}

	// is there a multi-stage Agg plan satisfying required properties
	bool
	FHasMultiStageAggPlan() const
	{
		return m_fHasMultiStageAggPlan;
	}

	// set optimization context id
	void
	SetId(uint32_t id)
	{
		GPOS_ASSERT(m_id == GPOPT_INVALID_OPTCTXT_ID);

		m_id = id;
	}

	// set optimization context state
	void
	SetState(EState estNewState)
	{
		GPOS_ASSERT(estNewState == (EState)(m_estate + 1));

		m_estate = estNewState;
	}

	// set best cost context
	void SetBest(CCostContext *pcc);

	// comparison operator for hashtables
	bool
	operator==(const COptimizationContext &oc) const
	{
		return oc.Matches(this);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &os) const;
	IOstream &OsPrintWithPrefix(IOstream &os, const CHAR *szPrefix) const;

	// check equality of optimization contexts
	static bool
	Equals(const COptimizationContext &ocLeft,
		   const COptimizationContext &ocRight)
	{
		return ocLeft == ocRight;
	}

	// hash function for optimization context
	static uint32_t
	HashValue(const COptimizationContext &oc)
	{
		GPOS_ASSERT(NULL != oc.Prpp());

		return oc.Prpp()->HashValue();
	}

	// equality function for cost contexts hash table
	static bool
	Equals(const OPTCTXT_PTR &pocLeft, const OPTCTXT_PTR &pocRight)
	{
		if (pocLeft == m_pocInvalid || pocRight == m_pocInvalid)
		{
			return pocLeft == m_pocInvalid && pocRight == m_pocInvalid;
		}

		return *pocLeft == *pocRight;
	}

	// hash function for cost contexts hash table
	static uint32_t
	HashValue(const OPTCTXT_PTR &poc)
	{
		GPOS_ASSERT(m_pocInvalid != poc);

		return HashValue(*poc);
	}

	// hash function used for computing stats during costing
	static uint32_t
	UlHashForStats(const COptimizationContext *poc)
	{
		GPOS_ASSERT(m_pocInvalid != poc);

		return HashValue(*poc);
	}

	// equality function used for computing stats during costing
	static bool FEqualForStats(const COptimizationContext *pocLeft,
							   const COptimizationContext *pocRight);

	// return true if given group expression should be optimized under given context
	static bool FOptimize(CMemoryPool *mp, CGroupExpression *pgexprParent,
						  CGroupExpression *pgexprChild,
						  COptimizationContext *pocChild, uint32_t ulSearchStages);

	// compare array of contexts based on context ids
	static bool FEqualContextIds(COptimizationContextArray *pdrgpocFst,
								 COptimizationContextArray *pdrgpocSnd);

	// compute required properties to CTE producer based on plan properties of CTE consumer
	static CReqdPropPlan *PrppCTEProducer(CMemoryPool *mp,
										  COptimizationContext *poc,
										  uint32_t ulSearchStages);

	// link for optimization context hash table in CGroup
	SLink m_link;

	// invalid optimization context, needed for hash table iteration
	static const COptimizationContext m_ocInvalid;

	// invalid optimization context pointer, needed for cost contexts hash table iteration
	static const OPTCTXT_PTR m_pocInvalid;

};	// class COptimizationContext

}

#endif