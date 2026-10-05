#ifndef __JOB_H__
#define __JOB_H__

#include <cassert>
#include <cstdint>
#include "link.h"
#include "list.h"
namespace opt {

class JobFactory;
class JobQueue;
class Scheduler;
class SchedulerCtx;

class Job {
	// friends
	friend class JobFactory;
	friend class JobQueue;
	friend class Scheduler;

public:
	// job type
	enum EJobType
	{
		EjtTest = 0,
		EjtGroupOptimization,
		EjtGroupImplementation,
		EjtGroupExploration,
		EjtGroupExpressionOptimization,
		EjtGroupExpressionImplementation,
		EjtGroupExpressionExploration,
		EjtTransformation,

		EjtInvalid,
		EjtSentinel = EjtInvalid
	};

private:

	// parent job
	Job *m_pjParent;

	// assigned job queue
	JobQueue *m_pjq;

	// reference counter
	uintptr_t m_ulpRefs;

	// job id - set by job factory
	uint32_t m_id;

	// job type
	EJobType m_ejt;

	// flag indicating if job is initialized
	bool m_fInit;

	// set type
	void
	SetJobType(EJobType ejt)
	{
		m_ejt = ejt;
	}

	// parent accessor
	Job *
	PjParent() const
	{
		return m_pjParent;
	}

	// set parent
	void
	SetParent(Job *pj)
	{
		assert(this != pj);

		m_pjParent = pj;
	}

	// increment reference counter
	void
	IncRefs()
	{
		m_ulpRefs++;
	}

	// decrement reference counter
	uintptr_t
	UlpDecrRefs()
	{
		assert(0 < m_ulpRefs && "Decrement counter from 0");
		return m_ulpRefs--;
	}

	// notify parent of job completion;
	// return true if parent is runnable;
	bool FResumeParent() const;

    // reference counter accessor
	uintptr_t
	UlpRefs() const
	{
		return m_ulpRefs;
	}

	// private copy ctor
	Job(const Job &)=delete;

protected:
	// id accessor
	uint32_t
	Id() const
	{
		return m_id;
	}

	// ctor
	Job()
		: m_pjParent(nullptr),
		  m_pjq(nullptr),
		  m_ulpRefs(0),
		  m_id(0),
		  m_ejt(EjtInvalid),
		  m_fInit(false)
	{
	}

	// dtor
	virtual ~Job()
	{
        assert(0 == m_ulpRefs);
	}

	// reset job
	virtual void Reset();

	// check if job is initialized
	bool
	FInit() const
	{
		return m_fInit;
	}

	// mark job as initialized
	void
	SetInit()
	{
		assert(false == m_fInit);

		m_fInit = true;
	}

public:
	// actual job execution given a scheduling context
	// returns true if job completes, false if it is suspended
	virtual bool FExecute(SchedulerCtx *psc) = 0;

	// type accessor
	EJobType
	Ejt() const
	{
		return m_ejt;
	}

	// job queue accessor
	JobQueue *
	Pjq() const
	{
		return m_pjq;
	}

	// set job queue
	void
	SetJobQueue(JobQueue *pjq)
	{
		assert(nullptr != pjq);
		m_pjq = pjq;
	}

	// cleanup internal state
	virtual void
	Cleanup()
	{
	}

	// link for job queueing
	SLink m_linkQueue;

};

class JobFactory
{
private:
	// number of jobs in each pool
	const uint32_t m_ulJobs;

	// retrieve job of specific type
	template <class T>
	T *
	PtRetrieve()
	{
		return new T();
	}

	// release job
	template <class T>
	void
	Release(T *pt)
	{
		delete pt;
	}

	// truncate job pool
	template <class T>
	void
	TruncatePool()
	{
	}

	// no copy ctor
	JobFactory(const JobFactory &)=delete;

public:
	// ctor
	JobFactory(uint32_t ulJobs);

	// dtor
	~JobFactory();

	// create job of specific type
	Job *PjCreate(Job::EJobType ejt);

	// release completed job
	void Release(Job *pj);

	// truncate the container for the specific job type
	void Truncate(Job::EJobType ejt);

};	// class CJobFactory

class JobQueue
{
private:
	// main job
	Job *m_pj;

	// flag indicating if main job has completed
	bool m_fCompleted;

	// list of jobs waiting for main job to complete
	List<Job> m_listjQueued;

public:
	// enum indicating job queueing result
	enum EJobQueueResult
	{
		EjqrMain = 0,
		EjqrQueued,
		EjqrCompleted
	};

	// ctor
	JobQueue() : m_pj(NULL), m_fCompleted(false)
	{
		m_listjQueued.Init(OPT_OFFSET(Job, m_linkQueue));
	}

	// dtor
	~JobQueue()
	{
        assert(m_listjQueued.IsEmpty());
	}

	// reset job queue
	void
	Reset()
	{
		assert(m_listjQueued.IsEmpty());

		m_pj = NULL;
		m_fCompleted = false;
	}

	// add job as a waiter;
	EJobQueueResult EjqrAdd(Job *pj);

	// notify waiting jobs of job completion
	void NotifyCompleted(SchedulerCtx *psc);
};	// class CJobQueue

class JobTest : public Job
{
	// friends
	friend class JobFactory;

public:
	// job test type
	enum ETestType
	{
		EttSpawn,
		EttStartQueue,
		EttQueueu
	};

private:
	// test type
	ETestType m_ett;

	// number of job spawning rounds
	uint32_t m_ulRounds;

	// spawning fanout
	uint32_t m_ulFanout;

	// CPU-burning iterations per job
	uint32_t m_ulIters;

	// iteration counter
	static uintptr_t m_ulpCnt;

	// job queue
	JobQueue *m_pjq;

	// test job spawning
	bool FSpawn(SchedulerCtx *psc);

	// start jobs to be queued
	bool FStartQueue(SchedulerCtx *psc);

	// test job queueing
	bool FQueue(SchedulerCtx *psc);

	// burn some CPU to simulate actual work
	void Loop();

public:
	// ctor
	JobTest();

	// dtor
	virtual ~JobTest();

	// execution
	virtual bool FExecute(SchedulerCtx *psc);

	// set execution parameters
	void
	Init(ETestType ett, uint32_t ulRounds, uint32_t ulFanout, uint32_t ulIters,
		 JobQueue *pjq)
	{
		m_ett = ett;
		m_ulRounds = ulRounds;
		m_ulFanout = ulFanout;
		m_ulIters = ulIters;
		m_pjq = pjq;
	}

	// copy execution parameters
	void
	Init(JobTest *pjt)
	{
		Init(pjt->m_ett, pjt->m_ulRounds, pjt->m_ulFanout, pjt->m_ulIters,
			 pjt->m_pjq);
	}

	// reset
	static void
	ResetCnt()
	{
		m_ulpCnt = 0;
	}

	static uintptr_t
	UlpCnt()
	{
		return m_ulpCnt;
	}

	// conversion function
	static JobTest *
	PjConvert(Job *pj)
	{
		assert(NULL != pj);
		assert(EjtTest == pj->Ejt());

		return dynamic_cast<JobTest *>(pj);
	}
};
}


#endif
