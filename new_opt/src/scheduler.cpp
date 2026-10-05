#include <cassert>
#include <cstdint>
#include <new>
#include "scheduler.h"
#include "job.h"
#include "engine.h"

namespace opt {

Scheduler::Scheduler():
	  m_ulpTotal(0),
	  m_ulpRunning(0),
	  m_ulpQueued(0),
	  m_ulpStatsQueued(0),
	  m_ulpStatsDequeued(0),
	  m_ulpStatsSuspended(0),
	  m_ulpStatsCompleted(0),
	  m_ulpStatsCompletedQueued(0),
	  m_ulpStatsResumed(0)
{
	// initialize list of waiting new jobs
	m_listjlWaiting.Init(OPT_OFFSET(SJobLink, m_link));
}

Scheduler::~Scheduler(){
    assert(0 == m_ulpTotal);
}

void *Scheduler::Run(void *pv)
{
	SchedulerCtx *psc = reinterpret_cast<SchedulerCtx *>(pv);
	psc->Psched()->ExecuteJobs(psc);

	return NULL;
}

void
Scheduler::ExecuteJobs(SchedulerCtx *psc)
{
	Job *pj = NULL;
	uint32_t count = 0;

	// keep retrieving jobs
	while (NULL != (pj = PjRetrieve()))
	{
		// prepare for job execution
		PreExecute(pj);

		// execute job
		bool fCompleted = FExecute(pj, psc);

		// process job result
		switch (EjrPostExecute(pj, fCompleted))
		{
			case EjrCompleted:
				// job is completed
				Complete(pj);

				psc->Pjf()->Release(pj);
				break;

			case EjrRunnable:
				// child jobs have completed, job can immediately resume
				Resume(pj);
				continue;

			case EjrSuspended:
				// job is suspended until child jobs complete
				Suspend(pj);
				break;

			default:
				assert(!"Invalid job execution result");
		}

		if (++count == OPT_SCHED_CFA)
		{
			// GPOS_CHECK_ABORT;
			count = 0;
		}
	}
}

void
Scheduler::Add(Job *pj, Job *pjParent)
{
	assert(NULL != pj);
	assert(0 == pj->UlpRefs());

	// increment ref counter for parent job
	if (NULL != pjParent)
	{
		pjParent->IncRefs();
	}

	// set current job as parent of its child
	pj->SetParent(pjParent);

	// increment total number of jobs
	m_ulpTotal++;

	Schedule(pj);
}

void
Scheduler::Schedule(Job *pj)
{
	assert(NULL != pj);

	// get job link
	SJobLink *pjl = new (std::nothrow) SJobLink();

	// throw OOM if no link can be retrieved
	if (NULL == pjl)
	{
		assert(1 && "oom");
	}
	pjl->Init(pj);

	// add to waiting list
	m_listjlWaiting.Push(pjl);

	// increment number of queued jobs
	m_ulpQueued++;

	// update statistics
	m_ulpStatsQueued++;
}

Job *
Scheduler::PjRetrieve()
{
	// retrieve runnable job from lists of waiting jobs
	SJobLink *pjl = m_listjlWaiting.Pop();
	Job *pj = NULL;

	if (NULL != pjl)
	{
		pj = pjl->m_pj;

		assert(NULL != pj);
		assert(0 == pj->UlpRefs());

		// decrement number of queued jobs
		m_ulpQueued--;

		// update statistics
		m_ulpStatsDequeued++;

		// recycle job link
		delete pjl;
	}

	return pj;
}

void
Scheduler::PreExecute(Job *pj)
{
	assert(NULL != pj);
	assert(0 == pj->UlpRefs() &&
				"Runnable job cannot have pending children");

	// increment number of running jobs
	m_ulpRunning++;

	// increment job ref counter
	pj->IncRefs();
}

bool
Scheduler::FExecute(Job *pj, SchedulerCtx *psc)
{
	bool fCompleted = true;
	JobQueue *pjq = pj->Pjq();

	// check if job is associated to a job queue
	if (NULL == pjq)
	{
		fCompleted = pj->FExecute(psc);
	}
	else
	{
		switch (pjq->EjqrAdd(pj))
		{
			case JobQueue::EjqrMain:
				// main job, runs job operation
				fCompleted = pj->FExecute(psc);
				if (fCompleted)
				{
					// notify queued jobs
					pjq->NotifyCompleted(psc);
				}
				else
				{
					// task is suspended
					(void) pj->UlpDecrRefs();
				}
				break;

			case JobQueue::EjqrQueued:
				// queued job
				fCompleted = false;
				break;

			case JobQueue::EjqrCompleted:
				break;
		}
	}

	return fCompleted;
}

Scheduler::JobResult
Scheduler::EjrPostExecute(Job *pj, bool fCompleted)
{
	assert(NULL != pj);
	assert(0 < pj->UlpRefs() && "IsRunning job is marked as completed");

	// decrement job ref counter
	uintptr_t ulRefs = pj->UlpDecrRefs();

	// decrement number of running jobs
	m_ulpRunning--;

	// check if job completed
	if (fCompleted)
	{
		assert(1 == ulRefs);

		return EjrCompleted;
	}

	// check if all children have completed
	if (1 == ulRefs)
	{
		return EjrRunnable;
	}

	return EjrSuspended;
}

void
Scheduler::Complete(Job *pj)
{
	assert(0 == pj->UlpRefs());
	ResumeParent(pj);

	// update statistics
	m_ulpTotal--;
	m_ulpStatsCompleted++;
}

void
Scheduler::Resume(Job *pj)
{
	assert(NULL != pj);
	assert(0 == pj->UlpRefs());

	Schedule(pj);
}

void
Scheduler::Suspend(Job *pj
)
{
	assert(NULL != pj);

	m_ulpStatsSuspended++;
}

void
Scheduler::CompleteQueued(Job *pj)
{
	assert(0 == pj->UlpRefs());
	ResumeParent(pj);

	// update statistics
	m_ulpTotal--;
	m_ulpStatsCompleted++;
	m_ulpStatsCompletedQueued++;
}

void
Scheduler::ResumeParent(Job *pj)
{
	assert(0 == pj->UlpRefs());

	Job *pjParent = pj->PjParent();

	if (NULL != pjParent)
	{
		// notify parent job
		if (pj->FResumeParent())
		{
			// reschedule parent
			Resume(pjParent);

			// update statistics
			m_ulpStatsResumed++;
		}
	}
}

SchedulerCtx::SchedulerCtx(): m_psched(NULL), m_fInit(false)
{
}

SchedulerCtx::~SchedulerCtx()
{
	OPT_ASSERT_IMP(FInit(), NULL != Psched());
}

void
SchedulerCtx::Init(JobFactory *pjf, Scheduler *psched, Engine *peng)
{
	assert(NULL != pjf);
	assert(NULL != psched);
	assert(NULL != peng);

	assert(!FInit() && "Scheduling context is already initialized");
	m_pjf = pjf;
	m_psched = psched;
	m_peng = peng;
	m_fInit = true;
}
}