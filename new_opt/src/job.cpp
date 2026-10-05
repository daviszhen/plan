#include "job.h"
#include <cassert>
#include <cstdint>
#include "scheduler.h"

namespace opt {

namespace {
constexpr uint32_t kJobTestDummyConst = 45;
}

void Job::Reset() {
  m_pjParent = NULL;
  m_pjq = NULL;
  m_ulpRefs = 0;
  m_fInit = false;
}

bool Job::FResumeParent() const {
  assert(0 == UlpRefs());
  assert(NULL != m_pjParent);
  assert(0 < m_pjParent->UlpRefs());

  // decrement parent's ref counter
  uintptr_t ulpRefs = m_pjParent->UlpDecrRefs();

  // check if job should be resumed
  return (1 == ulpRefs);
}

uintptr_t JobTest::m_ulpCnt = 0;

JobTest::JobTest()
	: Job(),
	  m_ett(EttSpawn),
	  m_ulRounds(0),
	  m_ulFanout(0),
	  m_ulIters(0),
	  m_pjq(nullptr)
{
}

JobTest::~JobTest() = default;

bool JobTest::FExecute(SchedulerCtx *psc)
{
	switch (m_ett)
	{
		case EttSpawn:
			return FSpawn(psc);
		case EttStartQueue:
			return FStartQueue(psc);
		case EttQueueu:
			return FQueue(psc);
	}

	assert(!"Invalid test job type");
	return true;
}

bool JobTest::FSpawn(SchedulerCtx *psc)
{
	const uintptr_t offset = m_ulpCnt++;
	if (m_ulRounds > offset)
	{
		for (uint32_t i = 0; i < m_ulFanout; ++i)
		{
			Job *pj = psc->Pjf()->PjCreate(Job::EjtTest);
			JobTest *pjt = PjConvert(pj);
			pjt->Init(this);
			psc->Psched()->Add(pj, this);
		}
		Loop();
		return false;
	}

	return true;
}

bool JobTest::FStartQueue(SchedulerCtx *psc)
{
	const uintptr_t offset = m_ulpCnt++;
	if (0 == offset)
	{
		assert(nullptr != m_pjq);
		for (uint32_t i = 0; i < m_ulFanout; ++i)
		{
			Job *pj = psc->Pjf()->PjCreate(Job::EjtTest);
			JobTest *pjt = PjConvert(pj);
			pjt->Init(EttQueueu, m_ulRounds, m_ulFanout, m_ulIters, m_pjq);
			psc->Psched()->Add(pj, this);
		}
		return false;
	}

	return true;
}

bool JobTest::FQueue(SchedulerCtx *psc)
{
	assert(nullptr != m_pjq);
	bool completed = true;
	switch (m_pjq->EjqrAdd(this))
	{
		case JobQueue::EjqrMain:
			Loop();
			m_pjq->NotifyCompleted(psc);
			break;
		case JobQueue::EjqrQueued:
			completed = false;
			break;
		case JobQueue::EjqrCompleted:
			break;
	}
	return completed;
}

void JobTest::Loop()
{
	uint32_t outer = 0;
	while (outer < m_ulIters)
	{
		for (uint32_t inner = outer; inner > 0; --inner)
		{
			if (0 < outer * (outer + kJobTestDummyConst))
			{
				++outer;
			}
		}
		++outer;
	}
}

JobFactory::JobFactory(uint32_t ulJobs)
	: m_ulJobs(ulJobs)
{
	(void)m_ulJobs;
	// initialize factories to be used first
    Release(PjCreate(Job::EjtTest));
	// Release(PjCreate(Job::EjtGroupExploration));
	// Release(PjCreate(Job::EjtGroupExpressionExploration));
	// Release(PjCreate(Job::EjtTransformation));
}

JobFactory::~JobFactory(){
    Truncate(Job::EjtTest);
}

Job *
JobFactory::PjCreate(Job::EJobType ejt)
{
	Job *pj = NULL;

	switch (ejt)
	{
		case Job::EjtTest:
			pj = PtRetrieve<JobTest>();
			break;

		case Job::EjtInvalid:
			assert(!"Invalid job type");
			return nullptr;

		default:
			assert(!"Unsupported job type");
			return nullptr;
	}


	// prepare task
	pj->Reset();
	pj->SetJobType(ejt);

	return pj;
}

void
JobFactory::Release(Job *pj)
{
	assert(NULL != pj);

	switch (pj->Ejt())
	{
		case Job::EjtTest:
			Release(JobTest::PjConvert(pj));
			break;

	
		default:
			assert(!"Invalid job type");
	}
}

void
JobFactory::Truncate(Job::EJobType ejt)
{
	// need to suspend cancellation while truncating job pool
		switch (ejt)
		{
			case Job::EjtTest:
				TruncatePool<JobTest>();
				break;


			case Job::EjtInvalid:
				assert(!"Invalid job type");
				break;

			default:
				assert(!"Unsupported job type");
				break;
		}

}

JobQueue::EJobQueueResult
JobQueue::EjqrAdd(Job *pj)
{
	assert(NULL != pj);

	EJobQueueResult ejer = EjqrCompleted;

	// check if job has completed before getting the lock
	if (!m_fCompleted)
	{
		// check if this is the main job
		if (pj == m_pj)
		{
			assert(!m_fCompleted);
			ejer = EjqrMain;
			pj->IncRefs();
		}
		else
		{
			// check if job is completed
			if (!m_fCompleted)
			{
				m_listjQueued.Append(pj);
				bool fOwner = (pj == m_listjQueued.First());

				// first caller becomes the owner
				if (fOwner)
				{
					assert(NULL == m_pj);

					m_pj = pj;
					ejer = EjqrMain;
				}
				else
				{
					ejer = EjqrQueued;
				}

				pj->IncRefs();
			}
		}
	}

	return ejer;
}

void
JobQueue::NotifyCompleted(SchedulerCtx *psc)
{
	assert(!m_fCompleted);
	m_fCompleted = true;

	assert(!m_listjQueued.IsEmpty());
	while (!m_listjQueued.IsEmpty())
	{
		Job *pj = m_listjQueued.RemoveHead();

		// check if job execution has completed
		if (1 == pj->UlpDecrRefs())
		{
			// update job as completed
			psc->Psched()->CompleteQueued(pj);

			// recycle job
			psc->Pjf()->Release(pj);
		}
	}
}

} // namespace opt
