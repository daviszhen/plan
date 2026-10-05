#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

#include "link.h"
#include "list.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
namespace opt {

class SchedulerCtx;
class Job;

class Scheduler {
  friend class Job;

private:
  struct SJobLink {
    uint32_t m_id;
    Job *m_pj;
    SLink m_link;
    void Init(Job *pj) {
      m_pj = pj;
      m_link._prev = m_link._next = nullptr;
    }
  };

public:
  enum JobResult {
    EjrRunnable = 0,
    EjrSuspended,
    EjrCompleted,

    EjrSentinel
  };

private:
  // job waiting list
  SyncList<SJobLink> m_listjlWaiting;

  // current job counters
  uintptr_t m_ulpTotal;
  uintptr_t m_ulpRunning;
  uintptr_t m_ulpQueued;

  // stats
  uintptr_t m_ulpStatsQueued;
  uintptr_t m_ulpStatsDequeued;
  uintptr_t m_ulpStatsSuspended;
  uintptr_t m_ulpStatsCompleted;
  uintptr_t m_ulpStatsCompletedQueued;
  uintptr_t m_ulpStatsResumed;

  // keep executing jobs (if any)
  void ExecuteJobs(SchedulerCtx *psc);

  // process job execution results
  void ProcessJobResult(Job *pj, SchedulerCtx *psc, bool fCompleted);

  // retrieve next job to run
  Job *PjRetrieve();

  // schedule job for execution
  void Schedule(Job *pj);

  // prepare for job execution
  void PreExecute(Job *pj);

  // execute job
  bool FExecute(Job *pj, SchedulerCtx *psc);

  // process job execution outcome
  JobResult EjrPostExecute(Job *pj, bool fCompleted);

  // resume parent job
  void ResumeParent(Job *pj);

  // check if all jobs have completed
  bool IsEmpty() const { return (0 == m_ulpTotal); }

  // no copy ctor
  Scheduler(const Scheduler &) = delete;

public:
  Scheduler();

  virtual ~Scheduler();

  // main job processing task
  static void *Run(void *);

  // transition job to completed
  void Complete(Job *pj);

  // transition queued job to completed
  void CompleteQueued(Job *pj);

  // transition job to suspended
  void Suspend(Job *pj);

  // add new job for scheduling
  void Add(Job *pj, Job *pjParent);

  // resume suspended job
  void Resume(Job *pj);

  // print statistics
  void PrintStats() const;
};

class JobFactory;
class Engine;

class SchedulerCtx {
private:
  // job factory
  JobFactory *m_pjf;

  // scheduler
  Scheduler *m_psched;

  // optimization engine
  Engine *m_peng;

  // flag indicating if context has been initialized
  bool m_fInit;

  bool FInit() const { return m_fInit; }

  // no copy ctor
  SchedulerCtx(const SchedulerCtx &) = delete;

public:
  SchedulerCtx();

  ~SchedulerCtx();

  // initialization
  void Init(JobFactory *pjf, Scheduler *psched, Engine *peng);

  JobFactory *Pjf() const {
    assert(FInit() && "Scheduling context is not initialized");
    return m_pjf;
  }

  // scheduler accessor
  Scheduler *Psched() const {
    assert(FInit() && "Scheduling context is not initialized");
    return m_psched;
  }

  // engine accessor
  Engine *Peng() const {
    assert(FInit() && "Scheduling context is not initialized");
    return m_peng;
  }
};

} // namespace opt

#endif