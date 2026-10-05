#ifndef __STATE_MACHINE_H__
#define __STATE_MACHINE_H__

#include "job.h"
#include "scheduler.h"
#include <cassert>
#include <cstdint>

namespace opt {

template <class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent,
          TEnumEvent tenumeventSentinel>
class CStateMachine {
private:
  // current state
  TEnumState m_tenumstate;

  // flag indicating if the state machine is initialized
  bool m_fInit;

  // array of transitions
  TEnumEvent m_rgrgtenumeventTransitions[tenumstateSentinel]
                                        [tenumstateSentinel];

  // actual implementation of transition
  bool FAttemptTransition(TEnumState tenumstateOld, TEnumEvent tenumevent,
                          TEnumState &tenumstateNew) const {
    assert(tenumevent < tenumeventSentinel);
    assert(m_fInit);

    for (uint32_t ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++) {
      if (m_rgrgtenumeventTransitions[tenumstateOld][ulOuter] == tenumevent) {
        tenumstateNew = (TEnumState)ulOuter;
        return true;
      }
    }

    return false;
  }

  // hidden copy ctor
  CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>(
      const CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent,
                          tenumeventSentinel> &) = delete;

public:
  // ctor
  CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent,
                tenumeventSentinel>()
      : m_tenumstate(TesInitial()), m_fInit(false) {
    assert(0 < tenumstateSentinel && 0 < tenumeventSentinel &&
           (uint32_t)tenumeventSentinel + 1 >= (uint32_t)tenumstateSentinel);
  }

  // initialize state machine
  void Init(const TEnumEvent rgrgtenumeventTransitions[tenumstateSentinel]
                                                      [tenumstateSentinel]) {
    assert(!m_fInit);

    for (uint32_t ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++) {
      for (uint32_t ulInner = 0; ulInner < tenumstateSentinel; ulInner++) {
        m_rgrgtenumeventTransitions[ulOuter][ulInner] =
            rgrgtenumeventTransitions[ulOuter][ulInner];
      }
    }
    m_tenumstate = TesInitial();
    m_fInit = true;
  }

  // dtor
  ~CStateMachine() {};

  // attempt transition
  bool FTransition(TEnumEvent tenumevent, TEnumState &tenumstate) {
    TEnumState tenumstateNew;
    bool fSucceeded =
        FAttemptTransition(m_tenumstate, tenumevent, tenumstateNew);

    if (fSucceeded) {
      m_tenumstate = tenumstateNew;
    }

    tenumstate = m_tenumstate;
    return fSucceeded;
  }

  // shorthand if current state and return value are not needed
  void Transition(TEnumEvent tenumevent) {
    TEnumState tenumstateDummy;
    bool fCheck = FTransition(tenumevent, tenumstateDummy);

    assert(fCheck && "Event rejected");
  }

  // inspect current state; to be used only in assertions
  TEnumState Estate() const { return m_tenumstate; }

  // get initial state
  TEnumState TesInitial() const { return (TEnumState)0; }

  // get final state
  TEnumState TesFinal() const { return (TEnumState)(tenumstateSentinel - 1); }

  // reset state
  void Reset() {
    m_tenumstate = TesInitial();
    m_fInit = false;
  }
}; // class CStateMachine

class Job;
class SchedulerCtx;

template <class TEnumState, TEnumState estSentinel, class TEnumEvent,
          TEnumEvent eevSentinel>
class CJobStateMachine {
private:
  // pointer to job action function
  typedef TEnumEvent (*PFuncAction)(SchedulerCtx *psc, Job *pjOwner);

  // shorthand for state machine
  typedef CStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel> SM;

  // array of actions corresponding to states
  PFuncAction m_rgPfuncAction[estSentinel];

  // job state machine
  SM m_sm;

  // hidden copy ctor
  CJobStateMachine(const CJobStateMachine &);

public:
  // ctor
  CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>(){};

  // dtor
  ~CJobStateMachine() {};

  // initialize state machine
  void Init(const TEnumEvent rgfTransitions[estSentinel][estSentinel]) {
    Reset();

    m_sm.Init(rgfTransitions);
  }

  // match action with state
  void SetAction(TEnumState est, PFuncAction pfAction) {
    assert(NULL != pfAction);
    assert(NULL == m_rgPfuncAction[est] && "Action has been already set");

    m_rgPfuncAction[est] = pfAction;
  }

  // run the state machine
  bool FRun(SchedulerCtx *psc, Job *pjOwner) {
    assert(NULL != psc);
    assert(NULL != pjOwner);

    TEnumState estCurrent = estSentinel;
    TEnumState estNext = estSentinel;
    do {
      // check if current search stage is timed-out
      // TODO:
      //   if (psc->Peng()->PssCurrent()->FTimedOut()) {
      //     // cleanup job state and terminate state machine
      //     pjOwner->Cleanup();
      //     return true;
      //   }

      // find current state
      estCurrent = m_sm.Estate();

      // get the function associated with current state
      PFuncAction pfunc = m_rgPfuncAction[estCurrent];
      assert(NULL != pfunc);

      // execute the function to get an event
      TEnumEvent eev = pfunc(psc, pjOwner);

      // use the event to transition state machine
      estNext = estCurrent;
      bool fSucceeded = m_sm.FTransition(eev, estNext);

      assert(fSucceeded);
    } while (estNext != estCurrent && estNext != m_sm.TesFinal());

    return (estNext == m_sm.TesFinal());
  }

  // reset state machine
  void Reset() {
    m_sm.Reset();

    // initialize actions array
    for (uint32_t i = 0; i < estSentinel; i++) {
      m_rgPfuncAction[i] = NULL;
    }
  }
}; // class CJobStateMachine

} // namespace opt

#endif