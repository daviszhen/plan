#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

#include <cstddef>
#include <cstdint>
#include <link.h>
namespace opt {

class SchedulerCtx;
class Job;

class Scheduler{
    friend class Job;

private:

    struct SJobLink {
        uint32_t _id;
        Job * _job;
        SLink _link;
        void Init(Job * job){
            _job = job;
            _link._prev = _link._next = nullptr;
        }
    };

public:
    enum class JobResultP{
        JobResult_Runnable = 0,
        JobResult_Suspended,
        JobResult_Completed,
        JobResult_Sentinel
    };
};

}

#endif