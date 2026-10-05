#ifndef __ENGINE_H__
#define __ENGINE_H__

namespace opt {
// SchedulerCtx only stores an Engine pointer.  The scheduler tests do not
// need an optimizer implementation, so keep the fixture deliberately small.
class Engine {};
} // namespace opt


#endif