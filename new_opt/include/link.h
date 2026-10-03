#ifndef __LINK_H__
#define __LINK_H__

namespace opt {
struct SLink {
    SLink(const SLink&)=delete;
    public:
    void * _next;
    void * _prev;
    SLink():_next(nullptr),_prev(nullptr){}
};
}

#endif