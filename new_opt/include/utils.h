#ifndef __UTILS_H__
#define __UTILS_H__

#include <cstdint>

#define ALIGNED_16(x) (((uintptr_t)x >> 1) << 1 == (uintptr_t) x)
#define ALIGNED_32(x) (((uintptr_t)x >> 2) << 2 == (uintptr_t) x)
#define ALIGNED_64(x) (((uintptr_t)x >> 3) << 3 == (uintptr_t) x)

#endif
