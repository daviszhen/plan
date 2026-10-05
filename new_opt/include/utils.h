#ifndef __UTILS_H__
#define __UTILS_H__

#define ALIGNED_16(x) (((uintptr_t)x >> 1) << 1 == (uintptr_t) x)
#define ALIGNED_32(x) (((uintptr_t)x >> 2) << 2 == (uintptr_t) x)
#define ALIGNED_64(x) (((uintptr_t)x >> 3) << 3 == (uintptr_t) x)

#define OPT_OFFSET(T, M) ((uint32_t)(size_t) & (((T *) 0x1)->M) - 1)

// implication assert
#define OPT_ASSERT_IMP(x, y) assert(!(x) || (y))

#define OPT_SCHED_CFA 100

enum OPT_RESULT
{
	OPT_OK = 0,

	OPT_FAILED = 1,
	OPT_OOM = 2,
	OPT_NOT_FOUND = 3,
	OPT_TIMEOUT = 4
};

#define ALIGN_STORAGE __attribute__((aligned(8)))

#define OPT_WSZ_LIT(x) L##x
#endif
