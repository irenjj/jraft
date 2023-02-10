// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <cstdint>

namespace jbase {
namespace CurrentThread {
// internal
// 线程编号，从0开始，全局唯一
extern __thread int t_cachedTno;
// 线程id
extern __thread int t_cachedTid;
extern __thread char t_tidString[32];
extern __thread int t_tidStringLength;
extern __thread const char *t_threadName;
void cacheTid();
void cacheTno();

inline int tid() {
  if (__builtin_expect(t_cachedTid == 0, 0)) {
    cacheTid();
  }
  return t_cachedTid;
}

inline int tno() {
  if (__builtin_expect(t_cachedTno == -1, 0)) {
    cacheTno();
  }
  return t_cachedTno;
}

inline const char *tidString()  // for logging
{
  return t_tidString;
}

inline int tidStringLength()  // for logging
{
  return t_tidStringLength;
}

inline const char *name() { return t_threadName; }

bool isMainThread();

void sleepUsec(int64_t usec);

}  // namespace CurrentThread

}  // namespace jbase
