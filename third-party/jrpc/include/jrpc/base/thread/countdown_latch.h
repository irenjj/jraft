// Copyright (c) renjj - All Rights Reserved
#pragma once

#include "base/thread/condition.h"
#include "base/thread/mutex.h"

namespace jbase {

class CountDownLatch : noncopyable {
 public:
  explicit CountDownLatch(int count);

  void wait();

  void countDown();

  int getCount() const;

 private:
  mutable MutexLock mutex_;
  Condition condition_;
  int count_;
};

}  // namespace jbase
