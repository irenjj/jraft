// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <cassert>
#include <deque>

#include "base/thread/condition.h"
#include "base/thread/mutex.h"

namespace jbase {

template <typename T>
class BlockingQueue : noncopyable {
 public:
  BlockingQueue() : mutex_(), notEmpty_(mutex_), queue_() {}

  void put(const T &x) {
    MutexLockGuard lock(mutex_);
    queue_.push_back(x);
    notEmpty_.notify();  // wait morphing saves us
                         // http://www.domaigne.com/blog/computing/condvars-signal-with-mutex-locked-or-not/
  }

  void put(T &&x) {
    MutexLockGuard lock(mutex_);
    queue_.push_back(std::move(x));
    notEmpty_.notify();
  }

  T take() {
    MutexLockGuard lock(mutex_);
    // always use a while-loop, due to spurious wakeup
    while (queue_.empty()) {
      notEmpty_.wait();
    }
    assert(!queue_.empty());
    T front(std::move(queue_.front()));
    queue_.pop_front();
    return std::move(front);
  }

  size_t size() const {
    MutexLockGuard lock(mutex_);
    return queue_.size();
  }

 private:
  mutable MutexLock mutex_;
  Condition notEmpty_;
  std::deque<T> queue_;
};

}  // namespace jbase
