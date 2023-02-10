// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <deque>
#include <vector>

#include "base/thread/condition.h"
#include "base/thread/mutex.h"
#include "base/thread/thread.h"
#include "base/types.h"

namespace jbase {

class ThreadPool : noncopyable {
 public:
  typedef std::function<void()> Task;

  explicit ThreadPool(const std::string &nameArg = std::string("ThreadPool"));
  ~ThreadPool();

  // Must be called before start().
  void setMaxQueueSize(int maxSize) { maxQueueSize_ = maxSize; }
  void setThreadInitCallback(const Task &cb) { threadInitCallback_ = cb; }

  void start(int numThreads);
  void stop();

  const std::string &name() const { return name_; }

  size_t queueSize() const;

  // Could block if maxQueueSize > 0
  void run(Task f);

 private:
  bool isFull() const;
  void runInThread();
  Task take();

  mutable MutexLock mutex_;
  Condition notEmpty_;
  Condition notFull_;
  std::string name_;
  Task threadInitCallback_;
  std::vector<std::unique_ptr<jbase::Thread> > threads_;
  std::deque<Task> queue_;
  size_t maxQueueSize_;
  bool running_;
};

}  // namespace jbase
