// Copyright (c) renjj - All Rights Reserved
#pragma once

#include <atomic>
#include <vector>
#include <string>

#include "base/logging/log_stream.h"
#include "base/thread/countdown_latch.h"
#include "base/thread/mutex.h"
#include "base/thread/thread.h"
#include "base/blocking_queue.h"

namespace jbase {

class AsyncLogging : noncopyable {
 public:
  AsyncLogging(const std::string &basename, size_t rollSize,
               int flushInterval = 1, bool sync = false, bool dupStd = true);

  ~AsyncLogging() {
    if (running_) {
      stop();
    }
  }

  void append(const char *logline, int len);
  void flush_notify();

  void start() {
    running_ = true;
    thread_.start();
    latch_.wait();
  }

  void stop() {
    running_ = false;
    cond_.notify();
    thread_.join();
  }

 private:
  void threadFunc();

  typedef jbase::detail::FixedBuffer<jbase::detail::kLargeBuffer> Buffer;
  typedef std::vector<std::unique_ptr<Buffer> > BufferVector;
  typedef BufferVector::value_type BufferPtr;

  const int flushInterval_;
  bool sync_;
  bool dupStd_;
  std::atomic<bool> running_;
  std::string basename_;
  size_t rollSize_;
  jbase::Thread thread_;
  jbase::CountDownLatch latch_;
  jbase::MutexLock mutex_;
  jbase::Condition cond_;
  BufferPtr currentBuffer_;
  BufferPtr nextBuffer_;
  BufferVector buffers_;
};

}  // namespace jbase
