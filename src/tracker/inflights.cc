// Copyright (c) renjj - All Rights Reserved

// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "inflights.h"

#include <jrpc/base/logging/logging.h>

namespace jraft {

Inflights::Inflights(size_t max_size, uint64_t max_bytes)
    : start_(0),
      count_(0),
      bytes_(0),
      max_size_(max_size),
      max_bytes_(max_bytes) {}

// Returns true if no more message can be sent at the moment.
bool Inflights::Full() const {
  return count_ == max_size_ || (max_bytes_ != 0 && bytes_ >= max_bytes_);
}

// Notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is
// room for one more message, and consecutive calls to add Add() must provide
// a monotonic sequence of indexes.
void Inflights::Add(uint64_t index, uint64_t bytes) {
  if (Full()) {
    JLOG_FATAL << "cannot add into a Full Inflights";
  }

  size_t next = start_ + count_;
  if (next >= max_size_) {
    next -= max_size_;
  }
  if (next >= buffer_.size()) {
    Grow();
  }
  buffer_[next] = Inflight{index, bytes};
  count_++;
  bytes_ += bytes;
}

// Frees the inflights smaller or equal to the given `to` flight.
void Inflights::FreeLE(uint64_t to) {
  if (count_ == 0 || to < buffer_[start_].index) {
    // out of the left side of the window
    return;
  }

  size_t idx = start_;
  uint64_t bytes = 0;
  size_t i = 0;
  for (; i < count_; i++) {
    if (to < buffer_[idx].index) {
      // found the first large inflight
      break;
    }
    bytes += buffer_[idx].bytes;

    // increase index and maybe rotate
    idx++;
    if (idx >= max_size_) {
      idx -= max_size_;
    }
  }

  // free i Inflight and set new start index
  count_ -= i;
  bytes_ -= bytes;
  start_ = idx;
  if (count_ == 0) {
    // Inflights is empty, reset the start index so that we don't grow the
    // buffer unnecessarily.
    start_ = 0;
  }
}

// Grow the inflight buffer by doubling up to Inflights.size. We grow on demand
// instead of pre allocating to Inflights.max_size_ to handle systems which have
// thousands of Raft groups per process.
void Inflights::Grow() {
  size_t new_size = buffer_.size() * 2;
  if (new_size == 0) {
    new_size = 1;
  } else if (new_size > max_size_) {
    new_size = max_size_;
  }
  buffer_.resize(new_size);
}

// Reset frees all Inflight.
void Inflights::Reset() {
  count_ = 0;
  start_ = 0;
}

}  // namespace jraft
