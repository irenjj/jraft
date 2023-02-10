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
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace jraft {

// Inflight describes an in-flight kMsgApp message.
struct Inflight {
  // the index of the last entry inside the message
  uint64_t index;
  // the total byte size of the entries in the message
  uint64_t bytes;
};

// Inflights limits the number of kMsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them.
// Callers use Full() to check whether more message can be sent, call Add()
// whenever they are sending a new append, and release "quota" via FreeLE()
// whenever an ack is received.
class Inflights {
 public:
  Inflights(size_t max_size, uint64_t max_bytes);
  Inflights(const Inflights& inflights) = default;
  ~Inflights() = default;

  Inflights& operator=(const Inflights& inflights) = default;

  bool Full() const;

  void Add(uint64_t index, uint64_t bytes);
  void FreeLE(uint64_t to);
  void Reset();

  size_t start() const { return start_; }
  void set_start(size_t start) { start_ = start; }

  size_t count() const { return count_; }
  void set_count(size_t count) { count_ = count; }

  uint64_t bytes() const { return bytes_; }
  void set_bytes(uint64_t bytes) { bytes_ = bytes; }

  size_t max_size() const { return max_size_; }
  void set_max_size(size_t max_size) { max_size_ = max_size; }

  uint64_t max_bytes() const { return max_bytes_; }
  void set_max_bytes(uint64_t max_bytes) { max_bytes_ = max_bytes; }

  const std::vector<Inflight>& buffer() const { return buffer_; }
  void set_buffer(const std::vector<Inflight>& buffer) { buffer_ = buffer; }

 private:
  void Grow();

  // the starting index in the buffer_
  size_t start_;

  // number of Inflight messages in the buffer_
  size_t count_;
  // number of all Inflight messages' bytes
  uint64_t bytes_;

  // the max number of Inflight messages
  size_t max_size_;
  // the max total byte size of Inflight messages
  uint64_t max_bytes_;

  // buffer_ is a ring buffer containing info about all Inflight messages.
  std::vector<Inflight> buffer_;
};

using InflightsPtr = std::unique_ptr<Inflights>;

}  // namespace jraft
