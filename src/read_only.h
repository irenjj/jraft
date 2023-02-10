// Copyright (c) renjj - All Rights Reserved

// Copyright 2016 The etcd Authors
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

#include "common/noncopyable.h"
#include "eraft.pb.h"

namespace jraft {

struct ReadIndexStatus {
  Message req;
  uint64_t index;
  std::map<uint64_t, bool> acks;
};

using ReadIndexStatusPtr = std::shared_ptr<ReadIndexStatus>;

enum ReadOnlyOption {
  // kReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  kReadOnlySafe = 0,
  // kReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than
  // it should (clock can move backward/pause without any bound). ReadIndex
  // is not safe in that case.
  kReadOnlyLeaseBased = 1,
};

class ReadOnly : public Noncopyable {
 public:
  explicit ReadOnly(ReadOnlyOption option);
  ~ReadOnly() = default;

  void AddRequest(uint64_t index, const Message& m);
  size_t RecvAck(uint64_t id, const std::string& context);
  void Advance(const Message& m, std::vector<ReadIndexStatusPtr>* rss);
  std::string LastPendingRequestCtx() const;
  void Reset(ReadOnlyOption option);

  const ReadOnlyOption& option() const { return option_; }
  ReadOnlyOption& mutable_option() { return option_; }
  void set_option(const ReadOnlyOption& option) { option_ = option; }

  const std::map<std::string, ReadIndexStatusPtr>& pending_read_index() const {
    return pending_read_index_;
  }
  std::map<std::string, ReadIndexStatusPtr>& mutable_pending_read_index() {
    return pending_read_index_;
  }
  void set_pending_read_index(
      const std::map<std::string, ReadIndexStatusPtr>& p) {
    pending_read_index_ = p;
  }

  const std::vector<std::string>& read_index_queue() const {
    return read_index_queue_;
  }
  std::vector<std::string>& mutable_read_index_queue() {
    return read_index_queue_;
  }
  void set_read_index_queue(const std::vector<std::string>& r) {
    read_index_queue_ = r;
  }

 private:
  ReadOnlyOption option_;
  std::map<std::string, ReadIndexStatusPtr> pending_read_index_;
  std::vector<std::string> read_index_queue_;
};

}  // namespace jraft
