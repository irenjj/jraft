// Copyright (c) renjj - All Rights Reserved

// Copyright 2015 The etcd Authors
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

#include <utility>

#include "storage.h"

namespace jraft {

class MemoryStorage : public Storage {
 public:
  MemoryStorage();

  ErrNum InitialState(HardState* hs, ConfState* cs) override;
  ErrNum Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                 std::vector<EntryPtr>* entries) override;
  ErrNum Term(uint64_t i, uint64_t* term) override;
  ErrNum FirstIndex(uint64_t* index) override;
  ErrNum LastIndex(uint64_t* index) override;

  ErrNum GetSnapshot(SnapshotPtr& snapshot) override;
  ErrNum CreateSnapshot(uint64_t i, ConfState* cs, const std::string& data,
                        SnapshotPtr* snap) override;
  ErrNum ApplySnapshot(const SnapshotPtr& snapshot) override;

  ErrNum Compact(uint64_t compact_index) override;
  ErrNum Append(const std::vector<EntryPtr>& entries) override;

  const HardState& hard_state() const { return hard_state_; }
  void set_hard_state(const HardState& hard_state) { hard_state_ = hard_state; }

  const Snapshot& snapshot() const { return *snapshot_; }
  SnapshotPtr mutable_snapshot() { return snapshot_; }
  void set_snapshot(const SnapshotPtr& snapshot) { snapshot_ = snapshot; }

  const std::vector<EntryPtr>& entries() const { return entries_; }
  const Entry& entries(size_t i) const { return *(entries_[i]); }
  std::vector<EntryPtr>& mutable_entries() { return entries_; }
  void set_entries(const std::vector<EntryPtr>& entries) { entries_ = entries; }

 private:
  uint64_t FirstIndex() const { return entries_[0]->index() + 1; }
  uint64_t LastIndex() const {
    return entries_[0]->index() + static_cast<uint64_t>(entries_.size()) - 1;
  }

  HardState hard_state_;
  SnapshotPtr snapshot_;
  // First Entry is <0, 0> when start up or latest snapshot's <index, term>.
  // So entries_[i] has raft log position i + snapshot_->metadata().index().
  std::vector<EntryPtr> entries_;
};

using MemoryStoragePtr = std::shared_ptr<MemoryStorage>;

}  // namespace jraft
