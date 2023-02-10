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

#include "common/conf.h"
#include "common/noncopyable.h"

namespace jraft {

// entries_[i] has raft log position i + offset_. offset_ maybe less than
// the highest log position in storage, which means that the next write to
// storage might need to truncate the log before persisting entries_.
class LogUnstable : public Noncopyable {
 public:
  LogUnstable();
  LogUnstable(SnapshotPtr snapshot, const std::vector<EntryPtr>& entries,
              uint64_t offset);
  ~LogUnstable() = default;

  bool MaybeFirstIndex(uint64_t* first) const;
  bool MaybeLastIndex(uint64_t* last) const;
  bool MaybeTerm(uint64_t i, uint64_t* term) const;
  void Slice(uint64_t lo, uint64_t hi, std::vector<EntryPtr>* slice) const;

  void StableTo(uint64_t i, uint64_t t);
  void StableSnapTo(uint64_t i);
  void Restore(SnapshotPtr s);
  void TruncateAndAppend(const std::vector<EntryPtr>& entries);

  // helper functions to get/set member variables.
  SnapshotPtr snapshot() const { return snapshot_; }
  SnapshotPtr mutable_snapshot() { return snapshot_; }
  void set_snapshot(SnapshotPtr snapshot) { snapshot_ = std::move(snapshot); }

  const std::vector<EntryPtr>& entries() const { return entries_; }
  void set_entries(const std::vector<EntryPtr>& entries) { entries_ = entries; }

  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

 private:
  void MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

  // the incoming unstable snapshot, if any.
  SnapshotPtr snapshot_;
  // all entries that have not yet been written to storage.
  std::vector<EntryPtr> entries_;
  uint64_t offset_;
};

}  // namespace jraft
