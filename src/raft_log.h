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

#include "common/conf.h"
#include "errnum.pb.h"
#include "log_unstable.h"
#include "storage.h"

namespace jraft {

class RaftLog {
 public:
  explicit RaftLog(StoragePtr s, uint64_t max_next_ents_size = kNoLimit);
  ~RaftLog() = default;

  std::string String();

  uint64_t FindConflict(const std::vector<EntryPtr>& entries) const;
  uint64_t FindConflictByTerm(uint64_t index, uint64_t term) const;

  bool HasNextEnts() const;
  bool HasPendingSnapshot() const;
  bool IsUpToDate(uint64_t last_i, uint64_t term) const;
  bool MatchTerm(uint64_t i, uint64_t term) const;
  ErrNum MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const;

  static uint64_t ZeroTermOnErrCompacted(uint64_t t, ErrNum err);

  ErrNum GetSnapshot(SnapshotPtr& s) const;
  uint64_t FirstIndex() const;
  uint64_t LastIndex() const;
  uint64_t LastTerm() const;
  ErrNum Term(uint64_t i, uint64_t* term) const;
  ErrNum Entries(uint64_t i, uint64_t max_size,
                 std::vector<EntryPtr>* entries) const;
  void AllEntries(std::vector<EntryPtr>* entries) const;
  void UnstableEntries(std::vector<EntryPtr>* entries) const;
  void NextEnts(std::vector<EntryPtr>* entries) const;
  ErrNum Slice(uint64_t lo, uint64_t hi, uint64_t max_size,
               std::vector<EntryPtr>* entries) const;

  bool MaybeCommit(uint64_t max_index, uint64_t term);
  void CommitTo(uint64_t to_commit);
  void AppliedTo(uint64_t i);
  void StableTo(uint64_t i, uint64_t t);
  void StableSnapTo(uint64_t i);
  bool MaybeAppend(uint64_t index, uint64_t log_term, uint64_t committed,
                   const std::vector<EntryPtr>& entries, uint64_t* last_i);
  uint64_t Append(const std::vector<EntryPtr>& entries);
  void Restore(SnapshotPtr s);

  // helper functions to get/set member variables.
  const Storage& storage() const { return *storage_; }
  StoragePtr mutable_storage() { return storage_; }

  const LogUnstable& unstable() const { return unstable_; }
  LogUnstable& mutable_unstable() { return unstable_; }

  uint64_t committed() const { return committed_; }
  void set_committed(uint64_t committed) { committed_ = committed; }

  uint64_t applied() const { return applied_; }

  uint64_t max_next_ents_size() const { return max_next_ents_size_; }

 private:
  // storage_ contains all stable entries since the last snapshot.
  StoragePtr storage_;
  // unstable_ contains all unstable entries and snapshot.
  // they will be saved into storage_
  LogUnstable unstable_;
  // committed is the highest log position that is known to be in
  // stable storage on a quorum of nodes
  uint64_t committed_;
  // applied_ is the highest log position that the application has
  // been instructed to apply to its state machine.
  // applied_ <= committed_
  uint64_t applied_;
  // max_next_ents_size_ is the maximum number aggregate byte size of
  // the messages returned from calls to NextEnts
  uint64_t max_next_ents_size_;
};

typedef std::shared_ptr<RaftLog> RaftLogPtr;

}  // namespace jraft
