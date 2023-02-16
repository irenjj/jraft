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

#include <vector>

#include "common/conf.h"
#include "common/noncopyable.h"
#include "errnum.pb.h"

namespace jraft {

// Storage is an interface that may be implemented by the application to
// retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will become
// inoperable and refuse to participate in elections, the application is
// responsible for cleanup and recovery in this case.
class Storage : Noncopyable {
 public:
  virtual ~Storage() = default;

  // Returns the saved HardState and ConfState information.
  virtual ErrNum InitialState(HardState* hs, ConfState* cs) = 0;

  // Returns a slice of log entries in the range [lo, hi), max_size limits the
  // total size of the log entries returned, but entries returns at least
  // one entry if any.
  virtual ErrNum Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                         std::vector<EntryPtr>* entries) = 0;

  // Returns the term of entry i, which must be in the range
  // [FirstIndex() - 1, LastIndex()], the term of the entry before FirstIndex()
  // is retained for matching purposes even though the rest of that entry may
  // not be available.
  virtual ErrNum Term(uint64_t i, uint64_t* term) = 0;

  // Returns the index of the first log entry that is possibly available via
  // Entries() (older entries have been incorporated into the latest
  // Snapshot; if Storage only contains the dummy entry the first log entry
  // is not available).
  virtual ErrNum FirstIndex(uint64_t* index) = 0;

  // Returns the index of the last entry in the log.
  virtual ErrNum LastIndex(uint64_t* index) = 0;

  // Returns the most recent snapshot. If snapshot is temporary unavailable,
  // it should return kErrSnapshotTemporarilyUnavailable, so raft state machine
  // could know that Storage needs some time to prepare snapshot and call
  // GetSnapshot() later.
  virtual ErrNum GetSnapshot(SnapshotPtr& snapshot) = 0;
  virtual ErrNum CreateSnapshot(uint64_t i, ConfState* cs,
                                const std::string& data,
                                SnapshotPtr* snapshot) = 0;
  virtual ErrNum ApplySnapshot(const Snapshot& snapshot) = 0;

  virtual ErrNum Compact(uint64_t compact_index) = 0;
  virtual ErrNum Append(const std::vector<EntryPtr>& entries) = 0;
};

using StoragePtr = std::shared_ptr<Storage>;

}  // namespace jraft
