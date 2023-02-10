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
#include "memory_storage.h"

#include <jrpc/base/logging/logging.h>

#include "common/util.h"

namespace jraft {

// Creates an empty MemoryStorage.
MemoryStorage::MemoryStorage() : snapshot_(std::make_shared<Snapshot>()) {
  auto meta = snapshot_->mutable_metadata();
  meta->set_index(0);
  meta->set_term(0);
  // When starting from scratch populate the list with a dummy entry at
  // term = 0, index = 0.
  entries_.push_back(NewEnt(0, 0));
}

ErrNum MemoryStorage::InitialState(HardState* hs, ConfState* cs) {
  *hs = hard_state_;
  *cs = snapshot_->metadata().conf_state();
  return kOk;
}

ErrNum MemoryStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                              std::vector<EntryPtr>* entries) {
  assert(entries->empty());

  uint64_t offset = entries_[0]->index();
  if (lo <= offset) {
    return kErrCompacted;
  }

  if (hi > LastIndex() + 1) {
    JLOG_FATAL << "entries' hi " << hi << " is out of bound of last index "
               << LastIndex();
  }

  // only contains dummy entries
  if (entries_.size() == 1) {
    return kErrUnavailable;
  }

  entries->insert(entries->end(),
                  entries_.begin() + static_cast<long>(lo - offset),
                  entries_.begin() + static_cast<long>(hi - offset));
  LimitSize(max_size, entries);
  return kOk;
}

ErrNum MemoryStorage::Term(uint64_t i, uint64_t* term) {
  *term = 0;

  uint64_t offset = entries_[0]->index();
  if (i < offset) {
    return kErrCompacted;
  }

  if (i - offset >= entries_.size()) {
    return kErrUnavailable;
  }

  *term = entries_[i - offset]->term();
  return kOk;
}

ErrNum MemoryStorage::FirstIndex(uint64_t* index) {
  *index = FirstIndex();
  return kOk;
}

ErrNum MemoryStorage::LastIndex(uint64_t* index) {
  *index = LastIndex();
  return kOk;
}

ErrNum MemoryStorage::GetSnapshot(SnapshotPtr& snapshot) {
  snapshot = snapshot_;
  return kOk;
}

// Make a snapshot which can be retrieved with GetSnapshot() and can be used
// to reconstruct the state at that point. If any configuration changes have
// been made since that last compaction, the result of the last
// ApplyConfChange() must be passed in.
ErrNum MemoryStorage::CreateSnapshot(uint64_t i, ConfState* cs,
                                     const std::string& data,
                                     SnapshotPtr* snap) {
  if (i <= snapshot_->metadata().index()) {
    return kErrSnapOutOfData;
  }

  uint64_t offset = entries_[0]->index();
  if (i > LastIndex()) {
    JLOG_FATAL << "snapshot" << i << " is out of bound last_index"
               << LastIndex();
  }

  auto meta = snapshot_->mutable_metadata();
  meta->set_index(i);
  meta->set_term(entries_[i - offset]->term());
  if (cs != nullptr) {
    *(meta->mutable_conf_state()) = *cs;
  }
  snapshot_->set_data(data);
  if (snap != nullptr) {
    *snap = snapshot_;
  }

  return kOk;
}

// Overwrites the content of Storage object with the given snapshot. If the
// snapshot is stale, ignore it.
ErrNum MemoryStorage::ApplySnapshot(const SnapshotPtr& snapshot) {
  // handle check for old snapshot being applied
  uint64_t ms_index = snapshot_->metadata().index();
  uint64_t snap_index = snapshot->metadata().index();
  if (ms_index >= snap_index) {
    return kErrSnapOutOfData;
  }

  snapshot_ = snapshot;
  entries_.clear();
  entries_.push_back(
      NewEnt(snapshot_->metadata().index(), snapshot_->metadata().term()));
  return kOk;
}

// Discards all log entries prior to compact_index. It is the application's
// responsibility to not attempt to compact an index greater than applied_.
ErrNum MemoryStorage::Compact(uint64_t compact_index) {
  uint64_t offset = entries_[0]->index();
  if (compact_index <= offset) {
    return kErrCompacted;
  }

  if (compact_index > LastIndex()) {
    JLOG_FATAL << "compact " << compact_index << " is out of bound last_idx "
               << LastIndex();
  }

  uint64_t i = compact_index - offset;
  entries_.erase(entries_.begin(), entries_.begin() + i);
  return kOk;
}

// If entries in MemoryStorage is overlapped with entries to append, truncate
// entries in MemoryStorage. It is the Application's responsibility to not
// attempt to pass entries that are not continuous.
ErrNum MemoryStorage::Append(const std::vector<EntryPtr>& entries) {
  if (entries.empty()) {
    return kOk;
  }

  uint64_t memory_first = FirstIndex();
  uint64_t app_last = entries[0]->index() + entries.size() - 1;

  // shortcut if there is no new entry
  if (app_last < memory_first) {
    return kOk;
  }

  // truncate compacted entries
  std::vector<EntryPtr> append_ents;
  if (memory_first > entries[0]->index()) {
    uint64_t index = memory_first - entries[0]->index();
    append_ents = std::vector<EntryPtr>(
        entries.begin() + static_cast<long>(index), entries.end());
  } else {
    append_ents = entries;
  }

  uint64_t offset = append_ents[0]->index() - entries_[0]->index();
  if (entries_.size() > offset) {
    entries_.erase(entries_.begin() + static_cast<long>(offset),
                   entries_.end());
    entries_.insert(entries_.end(), append_ents.begin(), append_ents.end());
  } else if (entries_.size() == offset) {
    entries_.insert(entries_.end(), append_ents.begin(), append_ents.end());
  } else {
    JLOG_FATAL << "missing log entry [last: " << LastIndex()
               << ", append at: " << append_ents[0]->index() << "]";
  }

  return kOk;
}

}  // namespace jraft
