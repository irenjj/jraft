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
#include "log_unstable.h"

#include <jrpc/base/logging/logging.h>

namespace jraft {

LogUnstable::LogUnstable() : snapshot_(nullptr), offset_(0) {}

LogUnstable::LogUnstable(SnapshotPtr snapshot,
                         const std::vector<EntryPtr>& entries, uint64_t offset)
    : snapshot_(std::move(snapshot)), entries_(entries), offset_(offset) {}

// Returns the index of the first possible entry in entries if it has
// a snapshot.
bool LogUnstable::MaybeFirstIndex(uint64_t* first) const {
  *first = 0;

  if (snapshot_ != nullptr) {
    *first = snapshot_->metadata().index() + 1;
    return true;
  }

  return false;
}

// Returns the last index if it has as least one unstable entry or snapshot.
bool LogUnstable::MaybeLastIndex(uint64_t* last) const {
  *last = 0;

  if (!entries_.empty()) {
    *last = offset_ + entries_.size() - 1;
    return true;
  }

  if (snapshot_ != nullptr) {
    *last = snapshot_->metadata().index();
    return true;
  }

  return false;
}

// Returns the term of the entry at index i, if there's any.
bool LogUnstable::MaybeTerm(uint64_t i, uint64_t* term) const {
  *term = 0;

  if (i < offset_) {
    if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
      *term = snapshot_->metadata().term();
      return true;
    }

    return false;
  }

  uint64_t last = 0;
  bool ok = MaybeLastIndex(&last);
  if (!ok || i > last) {
    return false;
  }

  *term = entries_[i - offset_]->term();
  return true;
}

void LogUnstable::Slice(uint64_t lo, uint64_t hi,
                        std::vector<EntryPtr>* slice) const {
  assert(slice->empty());

  MustCheckOutOfBounds(lo, hi);
  slice->assign(entries_.begin() + static_cast<long>(lo - offset_),
                entries_.begin() + static_cast<long>(hi - offset_));
}

void LogUnstable::StableTo(uint64_t i, uint64_t t) {
  uint64_t gt = 0;
  bool ok = MaybeTerm(i, &gt);
  if (!ok) {
    return;
  }

  // if i < offset, term is matched with the snapshot
  // only update the unstable entries if term is
  // matched with an unstable entry
  if (gt == t && i >= offset_) {
    entries_.erase(entries_.begin(),
                   entries_.begin() + static_cast<long>(i + 1 - offset_));
    offset_ = i + 1;
  }
}

void LogUnstable::StableSnapTo(uint64_t i) {
  if (snapshot_ != nullptr && snapshot_->metadata().index() == i) {
    snapshot_ = nullptr;
  }
}

void LogUnstable::Restore(SnapshotPtr s) {
  offset_ = s->metadata().index() + 1;
  entries_.clear();
  snapshot_ = s;
}

void LogUnstable::TruncateAndAppend(const std::vector<EntryPtr>& entries) {
  uint64_t after = entries[0]->index();
  if (after == offset_ + static_cast<uint64_t>(entries_.size())) {
    // after is the next index in the entries_
    // directly append
    entries_.insert(entries_.end(), entries.begin(), entries.end());
  } else if (after <= offset_) {
    JLOG_INFO << "replace the unstable entries from index " << after;
    // the log is being truncated to before out current offset
    // portion, so set the offset and replace the entries
    offset_ = after;
    entries_ = entries;
  } else {
    // truncate to after and copy to entries_, then append
    JLOG_INFO << "truncate the unstable entries before index " << after;
    std::vector<EntryPtr> tmp;
    Slice(offset_, after, &tmp);
    entries_ = tmp;
    entries_.insert(entries_.end(), entries.begin(), entries.end());
  }
}

void LogUnstable::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
  if (lo > hi) {
    JLOG_FATAL << "invalid slice lo: " << lo << " > "
               << "hi: " << hi;
  }

  uint64_t upper = offset_ + static_cast<uint64_t>(entries_.size());
  if (lo < offset_ || hi > upper) {
    JLOG_FATAL << "unstable slice[" << lo << ", " << hi << "] out of bound"
               << "[" << offset_ << ", " << upper << "]";
  }
}

}  // namespace jraft
