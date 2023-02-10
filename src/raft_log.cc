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
#include "raft_log.h"

#include <jrpc/base/logging/logging.h>

#include <utility>

#include "common/util.h"

namespace jraft {

// Returns log using the given storage. It recovers the log to
// the state that it just commits and applies the latest snapshot.
RaftLog::RaftLog(StoragePtr s, uint64_t max_next_ents_size)
    : storage_(std::move(s)), max_next_ents_size_(max_next_ents_size) {
  if (storage_ == nullptr) {
    JLOG_FATAL << "storage must not be nullptr";
  }

  uint64_t first = 0;
  ErrNum err = storage_->FirstIndex(&first);
  if (err != kOk) {
    JLOG_FATAL << "fail to get first index, err: " << err;
  }
  uint64_t last = 0;
  err = storage_->LastIndex(&last);
  if (err != kOk) {
    JLOG_FATAL << "fail to get last index, err: " << err;
  }
  unstable_.set_offset(last + 1);
  // Initialize our committed and applied pointers to the time of the last
  // compaction.
  committed_ = first - 1;
  applied_ = first - 1;
}

std::string RaftLog::String() {
  char tmp[200];
  snprintf(tmp, sizeof(tmp),
           "committed=%lu, applied=%lu, unstable_.offset=%lu,\
           unstable_.entries_.size()=%lu",
           committed_, applied_, unstable_.offset(),
           unstable_.entries().size());
  return tmp;
}

// Finds the index of the conflict.
// + It returns the first pair of conflicting entries between the existing
//   entries and the given entries, if there are any.
// + If there is no conflicting entries, and the existing entries contains
//   all the given entries, zero will be returned.
// + If there is no conflicting entries, but the given entries contains
//   new entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same
// index but a different term.
// The index of the given entries MUST be continuously increasing.
uint64_t RaftLog::FindConflict(const std::vector<EntryPtr>& entries) const {
  for (const auto& ne : entries) {
    if (!MatchTerm(ne->index(), ne->term())) {
      if (ne->index() <= LastIndex()) {
        uint64_t term = 0;
        ErrNum err = Term(ne->index(), &term);
        JLOG_INFO << "found conflict at index " << ne->index()
                  << " [existing term: " << ZeroTermOnErrCompacted(term, err)
                  << "]";
      }

      return ne->index();
    }
  }

  return 0;
}

// Takes an <index, term> pair (indicating a conflicting log entry on a
// leader/follower during an append) and finds the largest index in log l with
// a term <= 'term' and an index <= 'index'. If no such index exists in the
// log, the log's first index is returned.
//
// The index provided MUST be equal to or less than LastIndex(). Invalid
// inputs log a warning and the input index is returned.
uint64_t RaftLog::FindConflictByTerm(uint64_t index, uint64_t term) const {
  uint64_t li = LastIndex();
  if (index > li) {
    // NB: such calls should not exist, but since there is a straight
    // forward way to recover, do it.
    //
    // It is tempting to also check something about the first index,
    // but there is odd behavior with peers that have no log,
    // in which case LastIndex will return zero and FirstIndex will
    // return one, which leads to calls with an index of zero into
    // this method.
    JLOG_WARN << "index " << index << " is out of range [0, " << li << "]";
    return index;
  }

  do {
    uint64_t log_term;
    ErrNum err = Term(index, &log_term);
    if (log_term <= term || err != kOk) {
      break;
    }
  } while (index--);

  return index;
}

// Returns if there is any available entries for execution.
// This is a fast check without heavy Slice() in NextEnts()
bool RaftLog::HasNextEnts() const {
  uint64_t off = std::max(applied_ + 1, FirstIndex());
  return committed_ + 1 > off;
}

// Returns if there is pending snapshot waiting for applying.
bool RaftLog::HasPendingSnapshot() const {
  return unstable_.snapshot() != nullptr &&
         !IsEmptySnap(*(unstable_.snapshot()));
}

// IsUpToDate determines if the given <last_i, term> log is more update-to-date
// by comparing the index and term of the last entries in the existing logs.
// if the logs have last entries with different term, then the log with the
// latter term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger last index is more up-to-date. If the logs
// are the same, the given log is up-to-date.
bool RaftLog::IsUpToDate(uint64_t last_i, uint64_t term) const {
  return term > LastTerm() || (term == LastTerm() && last_i >= LastIndex());
}

bool RaftLog::MatchTerm(uint64_t i, uint64_t term) const {
  uint64_t t = 0;
  ErrNum err = Term(i, &t);
  if (err != kOk) {
    return false;
  }
  return t == term;
}

// FirstIndex() <= lo <= hi <= FirstIndex() + len(entries)
ErrNum RaftLog::MustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
  if (lo > hi) {
    JLOG_FATAL << "invalid slice " << lo << " > " << hi;
  }

  uint64_t fi = FirstIndex();
  if (lo < fi) {
    return kErrCompacted;
  }

  uint64_t len = LastIndex() + 1 - fi;
  if (hi > fi + len) {
    JLOG_FATAL << "slice[" << lo << "," << hi << "], out of bound "
               << "[" << fi << "," << LastIndex() << "]";
  }
  return kOk;
}

uint64_t RaftLog::ZeroTermOnErrCompacted(uint64_t t, ErrNum err) {
  if (err == kOk) {
    return t;
  }
  if (err == kErrCompacted) {
    return 0;
  }
  JLOG_FATAL << "unexpected err " << err;
  return 0;
}

ErrNum RaftLog::GetSnapshot(SnapshotPtr& s) const {
  if (unstable_.snapshot() != nullptr) {
    s = unstable_.snapshot();
    return kOk;
  }

  return storage_->GetSnapshot(s);
}

uint64_t RaftLog::FirstIndex() const {
  uint64_t first = 0;
  if (unstable_.MaybeFirstIndex(&first)) {
    return first;
  }

  ErrNum err = storage_->FirstIndex(&first);
  if (err != kOk) {
    JLOG_FATAL << "shouldn't err";
  }
  return first;
}

uint64_t RaftLog::LastIndex() const {
  uint64_t last = 0;
  if (unstable_.MaybeLastIndex(&last)) {
    return last;
  }

  ErrNum err = storage_->LastIndex(&last);
  if (err != kOk) {
    JLOG_FATAL << "shouldn't err";
  }
  return last;
}

uint64_t RaftLog::LastTerm() const {
  uint64_t t = 0;
  ErrNum err = Term(LastIndex(), &t);
  if (err != kOk) {
    JLOG_FATAL << "unexpected error when getting the last term " << err;
  }
  return t;
}

ErrNum RaftLog::Term(uint64_t i, uint64_t* term) const {
  *term = 0;

  // the valid term range [index of dummy entry, last index]
  uint64_t dummy_index = FirstIndex() - 1;
  if (i < dummy_index || i > LastIndex()) {
    return kOk;
  }

  uint64_t t = 0;
  if (unstable_.MaybeTerm(i, &t)) {
    *term = t;
    return kOk;
  }

  ErrNum err = storage_->Term(i, &t);
  if (err == kOk) {
    *term = t;
    return kOk;
  }
  if (err == kErrCompacted || err == kErrUnavailable) {
    return err;
  }

  JLOG_FATAL << "shouldn't come here";
  return kOk;
}

ErrNum RaftLog::Entries(uint64_t i, uint64_t max_size,
                        std::vector<EntryPtr>* entries) const {
  if (i > LastIndex()) {
    return kOk;
  }

  return Slice(i, LastIndex() + 1, max_size, entries);
}

// Returns all entries in the log.
void RaftLog::AllEntries(std::vector<EntryPtr>* entries) const {
  ErrNum err = Entries(FirstIndex(), kNoLimit, entries);
  if (err == kOk) {
    return;
  }
  if (err == kErrCompacted) {
    // try again if there was a racing compaction
    return AllEntries(entries);
  }
  JLOG_FATAL << err;
}

void RaftLog::UnstableEntries(std::vector<EntryPtr>* entries) const {
  if (unstable_.entries().empty()) {
    return;
  }
  *entries = unstable_.entries();
}

// Returns all the available entries for execution. If applied is smaller than
// the index of snapshot, it returns all committed entries after the index of
// snapshot.
void RaftLog::NextEnts(std::vector<EntryPtr>* entries) const {
  uint64_t off = std::max(applied_ + 1, FirstIndex());
  if (committed_ + 1 > off) {
    ErrNum err = Slice(off, committed_ + 1, kNoLimit, entries);
    if (err != kOk) {
      JLOG_FATAL << "unexpected err when getting unapplied entries " << err;
    }
  }
}

// Returns a slice of log entries from lo through hi - 1.
ErrNum RaftLog::Slice(uint64_t lo, uint64_t hi, uint64_t max_size,
                      std::vector<EntryPtr>* entries) const {
  ErrNum err = MustCheckOutOfBounds(lo, hi);
  if (err != kOk) {
    return err;
  }

  if (lo == hi) {
    return kOk;
  }

  if (lo < unstable_.offset()) {
    std::vector<EntryPtr> stored_ents;
    ErrNum err2 = storage_->Entries(lo, std::min(hi, unstable_.offset()),
                                    max_size, &stored_ents);
    if (err2 == kErrCompacted) {
      JLOG_FATAL << "entries[" << lo << "," << std::min(hi, unstable_.offset())
                 << "] is unavailable from storage";
    } else if (err2 != kOk) {
      return err;
    }

    entries->insert(entries->end(), stored_ents.begin(), stored_ents.end());

    // check if entries has reached the size limitation
    if (stored_ents.size() < std::min(hi, unstable_.offset()) - lo) {
      return kOk;
    }
  }

  if (hi > unstable_.offset()) {
    std::vector<EntryPtr> unstable_ents;
    unstable_.Slice(std::max(lo, unstable_.offset()), hi, &unstable_ents);
    entries->insert(entries->end(), unstable_ents.begin(), unstable_ents.end());
  }

  LimitSize(max_size, entries);
  return kOk;
}

bool RaftLog::MaybeCommit(uint64_t max_index, uint64_t term) {
  uint64_t t;
  ErrNum err = Term(max_index, &t);
  if (max_index > committed_ && ZeroTermOnErrCompacted(t, err) == term) {
    CommitTo(max_index);
    return true;
  }
  return false;
}

void RaftLog::CommitTo(uint64_t to_commit) {
  // never decrease commit
  if (committed_ < to_commit) {
    if (LastIndex() < to_commit) {
      JLOG_FATAL << "to_commit " << to_commit << " is out of range "
                 << LastIndex() << ". Was the raft log corrupted, "
                 << "truncated, or lost?";
    }
    committed_ = to_commit;
  }
}

void RaftLog::AppliedTo(uint64_t i) {
  if (i == 0) {
    return;
  }
  if (committed_ < i || i < applied_) {
    JLOG_FATAL << "applied " << i << " is out of range [prev_applied"
               << applied_ << ", committed" << committed_;
  }
  applied_ = i;
}

void RaftLog::StableTo(uint64_t i, uint64_t t) { unstable_.StableTo(i, t); }

void RaftLog::StableSnapTo(uint64_t i) { unstable_.StableSnapTo(i); }

// Returns (0, false) if the entries cannot be appended.
// Otherwise, it returns (last index of new entries, true).
bool RaftLog::MaybeAppend(uint64_t index, uint64_t log_term, uint64_t committed,
                          const std::vector<EntryPtr>& entries,
                          uint64_t* last_i) {
  *last_i = 0;

  if (MatchTerm(index, log_term)) {
    uint64_t last_new_i = index + entries.size();
    uint64_t ci = FindConflict(entries);
    if (ci != 0 && ci <= committed_) {
      JLOG_FATAL << "entry " << ci << " conflict with committed entry"
                 << " [committed: " << committed_ << "]";
    }

    if (ci != 0) {
      uint64_t offset = index + 1;
      std::vector<EntryPtr> app_entries(
          entries.begin() + static_cast<long>(ci - offset), entries.end());
      Append(app_entries);
    }

    CommitTo(std::min(committed, last_new_i));
    *last_i = last_new_i;
    return true;
  }

  return false;
}

uint64_t RaftLog::Append(const std::vector<EntryPtr>& entries) {
  if (entries.empty()) {
    return LastIndex();
  }

  uint64_t after = entries[0]->index() - 1;
  if (after < committed_) {
    JLOG_FATAL << "after: " << after
               << " is out of range committed: " << committed_;
  }
  unstable_.TruncateAndAppend(entries);
  return LastIndex();
}

void RaftLog::Restore(SnapshotPtr s) {
  JLOG_INFO << "log " << String() << " starts to restore_test snapshot [index: "
            << s->metadata().index() << ", term: " << s->metadata().term()
            << "]";
  committed_ = s->metadata().index();
  unstable_.Restore(s);
}

}  // namespace jraft
