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
#include "ready.h"

#include "common/util.h"

namespace jraft {

Ready::Ready()
    : soft_state_(),
      hard_state_(),
      snapshot_(std::make_shared<Snapshot>()),
      must_sync_(false) {}

Ready::Ready(const RaftPtr& r, const SoftState& prev_cs,
             const HardState& prev_hs) {
  entries_.clear();
  r->raft_log()->UnstableEntries(&entries_);
  committed_entries_.clear();
  r->raft_log()->NextEnts(&committed_entries_);

  auto ss = r->ConstructSoftState();
  if (!IsSoftStateEqual(ss, prev_cs)) {
    soft_state_ = ss;
  }

  auto hs = r->ConstructHardState();
  if (!IsHardStateEqual(hs, prev_hs)) {
    hard_state_ = hs;
  }

  auto snap = r->raft_log()->mutable_unstable().mutable_snapshot();
  if (snap != nullptr) {
    snapshot_ = snap;
  } else {
    snapshot_ = std::make_shared<Snapshot>();
    snapshot_->mutable_metadata()->set_index(0);
  }

  if (!r->read_states().empty()) {
    read_states_ = r->read_states();
  }
  must_sync_ = MustSync(r->ConstructHardState(), prev_hs, entries_.size());
}

// MustSync returns true is the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
bool Ready::MustSync(const HardState& hs, const HardState& prev_hs,
                     size_t ents_num) {
  // Persistent state on all servers:
  // (Updated on stable storage before responding to RPCs)
  // current_term
  // voted_for
  // log_entries
  return ents_num != 0 || hs.vote() != prev_hs.vote() ||
         hs.term() != prev_hs.term();
}

bool Ready::ContainsUpdates() {
  return !IsEmptySoftState(soft_state_) || !IsEmptyHardState(hard_state_) ||
         !IsEmptySnap(snapshot_) || !entries_.empty() ||
         !committed_entries_.empty() || !messages_.empty() ||
         !read_states_.empty();
}

// AppliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information
// is contained in the Ready, returns zero.
uint64_t Ready::AppliedCursor() const {
  if (!committed_entries_.empty()) {
    return committed_entries_.back()->index();
  }

  if (snapshot_ == nullptr) {
    JLOG_FATAL << "snapshot shouldn't be nullptr";
  }

  if (snapshot_->metadata().index() > 0) {
    return snapshot_->metadata().index();
  }

  return 0;
}

void Ready::ClearMessages() { messages_.clear(); }

}  // namespace jraft
