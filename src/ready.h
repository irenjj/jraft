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
#include "common/noncopyable.h"
#include "soft_state.h"

namespace jraft {

class Raft;
using RaftPtr = std::shared_ptr<Raft>;

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, e.g. given a unique id as
// RequestCtx.
struct ReadState {
  uint64_t index;
  std::string request_ctx;
};

// Ready encapsulates the entries and messages that are ready to read, be
// saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
class Ready : public Noncopyable {
 public:
  Ready();
  Ready(const RaftPtr& r, const SoftState& prev_cs, const HardState& prev_hs);
  ~Ready() = default;

  static bool MustSync(const HardState& hs, const HardState& prev_hs,
                       size_t ents_num);
  bool ContainsUpdates();
  uint64_t AppliedCursor() const;

  void ClearMessages();

  const SoftState& soft_state() const { return soft_state_; }
  const HardState& hard_state() const { return hard_state_; }
  const std::vector<ReadState>& read_states() const { return read_states_; }
  const std::vector<EntryPtr>& entries() const { return entries_; }
  SnapshotPtr mutable_snapshot() { return snapshot_; }
  const std::vector<EntryPtr>& committed_entries() const {
    return committed_entries_;
  }
  void set_committed_entries(const std::vector<EntryPtr>& committed_entries) {
    committed_entries_ = committed_entries;
  }
  const std::vector<Message>& messages() const { return messages_; }
  bool must_sync() const { return must_sync_; }

 private:
  // The current volatile state of a Node.
  // SoftState will be nullptr if there is no update.
  // It is not required to consume or store SoftState.
  SoftState soft_state_;

  // The current state of a Node to be saved to stable storage BEFORE
  // Messages are sent.
  // HardState will be equal to empty state if there is no update.
  HardState hard_state_;

  // read_states_ can be used for node to save linearizable read requests
  // locally when its applied index is greater than the index in
  // ReadState.
  std::vector<ReadState> read_states_;

  // entries_ specifies entries to be saved to stable storage BEFORE
  // Messages are sent.
  std::vector<EntryPtr> entries_;

  // snapshot_ specifies the snapshot to be saved to stable storage.
  SnapshotPtr snapshot_;

  // committed_entries_ specifies entries to be committed to a
  // store/state-machine. These have previously been committed to stable store.
  std::vector<EntryPtr> committed_entries_;

  // messages_ specifies outbound message to be sent AFTER entries are
  // committed to stable storage.
  // If it contains a kMsgSnap message, the application MUST report back to raft
  // when the snapshot has been received or has failed by calling
  // ReportSnapshot().
  std::vector<Message> messages_;

  // must_sync_ indicates whether the hard_state_ and entries_ must be
  // synchronously written to disk of if an asynchronous write is permissible.
  bool must_sync_;
};

using ReadyPtr = std::shared_ptr<Ready>;

}  // namespace jraft
