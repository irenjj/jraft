// Copyright (c) renjj - All Rights Reserved

// Copyright 2019 The etcd Authors
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

#include <cstdint>
#include <string>

#include "common/conf.h"
#include "common/noncopyable.h"
#include "errnum.pb.h"
#include "inflights.h"

namespace jraft {

// ProgressState is the state of a tracked follower.
enum ProgressState {
  // kStateProbe indicates a follower whose last index isn't known. Such a
  // follower is "probed" (i.e. an append sent periodically) to narrow down
  // its last index. In the ideal (and common) case, only one round of probing
  // is necessary as the follower will react with a hint. Followers that are
  // probed over extended periods of time are often offline.
  kStateProbe = 0,
  // kStateReplicate is the state steady in which a follower eagerly receives
  // log entries to append to its log.
  kStateReplicate = 1,
  // kStateSnapshot indicates a follower that needs log entries not available
  // from the leader's Raft log. Such a follower needs a full snapshot to
  // return to kStateReplicate.
  kStateSnapshot = 2,
};

static const std::string ProgressStateStr[] = {
    "kStateProbe",
    "kStateReplicate",
    "kStateSnapshot",
};

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
class Progress : Noncopyable {
 public:
  explicit Progress(uint64_t match = 0, uint64_t next = 1,
                    size_t max_size = kMaxSize, uint64_t max_bytes = kNoLimit);

  void ResetState(ProgressState state);
  void BecomeProbe();
  void BecomeReplicate();
  void BecomeSnapshot(uint64_t snapshoti);
  bool UpdateOnEntriesSend(size_t ents_size, uint64_t bytes,
                           uint64_t next_index);
  bool MaybeUpdate(uint64_t n);
  void OptimisticUpdate(uint64_t n);
  bool MaybeDecrTo(uint64_t rejected, uint64_t match_hint);
  bool IsPaused();
  std::string String();
  void Clear();

  uint64_t match() const { return match_; }
  void set_match(uint64_t match) { match_ = match; }

  uint64_t next() const { return next_; }
  void set_next(uint64_t next) { next_ = next; }

  ProgressState state() const { return state_; }
  void set_state(ProgressState state) { state_ = state; }

  uint64_t pending_snapshot() const { return pending_snapshot_; }
  void set_pending_snapshot(uint64_t pending_snapshot) {
    pending_snapshot_ = pending_snapshot;
  }

  bool recent_active() const { return recent_active_; }
  void set_recent_active(bool recent_active) { recent_active_ = recent_active; }

  bool msg_app_flow_paused() const { return msg_app_flow_paused_; }
  void set_msg_app_flow_paused(bool msg_app_flow_paused) {
    msg_app_flow_paused_ = msg_app_flow_paused;
  }

  bool is_learner() const { return is_learner_; }
  void set_is_learner(bool is_learner) { is_learner_ = is_learner; }

  Inflights* mutable_inflights() { return inflights_.get(); }

 private:
  uint64_t match_;
  uint64_t next_;
  // state_ defines how the leader should interact with the follower.
  //
  // When in kStateProbe, leader sends at most one replication message per
  // heartbeat interval. It also probes actual progress of the follower.
  //
  // When in kStateReplicate, leader optimistically increases next to the
  // latest entry sent after sending replication message. This is an optimized
  // state for fast replicating log entries to the follower.
  //
  // When in kStateSnapshot, leader should have sent out snapshot before and
  // stops sending any replication message.
  ProgressState state_;

  // pending_snapshot_ is used in StateSnapshot.
  // If there is a pending snapshot, the pendingSnapshot will be set to the
  // index of the snapshot. If pendingSnapshot is set, the replication process
  // of this Progress will be paused. raft will not resend snapshot until the
  // pending one is reported to be failed.
  uint64_t pending_snapshot_;

  // recent_active_ is true if the progress is recently active. Receiving any
  // messages from the corresponding follower indicates the progress is active.
  // RecentActive can be reset to false after an election timeout.
  // This is always true on the leader.
  bool recent_active_;

  // msg_app_flow_paused_ is used when the kMsgApp flow to a node is throttled.
  // This happens in kStateProbe, or kStateReplicate with saturated Inflights.
  // In both cases, we need to continue sending kMsgApp once in a while to
  // guarantee progress, but we only do so when msg_app_flow_paused_ is false
  // (it is reset on receiving a heartbeat response), to not overflow the
  // receiver. See IsPaused().
  bool msg_app_flow_paused_;

  // inflights_ is a sliding window for the inflight messages.
  // Each inflight message contains one or more log entries.
  // The max number of entries per message is defined in raft config as
  // kMaxSizePerMsg. Thus, inflight effectively limits both the number of
  // inflight messages and the bandwidth each Progress can use.
  // When inflights is Full, no more message should be sent.
  // When a leader sends out a message, the index of the last
  // entry should be added to inflights. The index MUST be added into inflights
  // in order. When a leader receives a reply, the previous inflights should
  // be freed by calling inflights. FreeLE with the index of the last received
  // entry.
  InflightsPtr inflights_;

  // is_learner_ is true if this progress is tracked for a learner.
  bool is_learner_;
};

using ProgressPtr = std::shared_ptr<Progress>;

}  // namespace jraft
