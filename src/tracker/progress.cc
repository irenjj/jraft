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
#include "progress.h"

#include <jrpc/base/logging/logging.h>

#include <cstdio>

#include "inflights.h"

namespace jraft {

Progress::Progress(uint64_t match, uint64_t next, size_t max_size,
                   uint64_t max_bytes)
    : match_(match),
      next_(next),
      state_(kStateProbe),
      pending_snapshot_(0),
      recent_active_(false),
      msg_app_flow_paused_(false),
      inflights_(new Inflights(max_size, max_bytes)),
      is_learner_(false) {}

// Moves the Progress into the specified state.
void Progress::ResetState(ProgressState state) {
  msg_app_flow_paused_ = false;
  pending_snapshot_ = 0;
  state_ = state;
  inflights_->Reset();
}

// Transitions into kStateProbe. next_ is reset to match_ + 1 or optionally
// and if larger, the index of the pending snapshot.
void Progress::BecomeProbe() {
  // If the original state is kStateSnapshot, progress knows that the pending
  // snapshot has been sent to this peer successfully, then probes from
  // pending_snapshot_ + 1.
  if (state_ == kStateSnapshot) {
    uint64_t pending_snapshot = pending_snapshot_;
    ResetState(kStateProbe);
    next_ = std::max(match_ + 1, pending_snapshot + 1);
  } else {
    ResetState(kStateProbe);
    next_ = match_ + 1;
  }
}

// Transitions into kStateReplicate, resetting next_ to match_ + 1.
void Progress::BecomeReplicate() {
  ResetState(kStateReplicate);
  next_ = match_ + 1;
}

// Moves the Progress to kStateSnapshot with the specified pending snapshot
// index.
void Progress::BecomeSnapshot(uint64_t snapshoti) {
  ResetState(kStateSnapshot);
  pending_snapshot_ = snapshoti;
}

// Updates the Progress on the given number of consecutive entries being sent
// in a kMsgApp, with the given total bytes size, appended at and after the
// given log index.
bool Progress::UpdateOnEntriesSend(size_t ents_size, uint64_t bytes,
                                   uint64_t next_index) {
  if (state_ == kStateReplicate) {
    if (ents_size > 0) {
      uint64_t last = next_index + ents_size - 1;
      OptimisticUpdate(last);
      inflights_->Add(last, bytes);
    }
    // If this message overflows the in-flights tracker, or it was already full
    // consider this message being a probe, so that the flow is paused.
    msg_app_flow_paused_ = inflights_->Full();
  } else if (state_ == kStateProbe) {
    // TODO(pavelkalinnikov): this condition captures the previous behaviour,
    // but we should set msg_app_flow_paused_ unconditionally for simplicity,
    // because any kMsgApp in kStateProbe is a probe, not only non-empty ones.
    if (ents_size > 0) {
      msg_app_flow_paused_ = true;
    }
  } else {
    JLOG_ERROR << "sending append in unhandled state "
               << ProgressStateStr[state_];
    return false;
  }
  return true;
}

// Is called when an kMsgAppResp arrives from the follower, with the index
// acked by it. The method returns false if the given n index comes from an
// outdated message. Otherwise, it updates the progress and returns true.
bool Progress::MaybeUpdate(uint64_t n) {
  bool updated = false;
  if (match_ < n) {
    match_ = n;
    updated = true;
    msg_app_flow_paused_ = false;
  }
  next_ = std::max(next_, n + 1);
  return updated;
}

// Signals that appends all the way up to and including index n are in-flight.
// As a result, next_ is increased to n+1.
void Progress::OptimisticUpdate(uint64_t n) { next_ = n + 1; }

// Adjusts the Progress to the receipt of a kMsgApp rejection. The arguments
// are the index of the append message rejected by the follower, and the hint
// that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, next_ is lowered sensibly, and the Progress is
// cleared for sending log entries.
bool Progress::MaybeDecrTo(uint64_t rejected, uint64_t match_hint) {
  if (state_ == kStateReplicate) {
    // The rejection must be state if the progress has matched and "rejected"
    // is smaller than "match".
    if (rejected <= match_) {
      return false;
    }
    // Directly decrease next_ to match_ + 1
    next_ = match_ + 1;
    return true;
  }

  // The rejection must be stale if "rejected" does not match next_ - 1. This
  // is because non-replicating followers are probed one entry at a time.
  if (next_ - 1 != rejected) {
    return false;
  }

  uint64_t min_idx = std::min(rejected, match_hint + 1);
  if (min_idx < 1) {
    next_ = 1;
  } else {
    next_ = min_idx;
  }
  msg_app_flow_paused_ = false;
  return true;
}

// Returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
bool Progress::IsPaused() {
  switch (state_) {
    case kStateProbe:
    case kStateReplicate:
      return msg_app_flow_paused_;
    case kStateSnapshot:
      return true;
    default:
      JLOG_FATAL << "unexpected state";
      return false;
  }
}

std::string Progress::String() {
  char buf[200];
  std::string tail;
  if (is_learner_) {
    tail += " learner";
  }
  if (IsPaused()) {
    tail += " paused";
  }
  if (pending_snapshot_ > 0) {
    tail += " pending_snapshot=";
    tail += std::to_string(pending_snapshot_);
  }
  if (!recent_active_) {
    tail += " inactive";
  }
  if (inflights_->count() > 0) {
    tail += " inflights=";
    tail += std::to_string(inflights_->count());
    if (inflights_->Full()) {
      tail += " [full]";
    }
  }
  snprintf(buf, sizeof(buf), "%s match=%lu next=%lu, %s",
           ProgressStateStr[state_].c_str(), match_, next_, tail.c_str());

  return buf;
}

void Progress::Clear() {
  match_ = 0;
  next_ = 0;
  state_ = kStateProbe;
  recent_active_ = false;
  msg_app_flow_paused_ = false;
  is_learner_ = false;
}

}  // namespace jraft
