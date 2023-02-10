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
#include "rawnode.h"

#include "common/util.h"
#include "ready.h"

namespace jraft {

RawNode::RawNode(Config c)
    : raft_(std::make_shared<Raft>(&c)),
      prev_ss_(raft_->ConstructSoftState()),
      prev_hs_(raft_->ConstructHardState()) {}

// Tick increments the internal logical clock for the RawNode by a single tick.
// Election timeouts and heartbeat timeouts are in units of ticks.
void RawNode::Tick() { raft_->Tick(); }

// Campaign causes the RawNode the transition to candidate state and start
// campaigning to become leader.
ErrNum RawNode::Campaign() { return raft_->Step(Msg(0, 0, kMsgHup)); }

// Propose proposes that data be appended to the log. Note that proposals can
// be lost without notice, therefore it is user's job to ensure proposal
// retries.
ErrNum RawNode::Propose(const std::string& data) {
  return raft_->Step(Msg(raft_->id(), 0, kMsgProp, {ConsEnt(0, 0, data)}));
}

// ProposeConfChange proposes a configuration change. Like any proposal, the
// configuration change may be dropped with or without an error being returned.
// In particular, configuration changes are dropped unless the leader has
// certainty that there is no prior unapplied configuration change in its log.
ErrNum RawNode::ProposeConfChange(const ConfChange& cc) {
  std::string data;
  if (!cc.SerializeToString(&data)) {
    return kErrSerializeFail;
  }

  return raft_->Step(
      Msg(0, 0, kMsgProp, {ConsEnt(0, 0, data, kEntryConfChange)}));
}

// ApplyConfChange applies a config change to the local node.
ConfState RawNode::ApplyConfChange(const ConfChange& cc) {
  if (cc.node_id() == kNone) {
    return raft_->ConstructConfState();
  }

  switch (cc.type()) {
    case kConfChangeAddNode:
      raft_->AddNode(cc.node_id(), false);
      break;
    case kConfChangeAddLearnerNode:
      raft_->AddNode(cc.node_id(), true);
      break;
    case kConfChangeRemoveNode:
      raft_->RemoveNode(cc.node_id());
      break;
    case kConfChangeUpdateNode:
      raft_->AddNode(cc.node_id());
      break;
    default:
      break;
  }

  return raft_->ConstructConfState();
}

// Step advances the state machine using the given message.
ErrNum RawNode::Step(Message m) {
  // ignore unexpected local messages receiving over network
  if (IsLocalMsg(m.type())) {
    return kErrStepLocalMsg;
  }

  auto pr = raft_->tracker()->mutable_progress(m.from());
  if (pr != nullptr || !IsResponseMsg(m.type())) {
    return raft_->Step(m);
  }

  return kErrStepPeerNotFound;
}

// GetReady returns the outstanding work that the application needs to handle.
// This includes appending and applying entries or a snapshot, updating the
// raft::HardState, and sending messages. The returned GetReady() MUST be
// handled and subsequently passed back via Advance()
ReadyPtr RawNode::GetReady() {
  auto rd = ReadyWithoutAccept();
  AcceptReady(rd);
  return rd;
}

// HasReady called when RawNode user need to check if any Ready pending.
// Checking logic in this method should be consistent with
// Ready.ContainsUpdates()
bool RawNode::HasReady() {
  if (!IsSoftStateEqual(raft_->ConstructSoftState(), prev_ss_)) {
    return true;
  }
  auto hs = raft_->ConstructHardState();
  if (!IsEmptyHardState(hs) && !IsHardStateEqual(hs, prev_hs_)) {
    return true;
  }
  if (raft_->raft_log()->HasPendingSnapshot()) {
    return true;
  }
  if (!raft_->msgs().empty()) {
    return true;
  }
  std::vector<EntryPtr> unstable_ents;
  raft_->raft_log()->UnstableEntries(&unstable_ents);
  if (!unstable_ents.empty() || raft_->raft_log()->HasNextEnts()) {
    return true;
  }
  if (!raft_->read_states().empty()) {
    return true;
  }
  return false;
}

// Advance notifies the RawNode that the application has applied and saved
// progress in the last Ready results.
void RawNode::Advance(const ReadyPtr& rd) {
  if (!IsEmptyHardState(rd->hard_state())) {
    prev_hs_ = rd->hard_state();
  }
  raft_->Advance(rd);
}

// GetProgress return the Progress of this node and its peers, if this node is
// leader.
std::unordered_map<uint64_t, ProgressPtr> RawNode::GetProgress() {
  std::unordered_map<uint64_t, ProgressPtr> prs;
  if (raft_->state() == kStateLeader) {
    for (const auto& n : raft_->tracker()->progress_map()) {
      prs[n.first] = n.second;
    }
  }

  return prs;
}

// TransferLeader tries to transfer leadership to the given transferee.
void RawNode::TransferLeaderShip(uint64_t lead, uint64_t transferee) {
  JLOG_DEBUG << "[" << raft_->id() << ":" << raft_->term() << "]"
             << ": transfer leadership to " << transferee << " ing...";
  raft_->Step(Msg(transferee, lead, kMsgTransferLeader));
}

// ReadIndex requests a read state. The read state will be set in ready.
// Read state has a read index. Once the application advances further than
// the read index, any linearized read requests issued before the read
// request can be processed safely. The read state will have the same ctx
// attached.
void RawNode::ReadIndex(const std::string& rctx) {
  raft_->Step(Msg(0, 0, kMsgReadIndex, {ConsEnt(0, 0, rctx)}));
}

// ReadyWithoutAccept returns a Ready. This is a read-only operation.
ReadyPtr RawNode::ReadyWithoutAccept() {
  auto rd = std::make_shared<Ready>(raft_, prev_ss_, prev_hs_);
  return rd;
}

// AcceptReady is called when the consumer of the RawNode has decided to go
// ahead and handle a Ready. Nothing must alter the state of the RawNode
// between this call and the prior call to GetReady
void RawNode::AcceptReady(const ReadyPtr& rd) {
  if (!IsEmptySoftState(rd->soft_state())) {
    prev_ss_ = rd->soft_state();
  }
  if (!rd->read_states().empty()) {
    raft_->ClearReadStates();
  }
  raft_->ReadMessages();
}
}  // namespace jraft
