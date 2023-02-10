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

#include "raft.h"
#include "soft_state.h"

namespace jraft {

// RawNode is a wrapper of raft.
class RawNode {
 public:
  explicit RawNode(Config c);
  ~RawNode() = default;

  void Tick();
  ErrNum Campaign();
  ErrNum Propose(const std::string& data);
  ErrNum ProposeConfChange(const ConfChange& cc);
  ConfState ApplyConfChange(const ConfChange& cc);
  ErrNum Step(Message m);
  ReadyPtr GetReady();
  bool HasReady();
  void Advance(const ReadyPtr& rd);
  std::unordered_map<uint64_t, ProgressPtr> GetProgress();
  void TransferLeaderShip(uint64_t lead, uint64_t transferee);
  void ReadIndex(const std::string& rctx);
  bool IsLeader() const { return raft_->state() == kStateLeader; }

  const Raft& raft() const { return *raft_; }
  RaftPtr mutable_raft() { return raft_; }

  const SoftState& prev_ss() const { return prev_ss_; }

  const HardState& prev_hs() const { return prev_hs_; }
  void set_prev_hs(const HardState& hs) { prev_hs_ = hs; }

 private:
  ReadyPtr ReadyWithoutAccept();
  void AcceptReady(const ReadyPtr& rd);

  RaftPtr raft_;
  SoftState prev_ss_;
  HardState prev_hs_;
};

}  // namespace jraft
