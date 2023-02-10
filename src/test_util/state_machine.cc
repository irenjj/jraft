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
#include "state_machine.h"

namespace jraft {

StateMachine::StateMachine(StateMachineType type, RaftPtr raft)
    : type_(type), raft_(std::move(raft)) {}

bool StateMachine::Step(const Message& m) {
  bool is_step = true;

  switch (type_) {
    case kBlackHoleType:
      break;
    case kRaftType:
      is_step = raft_->Step(m);
      break;
    default:
      JLOG_FATAL << "shouldn't come here";
      break;
  }

  return is_step;
}

void StateMachine::ReadMessages(std::vector<Message>* msgs) {
  switch (type_) {
    case kBlackHoleType:
      break;
    case kRaftType:
      raft_->ReadMessages(msgs);
      break;
    default:
      JLOG_FATAL << "shouldn't come here";
      break;
  }
}

RaftPtr StateMachine::raft() const {
  RaftPtr raft = nullptr;

  switch (type_) {
    case kBlackHoleType:
      break;
    case kRaftType:
      raft = raft_;
      break;
    default:
      JLOG_FATAL << "shouldn't come here";
      break;
  }

  return raft;
}

}  // namespace jraft
