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

namespace jraft {

enum StateMachineType {
  kBlackHoleType = 0,
  kRaftType = 1,
};

class StateMachine {
 public:
  explicit StateMachine(StateMachineType type = kBlackHoleType,
                        RaftPtr raft = nullptr);
  ~StateMachine() = default;

  bool Step(const Message& m);
  void ReadMessages(std::vector<Message>* msgs);

  StateMachineType type() const { return type_; }
  void set_type(StateMachineType type) { type_ = type; }

  RaftPtr raft() const;

 private:
  StateMachineType type_;
  RaftPtr raft_;
};

typedef std::shared_ptr<StateMachine> StateMachinePtr;

}  // namespace jraft
