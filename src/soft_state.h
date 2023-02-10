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

#include <cstdint>

#include "raft.h"

namespace jraft {

// SoftState provides state that is useful for logging and debugging.
// The state_ is volatile and does not need to be persisted to WAL.
class SoftState {
 public:
  SoftState();
  SoftState(uint64_t leader, RaftStateType state);
  SoftState(const SoftState& from);
  ~SoftState() = default;

  inline SoftState& operator=(const SoftState& from) {
    if (&from == this) {
      return *this;
    }
    lead_ = from.lead();
    state_ = from.state();

    return *this;
  }

  inline bool operator==(const SoftState& from) const {
    return lead_ == from.lead() && state_ == from.state();
  }

  inline bool operator!=(const SoftState& from) const {
    return !(*this == from);
  }

  uint64_t lead() const { return lead_; }
  void set_lead(uint64_t lead) { lead_ = lead; }

  const RaftStateType& state() const { return state_; }
  void set_state(const RaftStateType& state) { state_ = state; }

 private:
  // must use atomic operations to access, keep 64-bit aligned
  uint64_t lead_;
  RaftStateType state_;
};

}  // namespace jraft
