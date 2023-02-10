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
#include "soft_state.h"

namespace jraft {

SoftState::SoftState() : lead_(kNone), state_(kStateFollower) {}

SoftState::SoftState(uint64_t leader, RaftStateType state)
    : lead_(leader), state_(state) {}

SoftState::SoftState(const SoftState& from) {
  lead_ = from.lead();
  state_ = from.state();
}

}  // namespace jraft
