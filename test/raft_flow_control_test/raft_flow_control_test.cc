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
#include <gtest/gtest.h>

#include "common/util.h"
#include "test_util/test_util.h"

namespace jraft {

// TestMsgAppFolowControlFull ensures:
// 1. kMsgApp can fill the sending window until full
// 2. when the window is full, no more kMsgApp can be sent.

TEST(RaftFlowControlTest, TestMsgAppFlowControlFull) {
  auto r = NewTestRaft(1, 5, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();

  auto pr2 = r->tracker()->mutable_progress(2);
  // force the mutable_progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (size_t i = 0; i < r->tracker()->max_inflight_size(); i++) {
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
  }

  // ensure 1
  EXPECT_TRUE(pr2->mutable_inflights()->Full());

  // ensure 2
  for (int i = 0; i < 10; i++) {
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
    std::vector<Message> msgs;
    EXPECT_TRUE(msgs.empty());
  }
}

// TestMsgAppFlowControlMoveForward ensures kMsgAppResp can move forward
// the sending window correctly:
// 1. valid kMsgAppResp.index() moves the window to pass all smaller or
// equal
//    index.
// 2. out-of-dated kMsgAppResp has no effect on the sliding window.
TEST(RaftFlowControlTest, TestMsgAppFlowControlMoveForward) {
  auto r = NewTestRaft(1, 5, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();

  auto pr2 = r->tracker()->mutable_progress(2);
  // force the mutable_progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (size_t i = 0; i < r->tracker()->max_inflight_size(); i++) {
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
  }

  // 1 is noop, 2 is the first proposal we just sent.
  // so we start with 2.
  for (size_t tt = 2; tt < r->tracker()->max_inflight_size(); tt++) {
    // move forward the window
    r->Step(ConsMsg(2, 1, kMsgAppResp, {}, 0, 0, static_cast<uint64_t>(tt)));
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);

    // fill in the inflights window again
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "tt: " << tt;

    // ensure 1
    EXPECT_TRUE(pr2->mutable_inflights()->Full());

    // ensure 2
    for (size_t i = 0; i < tt; i++) {
      r->Step(ConsMsg(2, 1, kMsgAppResp, {}, 0, 0, static_cast<uint64_t>(i)));
      EXPECT_TRUE(pr2->mutable_inflights()->Full())
          << "tt: " << tt << " i: " << i;
    }
  }
}

// TestMsgAppFlowControlRecvHeartbeat ensures a heartbeat response frees one
// slot if the window is full
TEST(RaftFlowControlTest, TestMsgAppFlowControlRecvHeartbeat) {
  auto r = NewTestRaft(1, 5, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();

  auto pr2 = r->tracker()->mutable_progress(2);
  // force the mutable_progress to be in replicate state
  pr2->BecomeReplicate();
  // fill in the inflights window
  for (size_t i = 0; i < r->tracker()->max_inflight_size(); i++) {
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
  }

  for (uint64_t tt = 1; tt < 5; tt++) {
    EXPECT_TRUE(pr2->mutable_inflights()->Full()) << "tt: " << tt;

    // recv tt kMsgHeartbeatResp and expect one free slot
    for (uint64_t i = 0; i < tt; i++) {
      EXPECT_TRUE(pr2->IsPaused()) << "tt: " << tt << " i: " << i;
      r->Step(ConsMsg(2, 1, kMsgHeartbeatResp));
      std::vector<Message> msgs;
      r->ReadMessages(&msgs);
      EXPECT_EQ(msgs.size(), 1) << "tt: " << tt << " i: " << i;
      EXPECT_TRUE(msgs[0].type() == kMsgApp) << "tt: " << tt << " i: " << i;
      EXPECT_TRUE(msgs[0].entries().empty()) << "tt: " << tt << " i: " << i;
    }

    // and just one slot
    for (uint64_t i = 0; i < 10; i++) {
      EXPECT_TRUE(pr2->IsPaused()) << "tt: " << tt << " i: " << i;
      r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
      std::vector<Message> msgs2;
      r->ReadMessages(&msgs2);
      EXPECT_TRUE(msgs2.empty());
    }

    // clear all pending messages
    r->Step(ConsMsg(2, 1, kMsgHeartbeatResp));
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
  }
}

}  // namespace jraft
