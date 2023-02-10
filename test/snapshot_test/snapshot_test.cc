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
#include <gtest/gtest.h>

#include "common/util.h"
#include "test_util/test_util.h"

namespace jraft {

Snapshot TestSnap() {
  Snapshot s;
  auto meta = s.mutable_metadata();
  meta->set_index(11);
  meta->set_term(11);
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  *meta->mutable_conf_state() = cs;

  return s;
}

TEST(SnapshotTest, TestSendingSnapshotSetPendingSnapshot) {
  auto storage = NewMSWithPeers({1});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(TestSnap());

  sm->BecomeCandidate();
  sm->BecomeLeader();

  // force set the next of node 2, so that node 2 needs a snapshot
  sm->tracker()->mutable_progress(2)->set_next(sm->raft_log()->FirstIndex());

  Message m = ConsMsg(2, 1, kMsgAppResp);
  m.set_index(sm->tracker()->mutable_progress(2)->next() - 1);
  m.set_reject(true);
  sm->Step(m);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->pending_snapshot(), 11);
}

TEST(SnapshotTest, TestPendingSnapshotPauseReplication) {
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(TestSnap());

  sm->BecomeCandidate();
  sm->BecomeLeader();

  sm->tracker()->mutable_progress(2)->BecomeSnapshot(11);

  sm->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));
  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_TRUE(msgs.empty());
}

TEST(SnapshotTest, TestSnapshotFailure) {
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(TestSnap());

  sm->BecomeCandidate();
  sm->BecomeLeader();

  sm->tracker()->mutable_progress(2)->set_next(1);
  sm->tracker()->mutable_progress(2)->BecomeSnapshot(11);

  auto m = ConsMsg(2, 1, kMsgSnapStatus);
  m.set_reject(true);
  sm->Step(m);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->pending_snapshot(), 0);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->next(), 1);
  EXPECT_TRUE(sm->tracker()->mutable_progress(2)->msg_app_flow_paused());
}

TEST(SnapshotTest, TestSnapshotSucceed) {
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(TestSnap());

  sm->BecomeCandidate();
  sm->BecomeLeader();

  sm->tracker()->mutable_progress(2)->set_next(1);
  sm->tracker()->mutable_progress(2)->BecomeSnapshot(11);

  auto m = ConsMsg(2, 1, kMsgSnapStatus);
  sm->Step(m);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->pending_snapshot(), 0);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->next(), 12);
  EXPECT_TRUE(sm->tracker()->mutable_progress(2)->msg_app_flow_paused());
}

TEST(SnapshotTest, TestSnapshotAbort) {
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(TestSnap());

  sm->BecomeCandidate();
  sm->BecomeLeader();

  sm->tracker()->mutable_progress(2)->set_next(1);
  sm->tracker()->mutable_progress(2)->BecomeSnapshot(11);

  // A successful kMsgAppResp that has a higher/equal index than the
  // pending snapshot should abort the pending snapshot.
  sm->Step(ConsMsg(2, 1, kMsgAppResp, {}, 0, 0, 11));
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->pending_snapshot(), 0);

  // The follower entered kStateReplicate and the leader send an append and
  // optinistically update the mutable_progress (so we see 13 instead of 12).
  // There is something to append because the leader appended an empty entry
  // to the log at index 12 when it assumed leadership
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->next(), 13);
  EXPECT_EQ(sm->tracker()->mutable_progress(2)->mutable_inflights()->count(),
            1);
}

}  // namespace jraft
