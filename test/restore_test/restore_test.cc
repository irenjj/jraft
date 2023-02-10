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
#include "test_util/network.h"
#include "test_util/test_util.h"
#include "ready.h"
#include "soft_state.h"

namespace jraft {

TEST(RestoreTest, TestRestore) {
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);
  auto s = NewSnap(11, 11, "", &cs);

  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  EXPECT_TRUE(sm->Restore(*s));
  EXPECT_EQ(sm->raft_log()->LastIndex(), s->metadata().index());
  uint64_t term = 0;
  ErrNum err = sm->raft_log()->Term(s->metadata().index(), &term);
  EXPECT_EQ(err, kOk);
  EXPECT_EQ(term, s->metadata().term());
  std::vector<uint64_t> voter_nodes;
  for (const auto& t : sm->tracker()->progress_map()) {
    if (!t.second->is_learner()) {
      voter_nodes.push_back(t.first);
    }
  }
  std::sort(voter_nodes.begin(), voter_nodes.end());
  EXPECT_EQ(voter_nodes.size(), cs.voters().size());
  for (size_t i = 0; i < voter_nodes.size(); i++) {
    EXPECT_EQ(voter_nodes[i], cs.voters()[i]);
  }

  EXPECT_FALSE(sm->Restore(*s));
  // is should not campaign before actually applying data.
  for (int i = 0; i < sm->randomized_election_timeout(); i++) {
    sm->Tick();
  }
  EXPECT_EQ(sm->state(), kStateFollower);
}

// TestRestoreWithLearner restores a snapshot which contains learners.
TEST(RestoreTest, TestRestoreWithLearner) {
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_learners(3);
  auto s = NewSnap(11, 11, "", &cs);

  auto storage = NewMSWithPL({1, 2}, {3});
  auto sm = NewTestRaft(3, 8, 2, storage);
  EXPECT_TRUE(sm->Restore(*s));
  EXPECT_EQ(sm->raft_log()->LastIndex(), s->metadata().index());
  uint64_t term = 0;
  ErrNum err = sm->raft_log()->Term(s->metadata().index(), &term);
  EXPECT_EQ(err, kOk);
  EXPECT_EQ(term, s->metadata().term());
  std::vector<uint64_t> voters;
  std::vector<uint64_t> learners;
  for (const auto& p : sm->tracker()->progress_map()) {
    if (p.second->is_learner()) {
      learners.push_back(p.first);
    } else {
      voters.push_back(p.first);
    }
  }
  EXPECT_EQ(learners.size(), cs.learners().size());
  EXPECT_EQ(voters.size(), cs.voters().size());
  for (size_t i = 0; i < learners.size(); i++) {
    EXPECT_EQ(learners[i], cs.learners()[i]);
  }

  for (size_t i = 0; i < voters.size(); i++) {
    EXPECT_EQ(voters[i], cs.voters()[i]);
  }

  EXPECT_FALSE(sm->Restore(*s));
}

// test outgoing voter can receive and apply snapshot correctly
TEST(RestoreTest, TestRestoreWithVotersOutgoing) {
  ConfState cs;
  cs.add_voters(2);
  cs.add_voters(3);
  cs.add_voters(4);
  cs.add_voters_outgoing(1);
  cs.add_voters_outgoing(2);
  cs.add_voters_outgoing(3);
  auto s = NewSnap(11, 11, "", &cs);

  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  EXPECT_TRUE(sm->Restore(*s));
  EXPECT_EQ(sm->raft_log()->LastIndex(), s->metadata().index());
  uint64_t term;
  ErrNum err = sm->raft_log()->Term(sm->raft_log()->LastIndex(), &term);
  EXPECT_EQ(err, kOk);
  EXPECT_EQ(term, s->metadata().term());

  std::vector<uint64_t> voters;
  for (const auto& v : sm->tracker()->progress_map()) {
    if (!v.second->is_learner()) {
      voters.push_back(v.first);
    }
  }
  EXPECT_EQ(voters.size(), 4);
  std::sort(voters.begin(), voters.end());
  for (size_t i = 0; i < 4; i++) {
    EXPECT_EQ(voters[i], static_cast<uint64_t>(i) + 1);
  }

  EXPECT_FALSE(sm->Restore(*s));

  // It should not campaign before actually applying data.
  for (int i = 0; i < sm->randomized_election_timeout(); i++) {
    sm->Tick();
  }
  EXPECT_EQ(sm->state(), kStateFollower);
}

// TestRestoreVoterToLearner verifies that a normal peer can be downgrade to a
// learner through a snapshot. At the time of writing, we don't allow
// configuration changes to do this directly, but note that the snapshot may
// compress multiple changes to the configuration into one: the voter could have
// been removed, then readded as a learner and the snapshot reflects both
// changes. In that case, a voter receives a snapshot telling it that it is now
// a learner. In fact, the node has to accept that snapshot, or it is
// permanently cut off from the Raft log.
TEST(RestoreTest, TestRestoreVoterToLearner) {
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_learners(3);
  auto s = NewSnap(11, 11, "", &cs);

  auto storage = NewMSWithPeers({1, 2, 3});
  auto sm = NewTestRaft(3, 10, 1, storage);

  EXPECT_FALSE(sm->is_learner());
  EXPECT_TRUE(sm->Restore(*s));
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower
// after restoring snapshot.
TEST(RestoreTest, TestRestoreLearnerPromotion) {
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);
  auto s = NewSnap(11, 11, "", &cs);

  auto storage = NewMSWithPL({1, 2}, {3});
  auto sm = NewTestRaft(3, 10, 1, storage);

  EXPECT_TRUE(sm->is_learner());
  EXPECT_TRUE(sm->Restore(*s));
  EXPECT_FALSE(sm->is_learner());
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snapshot from
// leader
TEST(RestoreTest, TestLearnerReceiveSnapshot) {
  // restore the state machine from a snapshot, so it has a compacted log and
  // a snapshot
  ConfState cs;
  cs.add_voters(1);
  cs.add_learners(2);
  auto s = NewSnap(11, 11, "", &cs);

  auto store1 = NewMSWithPL({1}, {2});
  auto store2 = NewMSWithPL({1}, {2});
  auto n1 = NewTestRaft(1, 10, 1, store1);
  auto n2 = NewTestRaft(2, 10, 1, store2);

  EXPECT_TRUE(n1->Restore(*s));
  SoftState empty_ss;
  HardState empty_hs;
  auto ready = std::make_shared<Ready>(n1, empty_ss, empty_hs);
  EXPECT_EQ(store1->ApplySnapshot(ready->mutable_snapshot()), kOk);
  n1->Advance(ready);

  // Force set n1 applied index.
  n1->raft_log()->AppliedTo(n1->raft_log()->committed());

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2)});

  n1->set_randomized_election_timeout(n1->election_timeout());
  for (int i = 0; i < n1->election_timeout(); i++) {
    n1->Tick();
  }

  nt.Send({ConsMsg(1, 1, kMsgBeat)});

  EXPECT_EQ(n2->raft_log()->committed(), n1->raft_log()->committed());
}

TEST(RestoreTest, TestRestoreIgnoreSnapshot) {
  std::vector<EntryPtr> prev_ents({NewEnt(1, 1), NewEnt(2, 1), NewEnt(3, 1)});
  uint64_t commit = 1;
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->raft_log()->Append(prev_ents);
  sm->raft_log()->CommitTo(commit);

  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  auto s = NewSnap(commit, 1, "", &cs);

  // ignore snapshot
  EXPECT_FALSE(sm->Restore(*s));
  EXPECT_EQ(sm->raft_log()->committed(), commit);

  // ignore snapshot and fast-forward commit
  s->mutable_metadata()->set_index(commit + 1);
  EXPECT_FALSE(sm->Restore(*s));
  EXPECT_EQ(sm->raft_log()->committed(), commit + 1);
}

TEST(RestoreTest, TestProvideSnap) {
  // restore the state machine from a snapshot so it has a compacted log and a
  // snapshot
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  auto s = NewSnap(11, 11, "", &cs);
  auto storage = NewMSWithPeers({1});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(*s);

  sm->BecomeCandidate();
  sm->BecomeLeader();

  // force set the next of node 2, so that node 2 needs a snapshot
  sm->tracker()->mutable_progress(2)->set_next(sm->raft_log()->FirstIndex());
  auto m = ConsMsg(2, 1, kMsgAppResp, {});
  m.set_index(sm->tracker()->mutable_progress(2)->next() - 1);
  m.set_reject(true);
  sm->Step(m);

  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  m = msgs[0];
  EXPECT_EQ(m.type(), kMsgSnap);
}

TEST(RestoreTest, TestIgnoreProvidingSnap) {
  // restore the state machine from a snapshot so that it has a compacted log
  // and a snapshot
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  auto s = NewSnap(11, 11, "", &cs);
  auto storage = NewMSWithPeers({1});
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->Restore(*s);

  sm->BecomeCandidate();
  sm->BecomeLeader();

  // force set the next of node 2, so that node 2 needs a snapshot change node
  // 2 to be inactive, expect node 1 ignore sending snapshot to 2
  sm->tracker()->mutable_progress(2)->set_next(sm->raft_log()->FirstIndex() -
                                               1);
  sm->tracker()->mutable_progress(2)->set_recent_active(false);

  sm->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(1, 1, "somedata")}));

  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 0);
}

TEST(RestoreTest, TestRestoreFromSnapMsg) {
  // TODO: what should this test?
  // ConfState cs;
  // cs.add_voters(1);
  // cs.add_voters(2);
  // auto s = NewSnap(11, 11, "", &cs);
  // auto m = ConsMsg(1, 0, kMsgSnap, {}, 2);
  // m.set_allocated_snapshot(s.get());

  // auto sm = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2}));
  // sm->Step(m);

  // EXPECT_EQ(sm->lead(), 1);
}

NetworkPtr NewNetwork(const std::vector<StateMachinePtr>& peers) {
  auto nt = std::make_shared<Network>(peers);

  return nt;
}

TEST(RestoreTest, TestSlowNodeRestore) {
  auto nt = NewNetwork({nullptr, nullptr, nullptr});
  nt->Send({ConsMsg(1, 1, kMsgHup)});

  nt->Isolate(3);
  for (int j = 0; j <= 100; j++) {
    nt->Send({ConsMsg(1, 1, kMsgProp, {ConsEnt()})});
  }
  auto lead = nt->peer(1)->raft();

  std::vector<EntryPtr> unstable;
  auto sm = lead->raft_log()->mutable_storage();
  lead->raft_log()->UnstableEntries(&unstable);
  sm->Append(unstable);
  lead->raft_log()->StableTo(lead->raft_log()->LastIndex(),
                             lead->raft_log()->LastTerm());
  lead->raft_log()->AppliedTo(lead->raft_log()->committed());

  ConfState cs;
  for (const auto& v : lead->tracker()->progress_map()) {
    if (!v.second->is_learner()) {
      cs.add_voters(v.first);
    }
  }
  sm->CreateSnapshot(lead->raft_log()->applied(), &cs, "", nullptr);
  sm->Compact(lead->raft_log()->applied());

  nt->Recover();
  // send heartbeats so that the leader can learn everyone is active. node 3
  // will only be considered as active when node 1 receives a reply from it.
  do {
    nt->Send({ConsMsg(1, 1, kMsgBeat)});
  } while (!lead->tracker()->mutable_progress(3)->recent_active());

  // trigger a snapshot
  nt->Send({ConsMsg(1, 1, kMsgProp, {ConsEnt()})});

  auto follower = nt->peer(3)->raft();

  // trigger a commit
  nt->Send({ConsMsg(1, 1, kMsgProp, {ConsEnt()})});
  EXPECT_EQ(follower->raft_log()->committed(), lead->raft_log()->committed());
}

}  // namespace jraft
