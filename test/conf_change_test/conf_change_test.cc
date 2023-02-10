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
#include "ready.h"
#include "soft_state.h"

namespace jraft {

// TestStepConfig tests that when raft step kMsgProp in EntryConfChange
// type, it appends the entry to log and sets pending_conf to be true
TEST(RaftTest, TestStepConfig) {
  // a raft that cannot make progress
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();
  uint64_t index = r->raft_log()->LastIndex();
  Entry e;
  e.type();
  r->Step({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "", kEntryConfChange)})});
  EXPECT_EQ(r->raft_log()->LastIndex(), index + 1);
  EXPECT_EQ(r->pending_conf_index(), index + 1);
}

// TestStepIgnoreConfig tests that if raft step the second kMsgProp in
// kEntryConfChange type when the first one is uncommitted, the node will
// set the proposal to noop keep its original state.
TEST(RaftTest, TestStepIgnoreConfig) {
  // a raft that cannot make progress
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();
  r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "", kEntryConfChange)}));
  uint64_t index = r->raft_log()->LastIndex();
  uint64_t pending_conf_index = r->pending_conf_index();
  r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "", kEntryConfChange)}));
  std::vector<Entry> wents({ConsEnt(3, 1)});
  std::vector<EntryPtr> ents;
  EXPECT_EQ(r->raft_log()->Entries(index + 1, kNoLimit, &ents), kOk);
  EXPECT_TRUE(EntryDeepEqual(wents, ents));
  EXPECT_EQ(r->pending_conf_index(), pending_conf_index);
}

void MustAppendEntry(RaftPtr r, std::vector<Entry> ents) {
  if (!r->AppendEntry(ents)) {
    JLOG_FATAL << "entry expectedly dropped";
  }
}

// TestNewLeaderPendingConfig tests that new leader sets its pending_conf_index
// based on uncommitted entries.
TEST(RaftTest, TestNewLeaderPendingConfig) {
  struct TestArgs {
    bool add_entry;

    uint64_t wpending_index;
  } tests[] = {
      {false, 0},
      {true, 1},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
    if (tt.add_entry) {
      MustAppendEntry(r, {ConsEnt()});
    }
    r->BecomeCandidate();
    r->BecomeLeader();
    EXPECT_EQ(r->pending_conf_index(), tt.wpending_index) << "i: " << i;

    i++;
  }
}

bool VectorDeepEqual(const std::vector<uint64_t>& v1,
                     const std::vector<uint64_t>& v2) {
  if (v1.size() != v2.size()) {
    return false;
  }

  for (size_t i = 0; i < v1.size(); i++) {
    if (v1[i] != v2[i]) {
      return false;
    }
  }

  return true;
}

// TestAddNode tests that AddNode could update nodes correctly.
TEST(RaftTest, TestAddNode) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
  r->AddNode(2);
  auto nodes = r->tracker()->VoterNodes();
  std::vector<uint64_t> wnodes({1, 2});
  EXPECT_TRUE(VectorDeepEqual(nodes, wnodes));
}

// TestAddLearner tests that AddLearner could update nodes correctly
TEST(RaftTest, TestAddLearner) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
  // Add new learner peer
  r->AddNode(2, true);
  EXPECT_FALSE(r->is_learner());
  auto nodes = r->tracker()->LearnerNodes();
  std::vector<uint64_t> wnodes({2});
  EXPECT_TRUE(VectorDeepEqual(nodes, wnodes));
  EXPECT_TRUE(r->tracker()->mutable_progress(2)->is_learner());

  // promote peer to voter
  r->AddNode(2);
  EXPECT_FALSE(r->tracker()->mutable_progress(2)->is_learner());

  // demote r
  r->AddNode(1, true);
  EXPECT_TRUE(r->tracker()->mutable_progress(1)->is_learner());
  EXPECT_TRUE(r->is_learner());

  // promote r again
  r->AddNode(1);
  EXPECT_FALSE(r->tracker()->mutable_progress(1)->is_learner());
  EXPECT_FALSE(r->is_learner());
}

// TestAddNodeCheckQuorum tests that AddNode does not trigger a leader election
// immediately when check quorum is set.
TEST(RaftTest, TestAddNodeCheckQuorum) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
  r->set_check_quorum(true);

  r->BecomeCandidate();
  r->BecomeLeader();

  r->set_randomized_election_timeout(r->election_timeout());
  for (int i = 0; i < r->election_timeout() - 1; i++) {
    r->Tick();
  }

  r->AddNode(2);

  // this tick will reach election timeout, which triggers a quorum check
  r->Tick();

  // Node 1 should still be the leader after a single tick.
  EXPECT_EQ(r->state(), kStateLeader);

  // After another election_timeout ticks without hearing from node 2, node 1
  // should step down.
  r->set_randomized_election_timeout(r->election_timeout());
  for (int i = 0; i < r->election_timeout(); i++) {
    r->Tick();
  }

  EXPECT_EQ(r->state(), kStateFollower);
}

// TestRemoveNode tests that RemoveNode could update nodes and removed list
// correctly
TEST(RaftTest, TestRemoveNode) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->RemoveNode(2);
  std::vector<uint64_t> w({1});
  auto g = r->tracker()->VoterNodes();
  EXPECT_TRUE(VectorDeepEqual(w, g));

  // Removing the remaining voter will panic
  // r->RemoveNode(1);
}

// TestRemoveLearner tests that RemoveNode could update nodes and removed list
// correctly
TEST(RaftTest, TestRemoveLearner) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPL({1}, {2}));
  r->RemoveNode(2);
  std::vector<uint64_t> w({1});
  auto g = r->tracker()->VoterNodes();
  EXPECT_TRUE(VectorDeepEqual(w, g));

  w.clear();
  g = r->tracker()->LearnerNodes();
  EXPECT_TRUE(VectorDeepEqual(w, g));

  // Removing the remaining voter will panic
  // r->RemoveNode(1);
}

TEST(RaftTest, TestPromotable) {
  uint64_t id = 1;
  struct TestArgs {
    std::vector<uint64_t> peers;

    bool wp;
  } tests[] = {
      {{1}, true},
      {{1, 2, 3}, true},
      {{}, false},
      {{2, 3}, false},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(id, 5, 1, NewMSWithPeers(tt.peers));
    EXPECT_EQ(r->Promotable(), tt.wp) << "i: " << i;

    i++;
  }
}

TEST(RaftTest, TestRaftNodes) {
  struct TestArgs {
    std::vector<uint64_t> ids;
    std::vector<uint64_t> wids;
  } tests[] = {
      {{1, 2, 3}, {1, 2, 3}},
      {{3, 2, 1}, {1, 2, 3}},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({tt.ids}));
    EXPECT_TRUE(VectorDeepEqual(r->tracker()->VoterNodes(), tt.wids))
        << "i: " << i;
    i++;
  }
}

// NextEnts returns the appliable entries and updates the applied index
void NextEnts(const Raft& r, MemoryStorage& s, std::vector<EntryPtr>* ents) {
  // Transfer all unstable entries to "stable" storage.
  auto rl = r.raft_log();
  std::vector<EntryPtr> unstable_ents;
  rl->UnstableEntries(&unstable_ents);
  s.Append(unstable_ents);
  rl->StableTo(rl->LastIndex(), rl->LastTerm());

  rl->NextEnts(ents);
  rl->AppliedTo(rl->committed());
}

void NextEnts(RaftPtr r, StoragePtr s, std::vector<EntryPtr>* ents) {
  // Transfer all unstable entries to "stable" storage.
  auto rl = r->raft_log();
  std::vector<EntryPtr> unstable_ents;
  rl->UnstableEntries(&unstable_ents);
  s->Append(unstable_ents);
  rl->StableTo(rl->LastIndex(), rl->LastTerm());

  rl->NextEnts(ents);
  rl->AppliedTo(rl->committed());
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
TEST(RaftTest, TestCommitAfterRemoveNode) {
  // Create a cluster with two nodes.
  auto s = NewMSWithPeers({1, 2});
  auto r = NewTestRaft(1, 5, 1, s);
  r->BecomeCandidate();
  r->BecomeLeader();

  // Begin to remove the second node.
  ConfChange cc;
  cc.set_type(kConfChangeRemoveNode);
  cc.set_node_id(2);
  std::string ccdata;
  cc.SerializeToString(&ccdata);
  auto m = ConsMsg(0, 0, kMsgProp);
  auto ent = m.add_entries();
  ent->set_type(kEntryConfChange);
  ent->set_data(ccdata);
  r->Step(m);

  // Stabilize the log and make sure noting is committed yet
  std::vector<EntryPtr> ents;
  NextEnts(*r, *s, &ents);
  EXPECT_TRUE(ents.empty());
  auto ccindex = r->raft_log()->LastIndex();

  // While the config change is pending, make another proposal.
  auto m2 = ConsMsg(0, 0, kMsgProp);
  auto ent2 = m2.add_entries();
  ent2->set_type(kEntryNormal);
  ent2->set_data("hello");
  r->Step(m2);

  // Node 2 acknowledges the config change, committing it.
  r->Step(ConsMsg(2, 0, kMsgAppResp, {}, 0, 0, ccindex));
  NextEnts(*r, *s, &ents);
  EXPECT_EQ(ents.size(), 2);
  EXPECT_EQ(ents[0]->type(), kEntryNormal);
  EXPECT_EQ(ents[0]->data(), "");
  EXPECT_EQ(ents[1]->type(), kEntryConfChange);

  // Apply the config change. This reduces quorum requirements so the pending
  // command can now commit.
  r->RemoveNode(2);
  ents.clear();
  NextEnts(*r, *s, &ents);
  EXPECT_EQ(ents.size(), 1);
  EXPECT_EQ(ents[0]->type(), kEntryNormal);
  EXPECT_EQ(ents[0]->data(), "hello");
}

}  // namespace jraft
