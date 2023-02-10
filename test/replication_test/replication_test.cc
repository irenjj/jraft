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

namespace jraft {

// TestLeaderStartReplication tests that when receiving client proposals, the
// leader appends the proposal to its log as a new entry, then issues
// AppendEntry RPCs in parallel to each of the other servers to replicate the
// entry. Also, when sending an AppendEntry RPC, the leader includes the index
// and term of the entry in its log that immediately precedes the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
TEST(ReplicationTest, TestLeaderStartReplication) {
  auto s = NewMSWithPeers({1, 2, 3});
  auto r = NewTestRaft(1, 10, 1, s);
  r->BecomeCandidate();
  r->BecomeLeader();
  CommitNoopEntry(r, s);
  auto li = r->raft_log()->LastIndex();

  std::vector<Entry> ents({ConsEnt(0, 0, "some data")});
  r->Step(ConsMsg(1, 1, kMsgProp, ents));

  EXPECT_EQ(r->raft_log()->LastIndex(), li + 1);
  EXPECT_EQ(r->raft_log()->committed(), li);

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  std::sort(msgs.begin(), msgs.end(), [](const Message &m1, const Message &m2) {
    return m1.to() < m2.to();
  });
  std::vector<EntryPtr> wents({NewEnt(li + 1, 1, "some data")});
  std::vector<Entry> wents2({ConsEnt(li + 1, 1, "some data")});
  std::vector<Message> wmsgs({ConsMsg(1, 2, kMsgApp, wents2, 1, 1, li, li),
                              ConsMsg(1, 3, kMsgApp, wents2, 1, 1, li, li)});
  EXPECT_TRUE(MsgDeepEqual(wmsgs, msgs));
  std::vector<EntryPtr> g;
  r->raft_log()->UnstableEntries(&g);
  EXPECT_TRUE(EntryDeepEqual(g, wents));
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntry RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
TEST(ReplicationTest, TestLeaderCommitEntry) {
  auto s = NewMSWithPeers({1, 2, 3});
  auto r = NewTestRaft(1, 10, 1, s);
  r->BecomeCandidate();
  r->BecomeLeader();
  CommitNoopEntry(r, s);
  auto li = r->raft_log()->LastIndex();
  r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")}));

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  for (const auto& m : msgs) {
    r->Step(AcceptAndReply(m));
  }

  EXPECT_EQ(r->raft_log()->committed(), li + 1);
  std::vector<EntryPtr> wents({NewEnt(li + 1, 1, "some data")});
  std::vector<EntryPtr> next_ents;
  r->raft_log()->NextEnts(&next_ents);
  EXPECT_TRUE(EntryDeepEqual(next_ents, wents));

  r->ReadMessages(&msgs);
  std::sort(msgs.begin(), msgs.end(), [](const Message& m1, const Message& m2) {
    return m1.to() < m2.to();
  });
  int i = 0;
  for (const auto& m : msgs) {
    EXPECT_EQ(m.to(), i + 2) << "i: " << i;
    EXPECT_EQ(m.type(), kMsgApp) << "i: " << i;
    EXPECT_EQ(m.commit(), li + 1) << "i: " << i;

    i++;
  }
}

std::unordered_map<uint64_t, bool> Map(const std::vector<uint64_t>& ids = {},
                                       const std::vector<bool>& vts = {}) {
  std::unordered_map<uint64_t, bool> res;
  for (size_t i = 0; i < ids.size(); i++) {
    res[ids[i]] = vts[i];
  }

  return res;
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: 5.3
TEST(ReplicationTest, TestLeaderAcknowledgeCommit) {
  struct TestArgs {
    int size;
    std::unordered_map<uint64_t, bool> acceptors;

    bool wack;
  } tests[] = {
      {1, {}, true},
      {3, {}, false},
      {3, Map({2}, {true}), true},
      {3, Map({2, 3}, {true, true}), true},
      {5, {}, false},
      {5, Map({2}, {true}), false},
      {5, Map({2, 3}, {true, true}), true},
      {5, Map({2, 3, 4}, {true, true, true}), true},
      {5, Map({2, 3, 4, 5}, {true, true, true, true}), true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto s = NewMSWithPeers(IdsBySize(tt.size));
    auto r = NewTestRaft(1, 10, 1, s);
    r->BecomeCandidate();
    r->BecomeLeader();
    CommitNoopEntry(r, s);
    auto li = r->raft_log()->LastIndex();
    r->Step(
        ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data", kEntryNormal)}));

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    for (const auto& m : msgs) {
      auto iter = tt.acceptors.find(m.to());
      if (iter != tt.acceptors.end() && iter->second) {
        r->Step(AcceptAndReply(m));
      }
    }

    auto g = r->raft_log()->committed();
    if (g > li) {
      EXPECT_TRUE(tt.wack) << "i: " << i;
    }

    i++;
  }
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader's log, including entries
// created by previous leaders.
// Also, it applies the entry to its state machine (in log order).
// Reference: section 5.3
TEST(ReplicationTest, TestLeaderCommitPrecedingEntries) {
  struct TestArgs {
    std::vector<EntryPtr> ents;
  } tests[] = {
      {},
      {{NewEnt(1, 2)}},
      {{NewEnt(1, 1), NewEnt(2, 2)}},
      {{NewEnt(1, 1)}},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2, 3});
    storage->Append(tt.ents);
    auto r = NewTestRaft(1, 10, 1, storage);
    HardState hs;
    hs.set_term(2);
    r->LoadState(hs);
    r->BecomeCandidate();
    r->BecomeLeader();
    r->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")}));

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    for (const auto& m : msgs) {
      r->Step(AcceptAndReply(m));
    }

    auto li = tt.ents.size();
    std::vector<EntryPtr> wents = tt.ents;
    wents.emplace_back(NewEnt(li + 1, 3));
    wents.emplace_back(NewEnt(li + 2, 3, "some data"));
    std::vector<EntryPtr> g;
    r->raft_log()->NextEnts(&g);
    EXPECT_TRUE(EntryDeepEqual(g, wents)) << "i: " << i;

    i++;
  }
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log orner).
// Reference: section 5.3
TEST(ReplicationTest, TestFollowerCommitEntry) {
  struct TestArgs {
    std::vector<Entry> ents;
    uint64_t commit;
  } tests[] = {
      {{ConsEnt(1, 1, "some data")}, 1},
      {{ConsEnt(1, 1, "some data"), ConsEnt(2, 1, "some data2")}, 2},
      {{ConsEnt(1, 1, "some data2"), ConsEnt(2, 1, "some data")}, 2},
      {{ConsEnt(1, 1, "some data"), ConsEnt(2, 1, "some data2")}, 1},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
    r->BecomeFollower(1, 2);

    r->Step(ConsMsg(2, 1, kMsgApp, {tt.ents}, 1, 0, 0, tt.commit));

    EXPECT_EQ(r->raft_log()->committed(), tt.commit) << "i: " << i;
    std::vector<Entry> wents;
    for (uint64_t j = 0; j < tt.commit; j++) {
      wents.push_back(tt.ents[j]);
    }
    std::vector<EntryPtr> ents;
    r->raft_log()->NextEnts(&ents);
    EXPECT_TRUE(EntryDeepEqual(ents, wents)) << "i: " << i;

    i++;
  }
}

// TestFollowerCheckMsgApp tests that if the follower does not find an entry
// in its log with the same index and term as the one in AppendEntry RPC,
// then it refuses the new entries. Otherwise, it replies that it accepts to
// append entries.
// Reference: section 5.3
TEST(ReplicationTest, TestFollowerCheckMsgApp) {
  std::vector<EntryPtr> ents({NewEnt(1, 1), NewEnt(2, 2)});
  struct TestArgs {
    uint64_t term;
    uint64_t index;
    uint64_t windex;
    bool wreject;
    uint64_t wreject_hint;
    uint64_t wlog_term;
  } tests[] = {
      // match with committed entries
      {0, 0, 1, false, 0, 0},
      {ents[0]->term(), ents[0]->index(), 1, false, 0, 0},
      // match with uncommitted entries
      {ents[1]->term(), ents[1]->index(), 2, false, 0, 0},

      // unmatch with existing entry
      {ents[0]->term(), ents[1]->index(), ents[1]->index(), true, 1, 1},
      // unexisting entry
      {ents[1]->term() + 1, ents[1]->index() + 1, ents[1]->index() + 1, true, 2,
       2},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2, 3});
    storage->Append(ents);
    auto r = NewTestRaft(1, 10, 1, storage);
    HardState hs;
    hs.set_commit(1);
    r->LoadState(hs);
    r->BecomeFollower(2, 2);

    r->Step(ConsMsg(2, 1, kMsgApp, {}, 2, tt.term, tt.index));

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    Message m = ConsMsg(1, 2, kMsgAppResp, {}, 2, tt.wlog_term, tt.windex);
    m.set_reject(tt.wreject);
    m.set_reject_hint(tt.wreject_hint);
    std::vector<Message> wmsgs({m});
    EXPECT_TRUE(MsgDeepEqual(msgs, wmsgs)) << "i: " << i;

    i++;
  }
}

Entry Ent(uint64_t term = 0, uint64_t index = 0) {
  Entry e;
  e.set_term(term);
  e.set_index(index);

  return e;
}

EntryPtr NEnt(uint64_t term = 0, uint64_t index = 0) {
  auto e = std::make_shared<Entry>();
  e->set_term(term);
  e->set_index(index);

  return e;
}

// TestFollowerAppendEntries tests that when AppendEntry RPCs in valid, the
// follower will delete the existing conflict entry and all that follow it.
// and append any new entries not already in the log. Also, it writes the
// new entry into stable storage.
// Reference: section 5.3
TEST(ReplicationTest, TestFollowerAppendEntries) {
  struct TestArgs {
    uint64_t index;
    uint64_t term;
    std::vector<Entry> ents;

    std::vector<EntryPtr> wents;
    std::vector<EntryPtr> wunstable;
  } tests[] = {
      {2, 2, {Ent(3, 3)}, {NEnt(1, 1), NEnt(2, 2), NEnt(3, 3)}, {NEnt(3, 3)}},
      {
          1,
          1,
          {Ent(3, 2), Ent(4, 3)},
          {NEnt(1, 1), NEnt(3, 2), NEnt(4, 3)},
          {NEnt(3, 2), NEnt(4, 3)},
      },
      {0, 0, {Ent(1, 1)}, {NEnt(1, 1), NEnt(2, 2)}, {}},
      {
          0,
          0,
          {Ent(3, 1)},
          {NEnt(3, 1)},
          {NEnt(3, 1)},
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2, 3});
    storage->Append({NEnt(1, 1), NEnt(2, 2)});
    auto r = NewTestRaft(1, 10, 1, storage);
    r->BecomeFollower(2, 2);

    auto m = ConsMsg(2, 1, kMsgApp, tt.ents);
    m.set_term(2);
    m.set_log_term(tt.term);
    m.set_index(tt.index);
    r->Step(m);

    std::vector<EntryPtr> g;
    r->raft_log()->AllEntries(&g);
    EXPECT_TRUE(EntryDeepEqual(g, tt.wents)) << "i: " << i;
    g.clear();
    r->raft_log()->UnstableEntries(&g);
    EXPECT_TRUE(EntryDeepEqual(g, tt.wunstable)) << "i: " << i;

    i++;
  }
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own
// Reference: section 5.3, figure 7
TEST(ReplicationTest, TestLeaderSyncFollowerLog) {
  std::vector<EntryPtr> ents({NEnt(1, 1), NEnt(1, 2), NEnt(1, 3), NEnt(4, 4),
                              NEnt(4, 5), NEnt(5, 6), NEnt(5, 7), NEnt(6, 8),
                              NEnt(6, 9), NEnt(6, 10)});
  uint64_t term = 8;
  struct TestArgs {
    std::vector<EntryPtr> ets;
  } tests[] = {
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
          NEnt(6, 10),
          NEnt(6, 11),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
          NEnt(6, 10),
          NEnt(7, 11),
          NEnt(7, 12),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(4, 6),
          NEnt(4, 7),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(2, 4),
          NEnt(2, 5),
          NEnt(2, 6),
          NEnt(3, 7),
          NEnt(3, 8),
          NEnt(3, 9),
          NEnt(3, 10),
          NEnt(3, 11),
      }},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto lead_storage = NewMSWithPeers({1, 2, 3});
    lead_storage->Append(ents);
    auto lead = NewTestRaft(1, 10, 1, lead_storage);
    HardState hs;
    hs.set_commit(lead->raft_log()->LastIndex());
    hs.set_term(term);
    lead->LoadState(hs);
    auto follower_storage = NewMSWithPeers({1, 2, 3});
    follower_storage->Append(tt.ets);
    auto follower = NewTestRaft(2, 10, 1, follower_storage);
    HardState hs2;
    hs2.set_term(term - 1);
    follower->LoadState(hs2);
    // It is necessary to have a three-node cluster.
    // The second may have more up-to-date log than the first one, so the first
    // node needs the vote from the third node to become the leader.
    Network n({NewSMWithRaft(lead), NewSMWithRaft(follower), NewBlackHole()});
    n.Send({ConsMsg(1, 1, kMsgHup)});
    // The election occurs in the term after the one we loaded with
    // lead.LoadState above.
    n.Send({ConsMsg(3, 1, kMsgVoteResp, {}, term + 1)});

    n.Send({ConsMsg(1, 1, kMsgProp, {Ent()})});

    EXPECT_TRUE(RaftLogDeepEqual(*lead->raft_log(), *follower->raft_log()))
        << "i: " << i;

    i++;
  }
}

// TestFollowerAppendEntries tests that when AppendEntry RPCs in valid, the
// follower will delete the existing conflict entry and all that follow it.
// and append any new entries not already in the log. Also, it writes the
// new entry into stable storage.
// Reference: section 5.3
TEST(ReplicationTest, TestFollowerAppendEntries1) {
  struct TestArgs {
    uint64_t index;
    uint64_t term;
    std::vector<Entry> ents;

    std::vector<EntryPtr> wents;
    std::vector<EntryPtr> wunstable;
  } tests[] = {
      {2, 2, {Ent(3, 3)}, {NEnt(1, 1), NEnt(2, 2), NEnt(3, 3)}, {NEnt(3, 3)}},
      {
          1,
          1,
          {Ent(3, 2), Ent(4, 3)},
          {NEnt(1, 1), NEnt(3, 2), NEnt(4, 3)},
          {NEnt(3, 2), NEnt(4, 3)},
      },
      {0, 0, {Ent(1, 1)}, {NEnt(1, 1), NEnt(2, 2)}, {}},
      {
          0,
          0,
          {Ent(3, 1)},
          {NEnt(3, 1)},
          {NEnt(3, 1)},
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2, 3});
    storage->Append({NEnt(1, 1), NEnt(2, 2)});
    auto r = NewTestRaft(1, 10, 1, storage);
    r->BecomeFollower(2, 2);

    auto m = ConsMsg(2, 1, kMsgApp, tt.ents);
    m.set_term(2);
    m.set_log_term(tt.term);
    m.set_index(tt.index);
    r->Step(m);

    std::vector<EntryPtr> g;
    r->raft_log()->AllEntries(&g);
    EXPECT_TRUE(EntryDeepEqual(g, tt.wents)) << "i: " << i;
    g.clear();
    r->raft_log()->UnstableEntries(&g);
    EXPECT_TRUE(EntryDeepEqual(g, tt.wunstable)) << "i: " << i;

    i++;
  }
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own
// Reference: section 5.3, figure 7
TEST(ReplicationTest, TestLeaderSyncFollowerLog2) {
  std::vector<EntryPtr> ents({NEnt(1, 1), NEnt(1, 2), NEnt(1, 3), NEnt(4, 4),
                              NEnt(4, 5), NEnt(5, 6), NEnt(5, 7), NEnt(6, 8),
                              NEnt(6, 9), NEnt(6, 10)});
  uint64_t term = 8;
  struct TestArgs {
    std::vector<EntryPtr> ets;
  } tests[] = {
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
          NEnt(6, 10),
          NEnt(6, 11),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(5, 6),
          NEnt(5, 7),
          NEnt(6, 8),
          NEnt(6, 9),
          NEnt(6, 10),
          NEnt(7, 11),
          NEnt(7, 12),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(4, 4),
          NEnt(4, 5),
          NEnt(4, 6),
          NEnt(4, 7),
      }},
      {{
          NEnt(),
          NEnt(1, 1),
          NEnt(1, 2),
          NEnt(1, 3),
          NEnt(2, 4),
          NEnt(2, 5),
          NEnt(2, 6),
          NEnt(3, 7),
          NEnt(3, 8),
          NEnt(3, 9),
          NEnt(3, 10),
          NEnt(3, 11),
      }},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto lead_storage = NewMSWithPeers({1, 2, 3});
    lead_storage->Append(ents);
    auto lead = NewTestRaft(1, 10, 1, lead_storage);
    HardState hs;
    hs.set_commit(lead->raft_log()->LastIndex());
    hs.set_term(term);
    lead->LoadState(hs);
    auto follower_storage = NewMSWithPeers({1, 2, 3});
    follower_storage->Append(tt.ets);
    auto follower = NewTestRaft(2, 10, 1, follower_storage);
    HardState hs2;
    hs2.set_term(term - 1);
    follower->LoadState(hs2);
    // It is necessary to have a three-node cluster.
    // The second may have more up-to-date log than the first one, so the first
    // node needs the vote from the third node to become the leader.
    Network n({NewSMWithRaft(lead), NewSMWithRaft(follower), NewBlackHole()});
    n.Send({ConsMsg(1, 1, kMsgHup)});
    // The election occurs in the term after the one we loaded with
    // lead.LoadState above.
    n.Send({ConsMsg(3, 1, kMsgVoteResp, {}, term + 1)});

    n.Send({ConsMsg(1, 1, kMsgProp, {Ent()})});

    EXPECT_TRUE(RaftLogDeepEqual(*lead->raft_log(), *follower->raft_log()))
        << "i: " << i;

    i++;
  }
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the
// leader's current term are committed by counting replicas.
// Reference: section 5.4.2
TEST(ReplicationTest, TestLeaderOnlyCommitsLogFromCurrentTerm) {
  std::vector<EntryPtr> ents({NEnt(1, 1), NEnt(2, 2)});
  struct TestArgs {
    uint64_t index;
    uint64_t wcommit;
  } tests[] = {
      // do not commit log entries in previous terms
      {1, 0},
      {2, 0},
      // commit log in current term
      {3, 3},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2});
    storage->Append(ents);
    auto r = NewTestRaft(1, 10, 1, storage);
    HardState hs;
    hs.set_term(2);
    r->LoadState(hs);
    // become leader at term 3
    r->BecomeCandidate();
    r->BecomeLeader();
    r->ReadMessages();

    // propose an entry to current term
    r->Step(ConsMsg(1, 1, kMsgProp, {Ent()}));

    r->Step(ConsMsg(2, 1, kMsgAppResp, {}, r->term(), 0, tt.index));
    EXPECT_EQ(r->raft_log()->committed(), tt.wcommit) << "i: " << i;

    i++;
  }
}

// NextEnts returns the appliable entries and updates the applied index
void GetNextEnts(RaftPtr r, MemoryStorage* s, std::vector<EntryPtr>* ents) {
  // Transfer all unstable entries to "stable" storage.
  std::vector<EntryPtr> unstable_ents;
  r->raft_log()->UnstableEntries(&unstable_ents);
  s->Append(unstable_ents);
  r->raft_log()->StableTo(r->raft_log()->LastIndex(),
                          r->raft_log()->LastTerm());

  r->raft_log()->NextEnts(ents);
  r->raft_log()->AppliedTo(r->raft_log()->committed());
}

TEST(ReplicationTest, TestLogReplication) {
  struct TestArgs {
    Network nt;
    std::vector<Message> msgs;

    uint64_t wcommitted;
  } tests[] = {
      {
          Network({nullptr, nullptr, nullptr}),
          {ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})},
          2,
      },
      {
          Network({nullptr, nullptr, nullptr}),
          {
              ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}),
              ConsMsg(1, 2, kMsgHup),
              ConsMsg(1, 2, kMsgProp, {ConsEnt(0, 0, "somedata")}),
          },
          4,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto nt = tt.nt;
    nt.Send({ConsMsg(1, 1, kMsgHup)});

    for (const auto& m : tt.msgs) {
      nt.Send({m});
    }

    int j = 0;
    for (const auto& x : nt.peers()) {
      auto r = x.second->raft();
      EXPECT_EQ(r->raft_log()->committed(), tt.wcommitted) << "j: " << j;

      std::vector<EntryPtr> ents;
      std::vector<EntryPtr> next_ents;
      auto ms =
          dynamic_cast<MemoryStorage*>(r->raft_log()->mutable_storage().get());
      GetNextEnts(r, ms, &next_ents);
      for (const auto& e : next_ents) {
        if (e->data() != "") {
          ents.push_back(e);
        }
      }

      std::vector<Message> props;
      for (const auto& m : tt.msgs) {
        if (m.type() == kMsgProp) {
          props.push_back(m);
        }
      }

      int k = 0;
      for (const auto& m : props) {
        EXPECT_EQ(ents[k]->data(), m.entries()[0].data()) << "k: " << k;
        k++;
      }
      j++;
    }

    i++;
  }
}

// TestLearnerLogReplication tests that a learner can receive entries from the
// leader.
TEST(ReplicationTest, TestLearnerLogReplication) {
  auto ms1 = NewMSWithPL({1}, {2});
  auto c1 = ConsConfig(1, 10, 1, ms1);
  RaftPtr n1 = std::make_shared<Raft>(&c1);
  StateMachinePtr sm1 = std::make_shared<StateMachine>(kRaftType, n1);

  auto ms2 = NewMSWithPL({1}, {2});
  auto c2 = ConsConfig(2, 10, 1, ms2);
  RaftPtr n2 = std::make_shared<Raft>(&c1);
  StateMachinePtr sm2 = std::make_shared<StateMachine>(kRaftType, n2);

  Network nt({sm1, sm2});

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);

  n1->set_randomized_election_timeout(n1->election_timeout());

  for (int i = 0; i < n1->election_timeout(); i++) {
    n1->Tick();
  }

  nt.Send({ConsMsg(1, 1, kMsgBeat)});

  // n1 is leader and n2 is learner
  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_TRUE(n2->is_learner());

  uint64_t next_committed = n1->raft_log()->committed() + 1;
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});
  EXPECT_EQ(n1->raft_log()->committed(), next_committed);
  EXPECT_EQ(n1->raft_log()->committed(), n2->raft_log()->committed());

  uint64_t match = n1->tracker()->mutable_progress(2)->match();
  EXPECT_EQ(match, n2->raft_log()->committed());
}

TEST(ReplicationTest, TestSingleNodeCommit) {
  Network tt({nullptr});
  tt.Send({ConsMsg(1, 1, kMsgHup)});
  tt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")})});
  tt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")})});

  auto r = tt.peer(1)->raft();
  EXPECT_EQ(r->raft_log()->committed(), 3);
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
TEST(ReplicationTest, TestCannotCommitWithoutNewTermEntry) {
  Network tt({nullptr, nullptr, nullptr, nullptr, nullptr});
  tt.Send({ConsMsg(1, 1, kMsgHup)});

  // 1 cannot reach 2, 3, 4
  tt.Cut(1, 3);
  tt.Cut(1, 4);
  tt.Cut(1, 5);

  tt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")})});
  tt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "some data")})});

  auto r = tt.peer(1)->raft();
  EXPECT_EQ(r->raft_log()->committed(), 1);

  // network recovery
  tt.Recover();
  // avoid committing ChangeType proposal
  tt.Ignore(kMsgApp);

  // elect 2 as the new leader with term 2
  tt.Send({ConsMsg(2, 2, kMsgHup)});

  // no log entries from previous term should be committed
  r = tt.peer(2)->raft();
  EXPECT_EQ(r->raft_log()->committed(), 1);

  tt.Recover();
  // send heartbeat, reset wati
  tt.Send({ConsMsg(2, 2, kMsgBeat)});
  // append an entry at current term
  tt.Send({ConsMsg(2, 2, kMsgProp, {ConsEnt(0, 0, "some data")})});
  // expect the committed to be advanced
  EXPECT_EQ(r->raft_log()->committed(), 5);
}

TEST(ReplicationTest, TestOldMessages) {
  Network tt({nullptr, nullptr, nullptr});
  // make 1 leader term 3
  tt.Send({ConsMsg(1, 1, kMsgHup)});
  tt.Send({ConsMsg(2, 2, kMsgHup)});
  tt.Send({ConsMsg(1, 1, kMsgHup)});
  // pretend we're an old leader trying to make progress; this entry is
  // expected to be ignored.
  tt.Send({ConsMsg(2, 1, kMsgApp, {ConsEnt(3, 2)}, 2)});
  // commit a new entry
  tt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});

  auto ms = NewMSWithEnts({NewEnt(), NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3),
                           NewEnt(4, 3, "somedata")});
  auto ilog = std::make_shared<RaftLog>(ms);
  ilog->mutable_unstable().set_offset(5);
  ilog->set_committed(4);

  for (const auto& p : tt.peers()) {
    auto sm = p.second->raft();
    EXPECT_TRUE(RaftLogDeepEqual(*sm->raft_log(), *ilog));
  }
}

NetworkPtr NewNetwork(const std::vector<StateMachinePtr>& peers) {
  auto nt = std::make_shared<Network>(peers);

  return nt;
}

TEST(ReplicationTest, TestProposal) {
  struct TestArgs {
    NetworkPtr nt;
    bool success;
  } tests[] = {
      {NewNetwork({nullptr, nullptr, nullptr}), true},
      {NewNetwork({nullptr, nullptr, NewBlackHole()}), true},
      {NewNetwork({nullptr, NewBlackHole(), NewBlackHole()}), false},
      {NewNetwork({nullptr, NewBlackHole(), NewBlackHole(), nullptr}), false},
      {NewNetwork({nullptr, NewBlackHole(), NewBlackHole(), nullptr, nullptr}),
       true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    std::string data("somedata");

    // promote 1 to become leader
    tt.nt->Send({ConsMsg(1, 1, kMsgHup)});
    tt.nt->Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, data)})});

    std::shared_ptr<RaftLog> want_log;
    if (tt.success) {
      auto ms = NewMSWithEnts({NewEnt(1, 1), NewEnt(2, 1, data)});
      want_log = std::make_shared<RaftLog>(ms);
      want_log->mutable_unstable().set_offset(3);
      want_log->set_committed(2);
    } else {
      want_log = std::make_shared<RaftLog>(NewMSWithEnts({}));
    }

    int j = 0;
    for (const auto& p : tt.nt->peers()) {
      auto sm = p.second->raft();
      if (sm == nullptr) {
        continue;
      }
      EXPECT_TRUE(RaftLogDeepEqual(*sm->raft_log(), *want_log)) << "j: " << j;

      j++;
    }

    auto sm = tt.nt->peer(1)->raft();
    EXPECT_EQ(sm->term(), 1) << "i: " << i;

    i++;
  }
}

TEST(ReplicationTest, TestProposalByProxy) {
  std::string data("somedata");
  struct TestArgs {
    NetworkPtr nt;
  } tests[] = {
      {NewNetwork({nullptr, nullptr, nullptr})},
      {NewNetwork({nullptr, nullptr, NewBlackHole()})},
  };

  int i = 0;
  for (const auto& tt : tests) {
    // promote 1 the leader
    tt.nt->Send({ConsMsg(1, 1, kMsgHup)});

    // propose via follower
    tt.nt->Send({ConsMsg(2, 2, kMsgProp, {ConsEnt(0, 0, data)})});

    auto ms = NewMSWithEnts({NewEnt(1, 1), NewEnt(2, 1, data)});
    auto want_log = std::make_shared<RaftLog>(ms);
    want_log->mutable_unstable().set_offset(3);
    want_log->set_committed(2);

    int j = 0;
    for (const auto& p : tt.nt->peers()) {
      auto sm = p.second->raft();
      if (sm == nullptr) {
        continue;
      }

      EXPECT_TRUE(RaftLogDeepEqual(*sm->raft_log(), *want_log)) << "j: " << j;
    }

    auto sm = tt.nt->peer(1)->raft();
    EXPECT_EQ(sm->term(), 1) << "i: " << i;

    i++;
  }
}

TEST(ReplicationTest, TestCommit) {
  struct TestArgs {
    std::vector<uint64_t> matches;
    std::vector<EntryPtr> logs;
    uint64_t sm_term;

    uint64_t w;
  } tests[] = {
      // single
      {{1}, {NewEnt(1, 1)}, 1, 1},
      {{1}, {NewEnt(1, 1)}, 2, 0},
      {{2}, {NewEnt(1, 1), NewEnt(2, 2)}, 2, 2},
      {{1}, {NewEnt(1, 2)}, 2, 1},

      // old
      {{2, 1, 1}, {NewEnt(1, 1), NewEnt(2, 2)}, 1, 1},
      {{2, 1, 1}, {NewEnt(1, 1), NewEnt(2, 1)}, 2, 0},
      {{2, 1, 2}, {NewEnt(1, 1), NewEnt(2, 2)}, 2, 2},
      {{2, 1, 2}, {NewEnt(1, 1), NewEnt(2, 1)}, 2, 0},

      // even
      {{2, 1, 1, 1}, {NewEnt(1, 1), NewEnt(2, 2)}, 1, 1},
      {{2, 1, 1, 1}, {NewEnt(1, 1), NewEnt(2, 1)}, 2, 0},
      {{2, 1, 1, 2}, {NewEnt(1, 1), NewEnt(2, 2)}, 1, 1},
      {{2, 1, 1, 2}, {NewEnt(1, 1), NewEnt(2, 1)}, 2, 0},
      {{2, 1, 2, 2}, {NewEnt(1, 1), NewEnt(2, 2)}, 2, 2},
      {{2, 1, 2, 2}, {NewEnt(1, 1), NewEnt(2, 1)}, 2, 0},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1});
    storage->Append(tt.logs);
    HardState hs;
    hs.set_term(tt.sm_term);
    storage->set_hard_state(hs);

    auto sm = NewTestRaft(1, 10, 2, storage);
    for (size_t j = 0; j < tt.matches.size(); j++) {
      uint64_t id = static_cast<uint64_t>(j) + 1;
      if (id > 1) {
        sm->AddNode(id);
      }
      auto pr = sm->tracker()->mutable_progress(id);
      pr->set_match(tt.matches[j]);
      pr->set_next(tt.matches[j] + 1);
    }

    sm->MaybeCommit();
    EXPECT_EQ(sm->raft_log()->committed(), tt.w) << "i: " << i;

    i++;
  }
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn't contain an entry at prev_log_index whose term
//    matches prev_log_term.
// 2. If an existing entry conflicts with a new one (same index but different
//    terms), delete the existing entry and all that follow it; append any
//    new entries not already in the log.
// 3. If leader_commit > commit_index,
//    set commit_index = min(leader_commit, index of last new entry).
TEST(ReplicationTest, TestHandleMsgApp) {
  struct TestArgs {
    Message m;

    uint64_t windex;
    uint64_t wcommit;
    bool wreject;
  } tests[] = {
      // Ensure 1
      // previous log mismatch
      {ConsMsg(0, 0, kMsgApp, {}, 2, 3, 2, 3), 2, 0, true},
      // previous log not-exist
      {ConsMsg(0, 0, kMsgApp, {}, 2, 3, 3, 3), 2, 0, true},

      // Ensure 2
      {ConsMsg(0, 0, kMsgApp, {}, 2, 1, 1, 1), 2, 1, false},
      {ConsMsg(0, 0, kMsgApp, {ConsEnt(1, 2)}, 2, 0, 0, 1), 1, 1, false},
      {ConsMsg(0, 0, kMsgApp, {ConsEnt(3, 2), ConsEnt(4, 2)}, 2, 2, 2, 3), 4, 3,
       false},
      {ConsMsg(0, 0, kMsgApp, {ConsEnt(3, 2)}, 2, 2, 2, 4), 3, 3, false},
      {ConsMsg(0, 0, kMsgApp, {ConsEnt(2, 2)}, 2, 1, 1, 4), 2, 2, false},

      // Ensure 3
      // match entry 1, commit up to last new entry 1
      {ConsMsg(0, 0, kMsgApp, {}, 1, 1, 1, 3), 2, 1, false},
      // match entry 1, commit up to last new entry 2
      {ConsMsg(0, 0, kMsgApp, {ConsEnt(2, 2)}, 1, 1, 1, 3), 2, 2, false},
      // match entry 2, commit up to last new entry 2
      {ConsMsg(0, 0, kMsgApp, {}, 2, 2, 2, 3), 2, 2, false},
      // commit up to log.Last()
      {ConsMsg(0, 0, kMsgApp, {}, 2, 2, 2, 4), 2, 2, false},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto ms = NewMSWithPeers({1});
    ms->Append({NewEnt(1, 1), NewEnt(2, 2)});
    auto sm = NewTestRaft(1, 10, 1, ms);
    sm->BecomeFollower(2, kNone);

    sm->HandleAppendEntries(tt.m);
    EXPECT_EQ(sm->raft_log()->LastIndex(), tt.windex) << "i: " << i;
    EXPECT_EQ(sm->raft_log()->committed(), tt.wcommit) << "i: " << i;
    std::vector<Message> m;
    sm->ReadMessages(&m);
    EXPECT_EQ(m.size(), 1) << "i: " << i;
    EXPECT_EQ(m[0].reject(), tt.wreject) << "i: " << i;

    i++;
  }
}

// TestHandleHeartbeat ensures that the follower commits to the commit in the
// message.
TEST(ReplicationTest, TestHandleHeartbeat) {
  uint64_t commit = 2;
  struct TestArgs {
    Message m;

    uint64_t wcommit;
  } tests[] = {
      {ConsMsg(2, 1, kMsgHeartbeat, {}, 2, 0, 0, commit + 1), commit + 1},
      // do not decrease commit
      {ConsMsg(2, 1, kMsgHeartbeat, {}, 2, 0, 0, commit - 1), commit},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2});
    storage->Append({NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)});
    auto sm = NewTestRaft(1, 5, 1, storage);
    sm->BecomeFollower(2, 2);
    sm->raft_log()->CommitTo(commit);
    sm->HandleHeartbeat(tt.m);
    EXPECT_EQ(sm->raft_log()->committed(), tt.wcommit) << "i: " << i;
    std::vector<Message> m;
    sm->ReadMessages(&m);
    EXPECT_EQ(m.size(), 1) << "i: " << i;
    EXPECT_EQ(m[0].type(), kMsgHeartbeatResp) << "i: " << i;

    i++;
  }
}

// TestHandleHeartbeatResp ensures that we re-send log entries when we get a
// heartbeat response.
TEST(ReplicationTest, TestHandleHeartbeatResp) {
  auto storage = NewMSWithPeers({1, 2});
  storage->Append({NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)});
  auto sm = NewTestRaft(1, 5, 1, storage);
  sm->BecomeCandidate();
  sm->BecomeLeader();
  sm->raft_log()->CommitTo(sm->raft_log()->LastIndex());

  // A heartbeat response from a node that is behind; re-send kMsgApp
  sm->Step(ConsMsg(2, 0, kMsgHeartbeatResp));
  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgApp);

  // A second heartbeat response generates another kMsgApp re-send
  sm->Step(ConsMsg(2, 0, kMsgHeartbeatResp));
  msgs.clear();
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgApp);

  // Once we have an kMsgAppResp, heartbeat no longer send kMsgApp.
  sm->Step(ConsMsg(2, 0, kMsgAppResp, {}, 0, 0,
                   msgs[0].index() + msgs[0].entries().size()));
  // Consume the message sent in response to kMsgAppResp
  std::vector<Message> tmp;
  sm->ReadMessages(&tmp);

  sm->Step(ConsMsg(2, 0, kMsgHeartbeatResp));
  msgs.clear();
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 0);
}

// TestRaftFreesReadOnlyMem ensures raft will free read request from read_only
// read_index_queue and pending_read_index map.
TEST(ReplicationTest, TestRaftFreesReadOnlyMem) {
  auto storage = NewMSWithPeers({1, 2});
  auto sm = NewTestRaft(1, 5, 1, storage);
  sm->BecomeCandidate();
  sm->BecomeLeader();
  sm->raft_log()->CommitTo(sm->raft_log()->LastIndex());

  std::string ctx("ctx");

  // leader starts linearizable read request.
  // more info: raft sissertation 6.4, step 2.
  sm->Step(ConsMsg(2, 0, kMsgReadIndex, {ConsEnt(0, 0, ctx)}));
  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgHeartbeat);
  EXPECT_EQ(msgs[0].context(), ctx);
  EXPECT_EQ(sm->read_only()->read_index_queue().size(), 1);
  EXPECT_EQ(sm->read_only()->pending_read_index().size(), 1);
  auto pending = sm->read_only()->pending_read_index();
  EXPECT_TRUE(pending.find(ctx) != pending.end());

  // heartbeat responses from majority of followers (1 in this case)
  // acknowledge the authority of the leader
  // more info: raft dissertation 6.4, step 3.
  sm->Step(ConsMsg(2, 0, kMsgHeartbeatResp, {}, 0, 0, 0, 0, ctx));
  EXPECT_EQ(sm->read_only()->read_index_queue().size(), 0);
  EXPECT_EQ(sm->read_only()->pending_read_index().size(), 0);
  pending = sm->read_only()->pending_read_index();
  EXPECT_TRUE(pending.find(ctx) == pending.end());
}

// TestMsgAppRespWaitReset verifies the resume behavior of a leader
// kMsgAppResp
TEST(ReplicationTest, TestMsgAppRespWaitReset) {
  auto storage = NewMSWithPeers({1, 2, 3});
  auto sm = NewTestRaft(1, 5, 1, storage);
  sm->BecomeCandidate();
  sm->BecomeLeader();

  // The new leader has just emitted a new term 4 entry; consume those messages
  // from the outgoing queue.
  sm->BcastAppend();
  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);

  // Node 2 acks the first entry, making it committed.
  sm->Step(ConsMsg(2, 0, kMsgAppResp, {}, 0, 0, 1));
  EXPECT_EQ(sm->raft_log()->committed(), 1);

  // Also consume the kMsgApp messages that update commit on the follower
  std::vector<Message> tmp;
  sm->ReadMessages(&tmp);

  // A new command is now proposed on node 1.
  sm->Step(ConsMsg(1, 0, kMsgProp, {ConsEnt()}));

  // the command is broadcast to all nodes not in the wait state.
  // Node 2 left the wait state due to its kMsgAppResp, but node 3 is
  // still waiting.
  msgs.clear();
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgApp);
  EXPECT_EQ(msgs[0].to(), 2);
  EXPECT_EQ(msgs[0].entries().size(), 1);
  EXPECT_EQ(msgs[0].entries()[0].index(), 2);

  // Now Node 3 acks the first entry. This releases the wait and entry 2 is
  // sent
  sm->Step(ConsMsg(3, 0, kMsgAppResp, {}, 0, 0, 1));
  msgs.clear();
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgApp);
  EXPECT_EQ(msgs[0].to(), 3);
  EXPECT_EQ(msgs[0].entries().size(), 1);
  EXPECT_EQ(msgs[0].entries()[0].index(), 2);
}

void TestRecvMsgVote(MessageType msg_type) {
  struct TestArgs {
    RaftStateType state;
    uint64_t index;
    uint64_t log_term;
    uint64_t vote_for;

    bool wreject;
  } tests[] = {
      {kStateFollower, 0, 0, kNone, true},
      {kStateFollower, 0, 1, kNone, true},
      {kStateFollower, 0, 2, kNone, true},
      {kStateFollower, 0, 3, kNone, false},

      {kStateFollower, 1, 0, kNone, true},
      {kStateFollower, 1, 1, kNone, true},
      {kStateFollower, 1, 2, kNone, true},
      {kStateFollower, 1, 3, kNone, false},

      {kStateFollower, 2, 0, kNone, true},
      {kStateFollower, 2, 1, kNone, true},
      {kStateFollower, 2, 2, kNone, false},
      {kStateFollower, 2, 3, kNone, false},

      {kStateFollower, 3, 0, kNone, true},
      {kStateFollower, 3, 1, kNone, true},
      {kStateFollower, 3, 2, kNone, false},
      {kStateFollower, 3, 3, kNone, false},

      {kStateFollower, 3, 2, 2, false},
      {kStateFollower, 3, 2, 1, true},

      {kStateLeader, 3, 3, 1, true},
      {kStatePreCandidate, 3, 3, 1, true},
      {kStateCandidate, 3, 3, 1, true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto sm = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
    sm->set_state(tt.state);
    sm->set_step_func();
    sm->set_vote(tt.vote_for);
    auto raft_log = new RaftLog(NewMSWithEnts({NewEnt(1, 2), NewEnt(2, 2)}));
    raft_log->mutable_unstable().set_offset(3);
    sm->set_raft_log(raft_log);

    // Raft.term() is greater than ro equal to Raft.raft_log()->LastTerm. In
    // this test we're only testing kMsgVote Responses when the
    // campaigning node has a different raft log compared to the recipient node.
    // Additionally we're verifying behavior when the recipient node has
    // already given out its vote for its current term. We're not testing what
    // the recipient node does when receiving a message with a different term
    // number, so we simply initialize both term numbers to be the same.
    uint64_t term = std::max(sm->raft_log()->LastTerm(), tt.log_term);
    sm->set_term(term);
    sm->Step(ConsMsg(2, 0, msg_type, {}, term, tt.log_term, tt.index));

    std::vector<Message> msgs;
    sm->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
    EXPECT_EQ(msgs[0].type(), VoteRespMsgType(msg_type)) << "i: " << i;
    EXPECT_EQ(msgs[0].reject(), tt.wreject) << "i: " << i;

    i++;
  }
}

TEST(ReplicationTest, TestMsgAppRespWaitResetWithoutPrevote) {
  TestRecvMsgVote(kMsgVote);
}

TEST(ReplicationTest, TestMsgAppRespWaitResetWithPrevote) {
  TestRecvMsgVote(kMsgPreVote);
}

// TODO: FIXME
// TEST(ReplicationTest, TestStateTransaction) {
//   struct TestArgs {
//     RaftStateType from;
//     RaftStateType to;

//     bool wallow;
//     uint64_t wterm;
//     uint64_t wlead;
//   } tests[] = {
//     {kStateFollower, kStateFollower, true, 1, kNone},
// 		{kStateFollower, kStatePreCandidate, true, 0, kNone},
// 		{kStateFollower, kStateCandidate, true, 1, kNone},
// 		{kStateFollower, kStateLeader, false, 0, kNone},

// 		{kStatePreCandidate, kStateFollower, true, 0, kNone},
// 		{kStatePreCandidate, kStatePreCandidate, true, 0, kNone},
// 		{kStatePreCandidate, kStateCandidate, true, 1, kNone},
// 		{kStatePreCandidate, kStateLeader, true, 0, 1},

// 		{kStateCandidate, kStateFollower, true, 0, kNone},
// 		{kStateCandidate, kStatePreCandidate, true, 0, kNone},
// 		{kStateCandidate, kStateCandidate, true, 1, kNone},
// 		{kStateCandidate, kStateLeader, true, 0, 1},

// 		{kStateLeader, kStateFollower, true, 1, kNone},
// 		{kStateLeader, kStatePreCandidate, false, 0, kNone},
// 		{kStateLeader, kStateCandidate, false, 1, kNone},
// 		{kStateLeader, kStateLeader, true, 0, 1},
//   };

//   int i = 0;
//   for (const auto& tt : tests) {
//     auto sm = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
//     sm->set_state(tt.from);

//     switch (tt.to)
//     {
//     case kStateFollower:
//       sm->BecomeFollower(tt.wterm, tt.wlead);
//       break;
//     case kStatePreCandidate:
//       sm->BecomePreCandidate();
//     case kStateCandidate:
//       sm->BecomeCandidate();
//     case kStateLeader:
//       sm->BecomeLeader();
//     default:
//       break;
//     }

//     EXPECT_EQ(sm->term(), tt.wterm) << "i: " << i;
//     EXPECT_EQ(sm->lead(), tt.wlead) << "i: " << i;

//     i++;
//   }
// }

TEST(ReplicationTest, TestAllServerStepDown) {
  struct TestArgs {
    RaftStateType state;

    RaftStateType wstate;
    uint64_t wterm;
    uint64_t windex;
  } tests[] = {
      {kStateFollower, kStateFollower, 3, 0},
      {kStatePreCandidate, kStateFollower, 3, 0},
      {kStateCandidate, kStateFollower, 3, 0},
      {kStateLeader, kStateFollower, 3, 1},
  };

  std::vector<MessageType> tmsg_types({kMsgVote, kMsgApp});
  uint64_t tterm = 3;

  int i = 0;
  for (const auto& tt : tests) {
    auto sm = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
    switch (tt.state) {
      case kStateFollower:
        sm->BecomeFollower(1, kNone);
        break;
      case kStatePreCandidate:
        sm->BecomePreCandidate();
        break;
      case kStateCandidate:
        sm->BecomeCandidate();
        break;
      case kStateLeader:
        sm->BecomeCandidate();
        sm->BecomeLeader();
        break;
      default:
        break;
    }

    int j = 0;
    for (const auto& msg_type : tmsg_types) {
      sm->Step(ConsMsg(2, 0, msg_type, {}, tterm, tterm));

      EXPECT_EQ(sm->state(), tt.wstate) << "i: " << i << " j: " << j;
      EXPECT_EQ(sm->term(), tt.wterm) << "i: " << i << " j: " << j;
      EXPECT_EQ(sm->raft_log()->LastIndex(), tt.windex)
          << "i: " << i << " j: " << j;
      std::vector<EntryPtr> ents;
      sm->raft_log()->AllEntries(&ents);
      EXPECT_EQ(ents.size(), tt.windex) << "i: " << i << " j: " << j;
      uint64_t wlead = 2;
      if (msg_type == kMsgVote) {
        wlead = kNone;
      }
      EXPECT_EQ(sm->lead(), wlead);

      j++;
    }

    i++;
  }
}

TEST(ReplicationTest, TestLeaderAppResp) {
  // initial progress: match = 0, next = 3
  struct TestArgs {
    uint64_t index;
    bool reject;
    // progress
    uint64_t wmatch;
    uint64_t wnext;
    // message
    int wmsg_num;
    uint64_t windex;
    uint64_t wcommitted;
  } tests[] = {
      // stale resp, no replies
      {3, true, 0, 3, 0, 0, 0},
      // denied resp, leader does not commit, decrease next and send probing msg
      {2, true, 0, 2, 1, 1, 0},
      // accept resp, leader commits, broadcast with commit index
      {2, false, 2, 4, 2, 2, 2},
      // ignore heartbeat replies
      {0, false, 0, 3, 0, 0, 0},
  };

  int i = 0;
  for (const auto& tt : tests) {
    // sm ter is 1 after it becomes the leader.
    // thus the last log term must be 1 to be committed.
    auto ms = NewMSWithPeers({1, 2, 3});
    ms->Append({NewEnt(1, 0), NewEnt(2, 1)});
    auto sm = NewTestRaft(1, 10, 1, ms);
    sm->mutable_raft_log()->mutable_unstable().set_offset(3);

    sm->BecomeCandidate();
    sm->BecomeLeader();
    std::vector<Message> msgs;
    sm->ReadMessages(&msgs);
    Message msg;
    msg.set_from(2);
    msg.set_type(kMsgAppResp);
    msg.set_index(tt.index);
    msg.set_term(sm->term());
    msg.set_reject(tt.reject);
    msg.set_reject_hint(tt.index);
    sm->Step(msg);

    auto p = sm->tracker()->mutable_progress(2);
    EXPECT_EQ(p->match(), tt.wmatch) << "i: " << i;
    EXPECT_EQ(p->next(), tt.wnext) << "i: " << i;

    sm->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), tt.wmsg_num);
    int j = 0;
    for (const auto& m : msgs) {
      EXPECT_EQ(m.index(), tt.windex) << "i: " << i << " j: " << j;
      EXPECT_EQ(m.commit(), tt.wcommitted) << "i: " << i << " j: " << j;

      j++;
    }

    i++;
  }
}

TEST(ReplicationTest, TestSendAppendForProgressProbe) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();
  std::vector<Message> tmp;
  r->ReadMessages(&tmp);
  r->tracker()->mutable_progress(2)->BecomeProbe();

  // each round is a heartbeat
  std::vector<Message> msgs;
  for (int i = 0; i < 3; i++) {
    if (i == 0) {
      // we expect that raft will only send out one kMsgApp on the first
      // loop. After that, the follower is paused until a heartbeat response is
      // received.
      std::vector<Entry> ents({ConsEnt(0, 0, "somedata")});
      EXPECT_TRUE(r->AppendEntry(ents));
      r->SendAppend(2);
      r->ReadMessages(&msgs);
      EXPECT_EQ(msgs.size(), 1) << "i: " << i;
      EXPECT_EQ(msgs[0].index(), 0) << "i: " << i;
    }

    EXPECT_TRUE(r->tracker()->mutable_progress(2)->msg_app_flow_paused())
        << "i: " << i;
    for (int j = 0; j < 10; j++) {
      std::vector<Entry> ents({ConsEnt(0, 0, "somedata")});
      r->AppendEntry(ents);
      r->SendAppend(2);
      r->ReadMessages(&msgs);
      EXPECT_EQ(msgs.size(), 0) << "i: " << i << " j: " << j;
    }

    // do a heartbeat
    for (int j = 0; j < r->heartbeat_timeout(); j++) {
      r->Step(ConsMsg(1, 1, kMsgBeat));
    }
    EXPECT_TRUE(r->tracker()->mutable_progress(2)->msg_app_flow_paused());

    // consume the heartbeat
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0].type(), kMsgHeartbeat);
  }

  // a heartbeat response will allow another message to be sent
  r->Step(ConsMsg(2, 1, kMsgHeartbeatResp));
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].index(), 0);
  EXPECT_TRUE(r->tracker()->mutable_progress(2)->msg_app_flow_paused());
}

TEST(ReplicationTest, TestSendAppendForProgressReplicate) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();
  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  r->tracker()->mutable_progress(2)->BecomeReplicate();

  for (int i = 0; i < 10; i++) {
    std::vector<Entry> ents({ConsEnt(0, 0, "somedata")});
    EXPECT_TRUE(r->AppendEntry(ents));
    r->SendAppend(2);
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
  }
}

TEST(ReplicationTest, TestSendAppendForProgressSnapshot) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  r->BecomeCandidate();
  r->BecomeLeader();
  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  r->tracker()->mutable_progress(2)->BecomeSnapshot(10);

  for (int i = 0; i < 10; i++) {
    std::vector<Entry> ents({ConsEnt(0, 0, "somedata")});
    EXPECT_TRUE(r->AppendEntry(ents));
    r->SendAppend(2);
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 0);
  }
}

TEST(ReplicationTest, TestRecvMsgUnreachable) {
  std::vector<EntryPtr> prev_ents({NewEnt(1, 1), NewEnt(2, 1), NewEnt(3, 1)});
  auto s = NewMSWithPeers({1, 2});
  s->Append(prev_ents);
  auto r = NewTestRaft(1, 10, 1, s);
  r->BecomeCandidate();
  r->BecomeLeader();
  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  // set node 2 to state replicate
  r->tracker()->mutable_progress(2)->set_match(3);
  r->tracker()->mutable_progress(2)->BecomeReplicate();
  r->tracker()->mutable_progress(2)->OptimisticUpdate(5);

  r->Step(ConsMsg(2, 1, kMsgUnreachable));

  EXPECT_EQ(r->tracker()->mutable_progress(2)->state(), kStateProbe);
  EXPECT_EQ(r->tracker()->mutable_progress(2)->match() + 1,
            r->tracker()->mutable_progress(2)->next());
}

TEST(ReplicationTest, TestFastLogRejection) {
  struct TestArgs {
    // logs on the leader
    std::vector<EntryPtr> leader_log;
    // logs on the follower
    std::vector<EntryPtr> follower_log;
    // expected term included in rejected kMsgAppResp
    uint64_t reject_hint_term;
    // expected index included in rejected kMsgAppResp
    uint64_t reject_hint_index;
    // expected term when leader appends after rejected.
    uint64_t next_append_term;
    // expected index when leader appends after rejected.
    uint64_t next_append_index;
  } tests[] = {
      {{
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(4, 4),
           NEnt(4, 5),
           NEnt(4, 6),
           NEnt(4, 7),
       },
       {
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(3, 4),
           NEnt(3, 5),
           NEnt(3, 6),
           NEnt(3, 7),
           NEnt(3, 8),
           NEnt(3, 9),
           NEnt(3, 10),
           NEnt(3, 11),
       },
       3,
       7,
       2,
       3},
      // This case tests that leader can find the conflict index quickly.
      // Firstly, leader appends (type = kMsgApp, index = 8, log_term = 5)
      // After rejected, leader appends (type = kMsgApp, index = 4,
      // log_term = 3)
      {{
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(3, 4),
           NEnt(4, 5),
           NEnt(4, 6),
           NEnt(4, 7),
           NEnt(5, 8),
       },
       {
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(3, 4),
           NEnt(3, 5),
           NEnt(3, 6),
           NEnt(3, 7),
           NEnt(3, 8),
           NEnt(3, 9),
           NEnt(3, 10),
           NEnt(3, 11),
       },
       3,
       8,
       3,
       4},
      // This case tests that follower can find the conflict index quickly.
      // Firstly, leader appends (type = kMsgApp, index = 4, log_term = 1)
      // After rejected, leader appends (type = kMsgApp, index = 1,
      // log_term = 1)
      {
          {
              NEnt(1, 1),
              NEnt(1, 2),
              NEnt(1, 3),
              NEnt(1, 4),
          },
          {
              NEnt(1, 1),
              NEnt(2, 2),
              NEnt(3, 3),
              NEnt(4, 4),
          },
          1,
          1,
          1,
          1,
      },
      // This case is similar to the previous case. However, this time, the
      // leader has a longer uncommitted log tail than the follower.
      // Firstly, leader appends (type = kMsgApp, index = 6, log_term = 1)
      // After rejected, leader appends (type = kMsgApp, index = 1,
      // log_term = 1)
      {{
           NEnt(1, 1),
           NEnt(1, 2),
           NEnt(1, 3),
           NEnt(1, 4),
           NEnt(1, 5),
           NEnt(1, 6),
       },
       {
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(3, 3),
           NEnt(4, 4),
       },
       1,
       1,
       1,
       1},
      // This case is similar to the previous case. However, this time, the
      // follower has a longer uncommitted log tail than the leader.
      // Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
      // After rejected leader appends (type=MsgApp,index=1,logTerm=1).
      {{
           NEnt(1, 1),
           NEnt(1, 2),
           NEnt(1, 3),
           NEnt(1, 4),
       },
       {
           NEnt(1, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(4, 4),
           NEnt(4, 5),
           NEnt(4, 6),
       },
       1,
       1,
       1,
       1},
      // An normal case that there are no log conflicts.
      // Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
      // After rejected leader appends (type=MsgApp,index=4,logTerm=4).
      {{
           NEnt(1, 1),
           NEnt(1, 2),
           NEnt(1, 3),
           NEnt(4, 4),
           NEnt(5, 5),
       },
       {
           NEnt(1, 1),
           NEnt(1, 2),
           NEnt(1, 3),
           NEnt(4, 4),
       },
       4,
       4,
       4,
       4},
      // Test case from example comment in stepLeader (on leader).
      {{
           NEnt(2, 1),
           NEnt(5, 2),
           NEnt(5, 3),
           NEnt(5, 4),
           NEnt(5, 5),
           NEnt(5, 6),
           NEnt(5, 7),
           NEnt(5, 8),
           NEnt(5, 9),
       },
       {
           NEnt(2, 1),
           NEnt(4, 2),
           NEnt(4, 3),
           NEnt(4, 4),
           NEnt(4, 5),
           NEnt(4, 6),
       },
       4,
       6,
       2,
       1},
      // Test case from example comment in handleAppendNEntries (on follower).
      {{
           NEnt(2, 1),
           NEnt(2, 2),
           NEnt(2, 3),
           NEnt(2, 4),
           NEnt(2, 5),
       },
       {
           NEnt(2, 1),
           NEnt(4, 2),
           NEnt(4, 3),
           NEnt(4, 4),
           NEnt(4, 5),
           NEnt(4, 6),
           NEnt(4, 7),
           NEnt(4, 8),
       },
       2,
       1,
       2,
       1}};

  int i = 0;
  for (const auto& tt : tests) {
    auto s1 = NewMSWithEnts({});
    auto meta = s1->mutable_snapshot()->mutable_metadata();
    ConfState cs;
    cs.add_voters(1);
    cs.add_voters(2);
    cs.add_voters(3);
    *meta->mutable_conf_state() = cs;
    s1->Append(tt.leader_log);
    auto s2 = NewMSWithEnts({});
    auto meta2 = s1->mutable_snapshot()->mutable_metadata();
    *meta2->mutable_conf_state() = cs;
    s2->Append(tt.follower_log);

    auto n1 = NewTestRaft(1, 10, 1, s1);
    auto n2 = NewTestRaft(2, 10, 1, s2);

    n1->BecomeCandidate();
    n1->BecomeLeader();

    n2->Step(ConsMsg(1, 1, kMsgHeartbeat));

    std::vector<Message> msgs;
    n2->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
    EXPECT_EQ(msgs[0].type(), kMsgHeartbeatResp) << "i: " << i;
    EXPECT_EQ(n1->Step(msgs[0]), kOk) << "i: " << i;

    msgs.clear();
    n1->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
    EXPECT_EQ(msgs[0].type(), kMsgApp) << "i: " << i;
    EXPECT_EQ(n2->Step(msgs[0]), kOk) << "i: " << i;

    msgs.clear();
    n2->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
    EXPECT_EQ(msgs[0].type(), kMsgAppResp) << "i: " << i;
    EXPECT_TRUE(msgs[0].reject()) << "i: " << i;
    EXPECT_EQ(msgs[0].log_term(), tt.reject_hint_term) << "i: " << i;
    EXPECT_EQ(msgs[0].reject_hint(), tt.reject_hint_index) << "i: " << i;

    EXPECT_EQ(n1->Step(msgs[0]), kOk) << "i: " << i;
    msgs.clear();
    n1->ReadMessages(&msgs);
    EXPECT_EQ(msgs[0].log_term(), tt.next_append_term) << "i: " << i;
    EXPECT_EQ(msgs[0].index(), tt.next_append_index) << "i: " << i;

    i++;
  }
}

// TestReadOnlyForNewLeader ensures that a leader only accepts
// raft::kMsgReadIndex message when it commits at least one log entry at it
// term.
TEST(RaftTest, TestReadOnlyForNewLeader) {
  struct ConfigsArgs {
    uint64_t id;
    uint64_t committed;
    uint64_t applied;
    uint64_t compact_index;
  } node_configs[] = {
      {1, 1, 1, 0},
      {2, 2, 2, 2},
      {3, 2, 2, 2},
  };

  std::vector<StateMachinePtr> peers;
  for (const auto& c : node_configs) {
    auto storage = NewMSWithPeers({1, 2, 3});
    storage->Append({NewEnt(1, 1), NewEnt(2, 1)});
    HardState hs;
    hs.set_term(1);
    hs.set_commit(c.committed);
    storage->set_hard_state(hs);
    if (c.compact_index != 0) {
      storage->Compact(c.compact_index);
    }
    auto cfg = ConsConfig(c.id, 10, 1, storage);
    cfg.applied = c.applied;
    auto raft = std::make_shared<Raft>(&cfg);
    auto sm = NewSMWithRaft(raft);
    peers.push_back(sm);
  }
  Network nt(peers);

  // drop kMsgApp to forbid peer a to commit any log entry at its term
  // after it becomes leader.
  nt.Ignore(kMsgApp);
  // force peer a to become leader.
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  auto sm = nt.peer(1)->raft();
  EXPECT_EQ(sm->state(), kStateLeader);

  // ensures peer a drops read only request
  uint64_t windex = 4;
  std::string wctx("ctx");
  nt.Send({ConsMsg(1, 1, kMsgReadIndex, {ConsEnt(0, 0, wctx)})});
  EXPECT_TRUE(sm->read_states().empty());

  nt.Recover();

  // Force peer a to commit a log entry at its term.
  for (int i = 0; i < sm->heartbeat_timeout(); i++) {
    sm->Tick();
  }
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt()})});
  EXPECT_EQ(sm->raft_log()->committed(), 4);
  uint64_t tmp_term;
  ErrNum err = sm->raft_log()->Term(sm->raft_log()->committed(), &tmp_term);
  uint64_t last_log_term =
      sm->raft_log()->ZeroTermOnErrCompacted(tmp_term, err);
  EXPECT_EQ(last_log_term, sm->term());

  // Ensure peer a processed postponed read only after it committed an entry at
  // its term.
  EXPECT_EQ(sm->read_states().size(), 1);

  auto rs = sm->read_states()[0];
  EXPECT_EQ(rs.index, windex);
  EXPECT_EQ(rs.request_ctx, wctx);

  // Ensures peer a accepts read only request after it committed an entry at
  // its term.
  nt.Send({ConsMsg(1, 1, kMsgReadIndex, {ConsEnt(0, 0, wctx)})});
  EXPECT_EQ(sm->read_states().size(), 2);
  rs = sm->read_states()[1];
  EXPECT_EQ(rs.index, windex);
  EXPECT_EQ(rs.request_ctx, wctx);
}

}  // namespace jraft
