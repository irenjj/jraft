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
#include <cmath>

#include <gtest/gtest.h>

#include "common/util.h"
#include "test_util/network.h"
#include "test_util/test_util.h"
#include "raft.h"

namespace jraft {

// TestUpdateTermFromMessage tests if one server's current term is smaller
// than the other's, then it updates its current term to the larger value.
// If a candidate or leader discovers that its term is out of date, it
// immediately reverts to follower state.
// Reference: section 5.1
void TestUpdateTermFromMessage(RaftStateType state) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  switch (state) {
    case kStateFollower:
      r->BecomeFollower(1, 2);
      break;
    case kStateCandidate:
      r->BecomeCandidate();
      break;
    case kStateLeader:
      r->BecomeCandidate();
      r->BecomeLeader();
      break;
    default:
      break;
  }

  r->Step(ConsMsg(0, 0, kMsgApp, {}, 2));
  EXPECT_EQ(r->term(), 2);
  EXPECT_EQ(r->state(), kStateFollower);
}

TEST(ElectionTest, TestFollowerUpdateTermFromMessage) {
  TestUpdateTermFromMessage(kStateFollower);
}

TEST(ElectionTest, TestCandidateUpdateTermFromMessage) {
  TestUpdateTermFromMessage(kStateCandidate);
}

TEST(ElectionTest, TestLeaderUpdateTermFromMessage) {
  TestUpdateTermFromMessage(kStateLeader);
}

// TestStartAsFollower tests that when servers start up, they begin as
// followers.
// Reference: 5.2
TEST(ElectionTest, TestStartAsFollower) {
  auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  EXPECT_EQ(r->state(), kStateFollower);
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a kMsgHeartbeat with m.index() = 0, m.log_term = 0 and
// empty entries as heartbeat to all followers. Reference: section 5.2
TEST(ElectionTest, TestLeaderBcastBeat) {
  // heartbeat interval
  int hi = 1;
  auto r = NewTestRaft(1, 10, hi, NewMSWithPeers({1, 2, 3}));
  r->BecomeCandidate();
  r->BecomeLeader();

  for (uint64_t i = 0; i < 10; i++) {
    std::vector<Entry> ents({ConsEnt(i + 1)});
    EXPECT_TRUE(r->AppendEntry(ents));
  }

  for (int i = 0; i < hi; i++) {
    r->Tick();
  }

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  std::vector<Message> wmsgs({ConsMsg(1, 2, kMsgHeartbeat, {}, 1),
                              ConsMsg(1, 3, kMsgHeartbeat, {}, 1)});
  EXPECT_TRUE(MsgDeepEqual(msgs, wmsgs));
}

// TestNonLeaderStartElection tests that if a follower receives no
// communication over election timeout, it begins an election to choose a new
// leader. It increases its current term and transitions to candidate state.
// It then votes for itself and issues RequestVote RPCs in parallel to each of
// the other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and start
// a new election by incrementing its term and initiating another round of
// RequestVote RPCs.
// reference: section 5.2
void TestNonLeaderStartElection(RaftStateType state) {
  // election timeout
  int et = 10;
  auto r = NewTestRaft(1, et, 1, NewMSWithPeers({1, 2, 3}));
  switch (state) {
    case kStateFollower:
      r->BecomeFollower(1, 2);
      break;
    case kStateCandidate:
      r->BecomeCandidate();
      break;
    default:
      break;
  };

  for (int i = 1; i < 2 * et; i++) {
    r->Tick();
  }

  EXPECT_EQ(r->term(), 2);
  EXPECT_EQ(r->state(), kStateCandidate);
  EXPECT_TRUE(r->tracker()->votes(r->id()));

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  std::vector<Message> wmsgs(
      {ConsMsg(1, 2, kMsgVote, {}, 2), ConsMsg(1, 3, kMsgVote, {}, 2)});
  std::sort(msgs.begin(), msgs.end(), [](const Message& m1, const Message& m2) {
    return m1.to() < m2.to();
  });
  EXPECT_TRUE(MsgDeepEqual(msgs, wmsgs));
}

TEST(ElectionTest, TestFollowerStartElection) {
  TestNonLeaderStartElection(kStateFollower);
}

TEST(ElectionTest, TestCandidateStartElection) {
  TestNonLeaderStartElection(kStateCandidate);
}

std::unordered_map<uint64_t, bool> Map(const std::vector<uint64_t>& ids = {},
                                       const std::vector<bool>& vts = {}) {
  std::unordered_map<uint64_t, bool> res;
  for (size_t i = 0; i < ids.size(); i++) {
    res[ids[i]] = vts[i];
  }

  return res;
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in leader
// election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
TEST(ElectionTest, TestLeaderElectionInOneRoundRPC) {
  struct TestArgs {
    int size;
    std::unordered_map<uint64_t, bool> votes;
    RaftStateType state;
  } tests[] = {
      // win the election when receiving votes from a majority of the servers
      {1, Map(), kStateLeader},
      {3, Map({2, 3}, {true, true}), kStateLeader},
      {3, Map({2}, {true}), kStateLeader},
      {5, Map({2, 3, 4, 5}, {true, true, true, true}), kStateLeader},
      {5, Map({2, 3, 4}, {true, true, true}), kStateLeader},
      {5, Map({3, 4}, {true, true}), kStateLeader},

      // return to follower state if it receives vote denial from a majority
      {3, Map({2, 3}, {false, false}), kStateFollower},
      {5, Map({2, 3, 4, 5}, {false, false, false, false}), kStateFollower},
      {5, Map({2, 3, 4, 5}, {false, false, false, false}), kStateFollower},

      // stay in candidate if it does not obtain the majority
      {3, Map(), kStateCandidate},
      {5, Map({2}, {true}), kStateCandidate},
      {5, Map({2, 3}, {false, false}), kStateCandidate},
      {5, Map(), kStateCandidate},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers(IdsBySize(tt.size)));

    r->Step(ConsMsg(1, 1, kMsgHup));
    for (const auto& iter : tt.votes) {
      auto m = ConsMsg(iter.first, 1, kMsgVoteResp, {}, r->term());
      m.set_reject(!iter.second);
      r->Step(m);
    }

    EXPECT_EQ(r->state(), tt.state) << "i: " << i;
    EXPECT_EQ(r->term(), 1) << "i: " << i;

    i++;
  }
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
TEST(ElectionTest, TestFollowerVote) {
  struct TestArgs {
    uint64_t vote;
    uint64_t nvote;
    bool wreject;
  } tests[] = {
      {kNone, 1, false}, {kNone, 2, false}, {1, 1, false},
      {2, 2, false},     {1, 2, true},      {2, 1, true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
    HardState hs;
    hs.set_term(1);
    hs.set_vote(tt.vote);
    r->LoadState(hs);

    r->Step(ConsMsg(tt.nvote, 1, kMsgVote, {}, 1));

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    Message m = ConsMsg(1, tt.nvote, kMsgVoteResp, {}, 1);
    m.set_reject(tt.wreject);
    std::vector<Message> wmsgs({m});
    EXPECT_TRUE(MsgDeepEqual(msgs, wmsgs)) << "i: " << i;

    i++;
  }
}

// TestCandidateFallback tests that while waiting for votes, if a candidate
// receives an AppendEntry RPC from another server claiming to be leader
// whose term is at least as large as the candidate's current term, it
// recognizes the leader as legitimate and returns to follower state.
// Reference: 5.2
TEST(ElectionTest, TestCandidateFallback) {
  struct TestArgs {
    Message m;
  } tests[] = {
      {ConsMsg(2, 1, kMsgApp, {}, 1)},
      {ConsMsg(2, 1, kMsgApp, {}, 2)},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
    r->Step(ConsMsg(1, 1, kMsgHup));
    EXPECT_EQ(r->state(), kStateCandidate) << "i: " << i;

    r->Step(tt.m);

    EXPECT_EQ(r->state(), kStateFollower) << "i: " << i;
    EXPECT_EQ(r->term(), tt.m.term()) << "i: " << i;

    i++;
  }
}

// TestNonLeaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
void TestNonLeaderElectionTimeoutRandomized(RaftStateType state) {
  int et = 10;
  auto r = NewTestRaft(1, et, 1, NewMSWithPeers({1, 2, 3}));
  std::unordered_map<int, bool> timeouts;
  for (int round = 0; round < 50 * et; round++) {
    switch (state) {
      case kStateFollower:
        r->BecomeFollower(r->term() + 1, 2);
        break;
      case kStateCandidate:
        r->BecomeCandidate();
        break;
      default:
        break;
    }

    int time = 0;
    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    while (msgs.empty()) {
      r->Tick();
      time++;
      r->ReadMessages(&msgs);
    }
    timeouts[time] = true;
  }

  for (int d = et + 1; d < et * 2; d++) {
    EXPECT_FALSE(timeouts.find(d) == timeouts.end());
  }
}

// TestNonLeadersElectionTimeoutNonconflict tests that in most cases only a
// single server (follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
void TestNonleadersElectionTimeoutNonconflict(RaftStateType state) {
  int et = 10;
  int size = 5;
  std::vector<RaftPtr> rs;
  auto ids = IdsBySize(size);
  for (const auto& id : ids) {
    rs.emplace_back(NewTestRaft(id, et, 1, NewMSWithPeers({ids})));
  }
  int conflict = 0;
  for (int round = 0; round < 1000; round++) {
    for (const auto& r : rs) {
      switch (state) {
        case kStateFollower:
          r->BecomeFollower(r->term() + 1, kNone);
          break;
        case kStateCandidate:
          r->BecomeCandidate();
          break;
        default:
          break;
      }
    }

    int timeout_num = 0;
    while (timeout_num == 0) {
      for (const auto& r : rs) {
        r->Tick();
        std::vector<Message> msgs;
        r->ReadMessages(&msgs);
        if (!msgs.empty()) {
          timeout_num++;
        }
      }
    }

    // server rafts time out at the same tick
    if (timeout_num > 1) {
      conflict++;
    }
  }

  double g = static_cast<double>(conflict) / 1000.0;
  EXPECT_LT(g, 0.3);
}

TEST(ElectionTest, TestFollowersElectionTimeoutNonconflict) {
  TestNonleadersElectionTimeoutNonconflict(kStateFollower);
}

TEST(ElectionTest, TestCandidatesElectionTimeoutNonconflict) {
  TestNonleadersElectionTimeoutNonconflict(kStateCandidate);
}

Entry Ent(uint64_t term = 0, uint64_t index = 0) {
  Entry e;
  e.set_term(term);
  e.set_index(index);

  return e;
}

// TestVoteRequest tests that the vote request includes information about
// the candidate's log and are sent to all the other nodes.
TEST(ElectionTest, TestVoteRequest) {
  struct TestArgs {
    std::vector<Entry> ents;
    uint64_t wterm;
  } tests[] = {{{Ent(1, 1)}, 2}, {{Ent(1, 1), Ent(2, 2)}, 3}};

  int i = 0;
  for (const auto& tt : tests) {
    auto r = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
    r->Step(ConsMsg(2, 1, kMsgApp, tt.ents, tt.wterm - 1));

    r->ReadMessages();

    for (int j = 1; j < r->election_timeout() * 2; j++) {
      r->Tick();
    }

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 2);
    for (uint64_t j = 0; j < msgs.size(); j++) {
      auto m = msgs[j];
      EXPECT_EQ(m.type(), kMsgVote) << "i: " << i << " j: " << j;
      EXPECT_EQ(m.to(), j + 2) << "i: " << i << "j: " << j;
      EXPECT_EQ(m.term(), tt.wterm) << "i: " << i << " j: " << j;
      uint64_t windex = tt.ents.back().index();
      uint64_t wterm = tt.ents.back().term();
      EXPECT_EQ(m.index(), windex) << "i: " << i << " j: " << j;
      EXPECT_EQ(m.log_term(), wterm) << "i: " << i << " j: " << j;
    }

    i++;
  }
}

EntryPtr NEnt(uint64_t term = 0, uint64_t index = 0) {
  auto e = std::make_shared<Entry>();
  e->set_term(term);
  e->set_index(index);

  return e;
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than of the candidate.
// Reference: section 5.4.1
TEST(ElectionTest, TestVoter) {
  struct TestArgs {
    std::vector<EntryPtr> ents;
    uint64_t log_term;
    uint64_t index;

    bool wreject;
  } tests[] = {
      // same log term
      {{NEnt(1, 1)}, 1, 1, false},
      {
          {NEnt(1, 1)},
          1,
          2,
          false,
      },
      {{NEnt(1, 1), NEnt(1, 2)}, 1, 1, true},

      // candidate higher log_term
      {{NEnt(1, 1)}, 2, 1, false},
      {{NEnt(1, 1)}, 2, 2, false},
      {{NEnt(1, 1), NEnt(1, 2)}, 2, 1, false},

      // voter higher log_term
      {{NEnt(2, 1)}, 1, 1, true},
      {{NEnt(2, 1)}, 1, 2, true},
      {{NEnt(2, 1), NEnt(1, 2)}, 1, 1, true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto storage = NewMSWithPeers({1, 2});
    storage->Append(tt.ents);
    auto r = NewTestRaft(1, 10, 1, storage);

    r->Step(ConsMsg(2, 1, kMsgVote, {}, 3, tt.log_term, tt.index));

    std::vector<Message> msgs;
    r->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), 1) << "i: " << i;
    EXPECT_EQ(msgs[0].type(), kMsgVoteResp) << "i: " << i;
    EXPECT_EQ(msgs[0].reject(), tt.wreject) << "i: " << i;

    i++;
  }
}

void TestLeaderElection(bool prevote) {
  std::function<void(Config& cfg)> cfg_func = nullptr;
  RaftStateType cand_state = kStateCandidate;
  uint64_t cand_term = 1;
  if (prevote) {
    cfg_func = PrevoteConfig;
    // In pre-vote mode, an election that fails to complete leaves the node in
    // pre-candidate state without advancing the term.
    cand_state = kStatePreCandidate;
    cand_term = 0;
  }

  struct TestArgs {
    Network net;
    RaftStateType state;
    uint64_t exp_term;
  } tests[] = {{
                   Network({nullptr, nullptr, nullptr}, cfg_func),
                   kStateLeader,
                   1,
               },
               {
                   Network({nullptr, nullptr, std::make_shared<StateMachine>()},
                           cfg_func),
                   kStateLeader,
                   1,
               },
               {
                   Network({nullptr, std::make_shared<StateMachine>(),
                            std::make_shared<StateMachine>()},
                           cfg_func),
                   cand_state,
                   cand_term,
               },
               {
                   Network({nullptr, std::make_shared<StateMachine>(),
                            std::make_shared<StateMachine>(), nullptr},
                           cfg_func),
                   cand_state,
                   cand_term,
               },
               {
                   Network({nullptr, std::make_shared<StateMachine>(),
                            std::make_shared<StateMachine>(), nullptr, nullptr},
                           cfg_func),
                   kStateLeader,
                   1,
               },

               // three logs further along than 0, but in the same term so
               // rejections are returned instead of the votes being ignores.
               {
                   Network({nullptr, NewSMWithEnts({1}, cfg_func),
                            NewSMWithEnts({1}, cfg_func),
                            NewSMWithEnts({1, 1}, cfg_func), nullptr},
                           cfg_func),
                   kStateFollower,
                   1,
               }};

  int i = 0;
  for (auto& tt : tests) {
    Message m = ConsMsg(1, 1, kMsgHup, {});
    std::vector<Message> msgs({m});
    tt.net.Send(msgs);
    auto sm = tt.net.peer(1);
    EXPECT_EQ(sm->raft()->state(), tt.state) << "i: " << i;
    EXPECT_EQ(sm->raft()->term(), tt.exp_term) << "i: " << i;

    i++;
  }
}

TEST(ElectionTest, TestLeaderElectionWithoutPrevote) {
  TestLeaderElection(false);
}

TEST(ElectionTest, TestLeaderElectionWithPrevote) { TestLeaderElection(true); }

// TestLearnerElectionTimeout verifies that the leader should not start election
// even when times out.
TEST(ElectionTest, TestLearnerElectionTimeout) {
  auto ms1 = NewMSWithPL({1}, {2});
  auto c1 = ConsConfig(1, 10, 1, ms1);
  Raft n1(&c1);

  auto ms2 = NewMSWithPL({1}, {2});
  auto c2 = ConsConfig(2, 10, 1, ms2);
  Raft n2(&c2);

  // n2 is learner. Learner should not start election even when times out.
  n1.BecomeFollower(1, kNone);
  n2.BecomeFollower(1, kNone);

  n2.set_randomized_election_timeout(n2.election_timeout());
  for (int i = 0; i < n2.election_timeout(); i++) {
    n2.Tick();
  }

  EXPECT_EQ(n2.state(), kStateFollower);
}

// TestLearnerPromotion verifies that the learner should not election until
// it is promoted to a normal peer.
TEST(ElectionTest, TestLearnerPromotion) {
  auto ms1 = NewMSWithPL({1}, {2});
  auto c1 = ConsConfig(1, 10, 1, ms1);
  auto n1 = std::make_shared<Raft>(&c1);

  auto ms2 = NewMSWithPL({1}, {2});
  auto c2 = ConsConfig(2, 10, 1, ms2);
  auto n2 = std::make_shared<Raft>(&c2);

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);

  auto sm1 = std::make_shared<StateMachine>(kRaftType, n1);
  auto sm2 = std::make_shared<StateMachine>(kRaftType, n2);
  Network nt({sm1, sm2}, nullptr);

  EXPECT_NE(n1->state(), kStateLeader);

  // n1 should become leader
  n1->set_randomized_election_timeout(n1->election_timeout());
  for (int i = 0; i < n1->election_timeout(); i++) {
    n1->Tick();
  }

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);

  Message m;
  m.set_from(1);
  m.set_to(1);
  m.set_type(kMsgBeat);
  nt.Send({m});

  n1->AddNode(2);
  n2->AddNode(2);
  EXPECT_FALSE(n2->is_learner());

  // n2 start election, should become leader
  n2->set_randomized_election_timeout(n2->election_timeout());
  for (int i = 0; i < n2->election_timeout(); i++) {
    n2->Tick();
  }

  Message m2;
  m2.set_from(2);
  m2.set_to(2);
  m2.set_type(kMsgBeat);
  nt.Send({m2});

  EXPECT_EQ(n1->state(), kStateFollower);
  EXPECT_EQ(n2->state(), kStateLeader);
}

// TestLearnerCanVote checks that a learner can vote when it receives a valid
// vote request. See Raft.Step for why this is necessary and correct behavior.
TEST(ElectionTest, TestLearnerCanVote) {
  auto ms2 = NewMSWithPL({1}, {2});
  auto c2 = ConsConfig(2, 10, 1, ms2);
  Raft n2(&c2);

  n2.BecomeFollower(1, kNone);

  Message m;
  m.set_from(1);
  m.set_to(2);
  m.set_term(2);
  m.set_type(kMsgVote);
  m.set_log_term(11);
  m.set_index(11);
  n2.Step(m);

  EXPECT_EQ(n2.msgs().size(), 1);
  Message msg = n2.msgs()[0];
  EXPECT_FALSE(msg.type() != kMsgVoteResp && !msg.reject());
}

// TestLeaderCycle verifies that each node in a cluster can campaign can be
// elected in turn. This ensures that elections (including pre-vote) work
// when not starting from a clean slate (as they do in TestLeaderElection)
void TestLeaderCycle(bool prevote) {
  std::function<void(Config& cfg)> cfg_func = nullptr;
  if (prevote) {
    cfg_func = PrevoteConfig;
  }
  Network n({nullptr, nullptr, nullptr}, cfg_func);
  for (uint64_t campaign_id = 1; campaign_id <= 3; campaign_id++) {
    Message m;
    m.set_from(campaign_id);
    m.set_to(campaign_id);
    m.set_type(kMsgHup);
    n.Send({m});

    for (const auto& peer : n.peers()) {
      auto r = peer.second->raft();
      EXPECT_FALSE(r->id() == campaign_id && r->state() != kStateLeader);
      EXPECT_FALSE(r->id() != campaign_id && r->state() != kStateFollower);
    }
  }
}

TEST(ElectionTest, TestLeaderCycleWithoutPrevote) { TestLeaderCycle(false); }

// TestLeaderCycleWithoutPrevote tests a scenario in which a newly-elected
// leader does *not* have the newest (i.e. highest term) log entries, and
// must overwrite higher-term log entries with lower-term once.
TEST(ElectionTest, TestLeaderCycleWithPrevote) { TestLeaderCycle(true); }

// remember to free raft as well as storage
RaftPtr EntsWithConfig(const std::vector<uint64_t>& terms,
                       ConfigFunc cfg_func) {
  auto ms = std::make_shared<MemoryStorage>();
  for (size_t i = 0; i < terms.size(); i++) {
    auto e = NewEnt(static_cast<uint64_t>(i + 1), terms[i]);
    ms->Append({e});
  }
  auto cfg = ConsConfig(1, 5, 1, ms);
  if (cfg_func != nullptr) {
    cfg_func(cfg);
  }
  auto r = std::make_shared<Raft>(&cfg);
  r->Reset(terms.back());
  return r;
}

// VotedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in the
// given term but has not received any logs).
RaftPtr VotedWithConfig(uint64_t vote, uint64_t term, ConfigFunc cfg_func) {
  auto ms = std::make_shared<MemoryStorage>();
  HardState hs;
  hs.set_vote(vote);
  hs.set_term(term);
  ms->set_hard_state(hs);
  auto cfg = ConsConfig(1, 5, 1, ms);
  if (cfg_func != nullptr) {
    cfg_func(cfg);
  }

  auto r = std::make_shared<Raft>(&cfg);
  r->Reset(term);
  return r;
}

void TestLeaderElectionOverwriteNewerLogs(bool prevote) {
  std::function<void(Config& cfg)> cfg_func = nullptr;
  if (prevote) {
    cfg_func = PrevoteConfig;
  }

  // This network represents the results of the following sequence of events:
  // - Node 1 won the election in term 1.
  // - Node 1 repicate a log entry to node 2 but died before sending it to
  //   other nodes.
  // - Node 3 won the second election in term 2.
  // - Node 3 wrote an entry to its logs but died without sending it to any
  // other nodes.
  //
  // At this point, nodes 1, 2, and 3 all have uncommitted entries in their
  // logs and could win a election at term 3. The winner's log entry overwrites
  // the losers'. (TestLeaderSyncFollowerLog tests the case where older log
  // entries are overwritten, so this test focuses on the case where the newer
  // entries are lost).
  auto r1 = EntsWithConfig({1}, cfg_func);    // won first election
  auto r2 = EntsWithConfig({1}, cfg_func);    // got logs from node 1
  auto r3 = EntsWithConfig({2}, cfg_func);    // won second election
  auto r4 = VotedWithConfig(3, 2, cfg_func);  // voted but didn't get logs
  auto r5 = VotedWithConfig(3, 2, cfg_func);  // voted but didn't get logs
  auto sm1 = NewSMWithRaft(r1);
  auto sm2 = NewSMWithRaft(r2);
  auto sm3 = NewSMWithRaft(r3);
  auto sm4 = NewSMWithRaft(r4);
  auto sm5 = NewSMWithRaft(r5);

  Network n({sm1, sm2, sm3, sm4, sm5}, cfg_func);

  // Node 1 campaign. The election fails because a quorum of nodes knows about
  //  the election that already happened at term 2. Node 1's term is
  // pushed ahead to 2.
  Message m;
  m.set_from(1);
  m.set_to(1);
  m.set_type(kMsgHup);
  n.Send({m});
  EXPECT_EQ(r1->state(), kStateFollower);
  EXPECT_EQ(r1->term(), 2);

  // Node 1 campaign again with a higher term. This time it succeeds.
  Message m2;
  m2.set_from(1);
  m2.set_to(1);
  m2.set_type(kMsgHup);
  n.Send({m2});
  EXPECT_EQ(r1->state(), kStateLeader);
  EXPECT_EQ(r1->term(), 3);

  // Now all nodes agree on a log entry with term 1 at index 1 (and term 3 at
  // index 2).
  int i = 0;
  for (const auto& p : n.peers()) {
    auto r = p.second->raft();
    std::vector<EntryPtr> ents;
    r->raft_log()->AllEntries(&ents);
    EXPECT_EQ(ents.size(), 2) << "i: " << i;
    EXPECT_EQ(ents[0]->term(), 1) << "i: " << i;
    EXPECT_EQ(ents[1]->term(), 3) << "i: " << i;

    i++;
  }
}

TEST(ElectionTest, TestLeaderElectionOverwriteNewerLogsWithoutPrevote) {
  TestLeaderElectionOverwriteNewerLogs(false);
}

TEST(ElectionTest, TestLeaderElectionOverwriteNewerLogsWithPrevote) {
  TestLeaderElectionOverwriteNewerLogs(true);
}

void TestVoteFromAnyState(MessageType vt) {
  for (auto i = (size_t)kStateFollower; i < (size_t)kNumStates; i++) {
    auto ms = NewMSWithPeers({1, 2, 3});
    auto r = NewTestRaft(1, 10, 1, ms);
    r->set_term(1);

    RaftStateType st = (RaftStateType)i;
    switch (st) {
      case kStateFollower:
        r->BecomeFollower(r->term(), 3);
        break;
      case kStatePreCandidate:
        r->BecomePreCandidate();
        break;
      case kStateCandidate:
        r->BecomeCandidate();
        break;
      case kStateLeader:
        r->BecomeCandidate();
        r->BecomeLeader();
        break;
      default:
        JLOG_FATAL << "shouldn't come here";
        break;
    }

    // Note that setting our state above may have advanced term past its
    // initial value.
    uint64_t origin_term = r->term();
    uint64_t new_term = r->term() + 1;

    Message msg;
    msg.set_from(2);
    msg.set_to(1);
    msg.set_type(vt);
    msg.set_term(new_term);
    msg.set_log_term(new_term);
    msg.set_index(42);
    EXPECT_EQ(r->Step(msg), kOk) << "i: " << i;
    EXPECT_EQ(r->msgs().size(), 1) << "i: " << i;
    auto resp = r->msgs()[0];
    EXPECT_EQ(resp.type(), VoteRespMsgType(vt));
    EXPECT_EQ(resp.reject(), false);

    // If this was a real vote, we reset our state and term.
    if (vt == kMsgVote) {
      EXPECT_EQ(r->state(), kStateFollower);
      EXPECT_EQ(r->term(), new_term);
      EXPECT_EQ(r->vote(), 2);
    } else {
      // In a prevote, nothing changes.
      EXPECT_EQ(r->state(), st);
      EXPECT_EQ(r->term(), origin_term);
      // if st == kStateFollower or kStatePreCandidate, r hasn't voted yet.
      // In kStateCandidate or kStateLeader, it's voted for itself.
      EXPECT_FALSE(r->vote() != kNone && r->vote() != 1);
    }
  }
}

TEST(ElectionTest, TestVoteFromAnyStateWithoutPrevote) {
  TestVoteFromAnyState(kMsgVote);
}

TEST(ElectionTest, TestVoteFromAnyStateWithPrevote) {
  TestVoteFromAnyState(kMsgPreVote);
}

TEST(ElectionTest, TestDuelingCandidates) {
  auto a = NewTestRaft(ConsConfig(1, 10, 1, NewMSWithPeers({1, 2, 3})));
  auto b = NewTestRaft(ConsConfig(2, 10, 1, NewMSWithPeers({1, 2, 3})));
  auto c = NewTestRaft(ConsConfig(3, 10, 1, NewMSWithPeers({1, 2, 3})));

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});
  nt.Cut(1, 3);

  nt.Send({ConsMsg(1, 1, kMsgHup)});
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  // 1 becomes leader since it receives votes from 1 and 2
  auto r = nt.peer(1)->raft();
  EXPECT_EQ(r->state(), kStateLeader);

  // 3 stays as candidate since it receives vote from 3 and rejection from 2
  r = nt.peer(3)->raft();
  EXPECT_EQ(r->state(), kStateCandidate);

  nt.Recover();

  // candidate 3 now increase its term and tries to vote again. We expect
  // it to disrupt the leader 1 since it has a higher term 3 will be follower
  // again since both 1 and 2 rejects its vote request since 3 does not have
  // a long enough log.
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  auto ms = std::make_shared<MemoryStorage>();
  ms->Append({NewEnt(1, 1, "")});
  auto wlog = std::make_shared<RaftLog>(ms);
  wlog->set_committed(1);
  wlog->mutable_unstable().set_offset(2);

  auto ms2 = std::make_shared<MemoryStorage>();
  ms2->Append({NewEnt(1, 1, "")});
  auto wlog2 = std::make_shared<RaftLog>(ms);

  struct TestArgs {
    RaftPtr sm;
    RaftStateType state;
    uint64_t term;
    RaftLogPtr raft_log;
  } tests[] = {
      {a, kStateFollower, 2, wlog},
      {b, kStateFollower, 2, wlog},
      {c, kStateFollower, 2, wlog2},
  };

  int i = 0;
  for (const auto& tt : tests) {
    EXPECT_EQ(tt.sm->state(), tt.state) << "i: " << i;
    EXPECT_EQ(tt.sm->term(), tt.term) << "i: " << i;
    auto r = nt.peer(i + 1);
    auto l1 = tt.sm->raft_log();
    auto l2 = r->raft()->raft_log();
    EXPECT_EQ(l1->committed(), l2->committed()) << "i: " << i;
    EXPECT_TRUE(RaftLogDeepEqual(*l1, *l2)) << "i: " << i;

    i++;
  }
}

TEST(ElectionTest, TestDuelingPreCandidates) {
  auto cfg_a = ConsConfig(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto cfg_b = ConsConfig(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto cfg_c = ConsConfig(3, 10, 1, NewMSWithPeers({1, 2, 3}));
  cfg_a.pre_vote = true;
  cfg_b.pre_vote = true;
  cfg_c.pre_vote = true;
  auto a = NewTestRaft(cfg_a);
  auto b = NewTestRaft(cfg_b);
  auto c = NewTestRaft(cfg_c);

  auto nt = Network({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});
  nt.Cut(1, 3);

  nt.Send({ConsMsg(1, 1, kMsgHup)});
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  // 1 becomes leader since it receives votes from 1 and 2
  auto sm = nt.peer(1)->raft();
  EXPECT_EQ(sm->state(), kStateLeader);

  // 3 campaigns then reverts to follower when its prevote is rejected
  sm = nt.peer(3)->raft();
  EXPECT_EQ(sm->state(), kStateFollower);

  nt.Recover();

  // candidate 3 now increases its term and tries to vote again. With Prevote
  // it does not disrupt the leader.
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  auto ms = std::make_shared<MemoryStorage>();
  ms->Append({NewEnt(1, 1, "")});
  auto wlog = std::make_shared<RaftLog>(ms);
  wlog->set_committed(1);
  wlog->mutable_unstable().set_offset(2);

  auto ms2 = std::make_shared<MemoryStorage>();
  ms2->Append({NewEnt(1, 1, "")});
  auto wlog2 = std::make_shared<RaftLog>(ms);

  struct TestArgs {
    RaftPtr sm;
    RaftStateType state;
    uint64_t term;
    RaftLogPtr raft_log;
  } tests[] = {
      {a, kStateLeader, 1, wlog},
      {b, kStateFollower, 1, wlog},
      {c, kStateFollower, 1, wlog2},
  };

  int i = 0;
  for (const auto& tt : tests) {
    EXPECT_EQ(tt.sm->state(), tt.state) << "i: " << i;
    EXPECT_EQ(tt.sm->term(), tt.term) << "i: " << i;
    auto l1 = tt.sm->raft_log();
    auto r = nt.peer(i + 1);
    auto l2 = r->raft()->raft_log();
    EXPECT_EQ(l1->committed(), l2->committed()) << "i: " << i;
    EXPECT_TRUE(RaftLogDeepEqual(*l1, *l2)) << "i: " << i;

    i++;
  }
}

TEST(ElectionTest, TestCandidateConcede) {
  Network tt({nullptr, nullptr, nullptr});
  tt.Isolate(1);

  tt.Send({ConsMsg(1, 1, kMsgHup)});
  tt.Send({ConsMsg(3, 3, kMsgHup)});

  // heal the partition
  tt.Recover();
  // send heartbeat; reset wait
  tt.Send({ConsMsg(3, 3, kMsgBeat)});

  std::string data("force follower");
  // send a proposal to 3 to flush out a msgApp to 1
  tt.Send({ConsMsg(3, 3, kMsgProp, {ConsEnt(0, 0, data)})});
  // send heartbeat, flush out commit
  tt.Send({ConsMsg(3, 3, kMsgBeat)});

  auto a = tt.peer(1)->raft();
  EXPECT_EQ(a->state(), kStateFollower);
  EXPECT_EQ(a->term(), 1);

  auto ms = NewMSWithEnts({NewEnt(1, 1, ""), NewEnt(2, 1, data)});
  auto want_log = std::make_shared<RaftLog>(ms);
  want_log->mutable_unstable().set_offset(3);
  want_log->set_committed(2);

  for (size_t i = 0; i < tt.peers().size(); i++) {
    auto p = tt.peer(static_cast<uint64_t>(i) + 1);
    auto sm = p->raft();
    EXPECT_TRUE(RaftLogDeepEqual(*sm->raft_log(), *want_log.get()))
        << "i: " << i;
  }
}

TEST(ElectionTest, TestSingleNodeCandidate) {
  Network tt({nullptr});
  tt.Send({ConsMsg(1, 1, kMsgHup)});

  auto sm = tt.peer(1)->raft();
  EXPECT_EQ(sm->state(), kStateLeader);
}

TEST(ElectionTest, TestSingleNodePreCandidate) {
  Network tt({nullptr}, PrevoteConfig);
  tt.Send({ConsMsg(1, 1, kMsgHup)});

  auto sm = tt.peer(1)->raft();
  EXPECT_EQ(sm->state(), kStateLeader);
}

TEST(ElectionTest, TestPastElectionTimeout) {
  struct TestArgs {
    int elapse;
    double wprobability;
    bool round;
  } tests[] = {
      {5, 0.0, false}, {10, 0.1, true}, {13, 0.4, true},
      {15, 0.6, true}, {18, 0.9, true}, {20, 1, false},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto sm = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
    sm->set_election_elapsed(tt.elapse);
    int c = 0;
    for (int j = 0; j < 10000; j++) {
      sm->ResetRandomizedElectionTimeout();
      if (sm->PastElectionTimeout()) {
        c++;
      }
    }
    double got = double(c) / 10000.0;
    if (tt.round) {
      got = std::floor(got * 10 + 0.5) / 10.0;
    }
    EXPECT_EQ(got, tt.wprobability) << "i: " << i;

    i++;
  }
}

// TestCandidateResetTerm test when a candidate receives a kMsgHeartbeat
// or kMsgApp from leader, "Step" resets the term with leader's and
// reverts back to follower.
void TestCandidateResetTerm(MessageType mt) {
  auto a = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto b = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto c = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});

  nt.Send({ConsMsg(1, 1, kMsgHup)});
  EXPECT_EQ(a->state(), kStateLeader);
  EXPECT_EQ(b->state(), kStateFollower);
  EXPECT_EQ(c->state(), kStateFollower);

  // isolate 3 and increase term in rest
  nt.Isolate(3);

  nt.Send({ConsMsg(2, 2, kMsgHup)});
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  EXPECT_EQ(a->state(), kStateLeader);
  EXPECT_EQ(b->state(), kStateFollower);

  // trigger campaign in isolated c
  c->ResetRandomizedElectionTimeout();
  for (int i = 0; i < c->randomized_election_timeout(); i++) {
    c->Tick();
  }

  EXPECT_EQ(c->state(), kStateCandidate);

  nt.Recover();

  // leader sends to isolate candidate
  // and expects candidate to revert to follower
  nt.Send({ConsMsg(1, 3, mt, {}, a->term())});

  EXPECT_EQ(c->state(), kStateFollower);

  // follower c term is reset with leader's
  EXPECT_EQ(a->term(), c->term());
}

TEST(ElectionTest, TestCandidateResetTermHeartbeat) {
  TestCandidateResetTerm(kMsgHeartbeat);
}

TEST(ElectionTest, TestCandidateResetTermApp) {
  TestCandidateResetTerm(kMsgApp);
}

TEST(ElectionTest, TestLeaderStepdownWhenQuorumActive) {
  auto sm = NewTestRaft(1, 5, 1, NewMSWithPeers({1, 2, 3}));

  sm->set_check_quorum(true);

  sm->BecomeCandidate();
  sm->BecomeLeader();

  for (int i = 0; i < sm->election_timeout() + 1; i++) {
    sm->Step(ConsMsg(2, 0, kMsgHeartbeatResp, {}, sm->term()));
    sm->Tick();
  }

  EXPECT_EQ(sm->state(), kStateLeader);
}

TEST(ElectionTest, TestLeaderStepdownWhenQuorumLost) {
  auto sm = NewTestRaft(1, 5, 1, NewMSWithPeers({1, 2, 3}));

  sm->set_check_quorum(true);

  sm->BecomeCandidate();
  sm->BecomeLeader();

  for (int i = 0; i < sm->election_timeout() + 1; i++) {
    sm->Tick();
  }

  EXPECT_EQ(sm->state(), kStateFollower);
}

TEST(ElectionTest, TestLeaderSupersedingWithCheckQuorum) {
  auto a = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto b = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto c = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  a->set_check_quorum(true);
  b->set_check_quorum(true);
  c->set_check_quorum(true);

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});

  b->set_randomized_election_timeout(b->election_timeout() + 1);

  for (int i = 0; i < b->election_timeout(); i++) {
    b->Tick();
  }
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  EXPECT_EQ(a->state(), kStateLeader);
  EXPECT_EQ(c->state(), kStateFollower);

  nt.Send({ConsMsg(3, 3, kMsgHup)});

  // peer b rejected c's vote since its election_elapsed had not reached to
  // election_timeout
  EXPECT_EQ(c->state(), kStateCandidate);

  // letting b's election_elapsed reach to election_timeout
  for (int i = 0; i < b->election_timeout(); i++) {
    b->Tick();
  }
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(c->state(), kStateLeader);
}

TEST(ElectionTest, TestLeaderElectionWithCheckQuorum) {
  auto a = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto b = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto c = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  a->set_check_quorum(true);
  b->set_check_quorum(true);
  c->set_check_quorum(true);

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});

  a->set_randomized_election_timeout(a->election_timeout() + 1);
  b->set_randomized_election_timeout(b->election_timeout() + 1);

  // immediately after creation, votes are cast regardless of the election
  // timeout.
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  EXPECT_EQ(a->state(), kStateLeader);
  EXPECT_EQ(c->state(), kStateFollower);

  // need to reset randomized_election_timeout larger than election_timeout
  // again, because the value might be reset to election_timeout since the
  // last state changes.
  a->set_randomized_election_timeout(a->election_timeout() + 1);
  b->set_randomized_election_timeout(b->election_timeout() + 1);
  for (int i = 0; i < a->election_timeout(); i++) {
    a->Tick();
  }
  for (int i = 0; i < b->election_timeout(); i++) {
    b->Tick();
  }
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(a->state(), kStateFollower);
  EXPECT_EQ(c->state(), kStateLeader);
}

// TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher
// term can disrupt the leader even if the leader still "officailly" holds the
// lease. The leader is expected to step down and adopt the candidate's term
TEST(ElectionTest, TestFreeStuckCandidateWithCheckQuorum) {
  auto a = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto b = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto c = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  a->set_check_quorum(true);
  b->set_check_quorum(true);
  c->set_check_quorum(true);

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b), NewSMWithRaft(c)});
  b->set_randomized_election_timeout(b->election_timeout() + 1);

  for (int i = 0; i < b->election_timeout(); i++) {
    b->Tick();
  }
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  nt.Isolate(1);
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(b->state(), kStateFollower);
  EXPECT_EQ(c->state(), kStateCandidate);
  EXPECT_EQ(c->term(), b->term() + 1);

  // Vote again for safety
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(b->state(), kStateFollower);
  EXPECT_EQ(c->state(), kStateCandidate);
  EXPECT_EQ(c->term(), b->term() + 2);

  nt.Recover();
  nt.Send({ConsMsg(1, 3, kMsgHeartbeat, {}, a->term())});

  // Distrupt the leader so that the stuck peer is freed
  EXPECT_EQ(a->state(), kStateFollower);
  EXPECT_EQ(c->term(), a->term());

  // Vote again, should become leader this time
  nt.Send({ConsMsg(3, 3, kMsgHup)});
  EXPECT_EQ(c->state(), kStateLeader);
}

TEST(ElectionTest, TestNonPromotableVoterWithCheckQuorum) {
  auto a = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
  auto b = NewTestRaft(2, 10, 1, NewMSWithPeers({1}));

  a->set_check_quorum(true);
  b->set_check_quorum(true);

  Network nt({NewSMWithRaft(a), NewSMWithRaft(b)});
  b->set_randomized_election_timeout(b->election_timeout() + 1);

  // Need to remove 2 again to make it a non-promotable node since new network
  // overwritten some internal states
  b->RemoveNode(2);

  EXPECT_FALSE(b->Promotable());

  for (int i = 0; i < b->election_timeout(); i++) {
    b->Tick();
  }
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  EXPECT_EQ(a->state(), kStateLeader);
  EXPECT_EQ(b->state(), kStateFollower);
  EXPECT_EQ(b->lead(), 1);
}

// TestDisruptiveFollower tests isolated follower, with slow network incoming
// from leader, election times out to become a candidate with an increased
// term. Then, the candidate's reponse to late leader heartbeat forces the
// leader to step down.
TEST(ElectionTest, TestDisruptiveFollower) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->set_check_quorum(true);
  n2->set_check_quorum(true);
  n3->set_check_quorum(true);

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});

  nt.Send({ConsMsg(1, 1, kMsgHup)});

  // check state
  // n1.state() == kStateLeader
  // n2.state() == kStateFollower
  // n3.staet() == kStateFollower
  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateFollower);

  // etcd server "advanceTicksForElection" on restart; this is to expedite
  // campaign trigger when given larger election timeouts
  // (e.g. multi-datacenter deploy)
  // Or leader messages are being delayed while ticks elapse
  n3->set_randomized_election_timeout(n3->election_timeout() + 2);
  for (int i = 0; i < n3->randomized_election_timeout() - 1; i++) {
    n3->Tick();
  }

  // Ideally, before last election tick elapses, the follower n3 receives
  // kMsgApp or kMsgHeartbeat from leader n1, and then resets its
  // election_elapsed. However, last tick may elapse before receiving any
  // messages from leader, thus triggering campaign
  n3->Tick();

  // n1 is still leader yet
  // while its heartbeat to candidate n3 is being delayed

  // check state
  // n1.state() == kStateleader
  // n2.state() == kStateFollower
  // n3.state() == kStateCandidate
  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateCandidate);

  // check term
  // n1.term() == 2;
  // n2.term() == 2;
  // n3.term() == 3;
  EXPECT_EQ(n1->term(), 2);
  EXPECT_EQ(n2->term(), 2);
  EXPECT_EQ(n3->term(), 3);

  // While outgoing vote requests are still queued in n3, leader heartbeat
  // leader heartbeat finally arrives at candidate n3. However, due to delayed
  // network from leader, leader heartbeat was sent with lower term than
  // candidate's
  nt.Send({ConsMsg(1, 3, kMsgHeartbeat, {}, n1->term())});

  // Then candidate n3 responds with kMsgAppREsp of higher term and leader
  // steps down from a message with higher term this is to disrupt the current
  // leader, so that candidate with higher term can be freed with following
  // election

  // check state
  // n1.state() == kStateFollower
  // n2.state() == kStateFollower
  // n3.state() == kStateCandidate
  EXPECT_EQ(n1->state(), kStateFollower);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateCandidate);

  // check term
  // n1.term() == 3
  // n2.term() == 2
  // n3.term() == 3
  EXPECT_EQ(n1->term(), 3);
  EXPECT_EQ(n2->term(), 2);
  EXPECT_EQ(n3->term(), 3);
}

// TestDisruptiveFollowerPreVote test isolated follower, with slow network
// incoming from leader, election times out to become a pre-candidate with
// less log than current leader. Then pre-vote phase prevents this isolated
// node from forcing current leader to step down, thus less disruptions.
TEST(ElectionTest, TestDisruptiveFollowerPreVote) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->set_check_quorum(true);
  n2->set_check_quorum(true);
  n3->set_check_quorum(true);

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});

  nt.Send({ConsMsg(1, 1, kMsgHup)});

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateFollower);

  nt.Isolate(3);
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});
  n1->set_pre_vote(true);
  n2->set_pre_vote(true);
  n3->set_pre_vote(true);
  nt.Recover();
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStatePreCandidate);

  EXPECT_EQ(n1->term(), 2);
  EXPECT_EQ(n2->term(), 2);
  EXPECT_EQ(n3->term(), 2);

  // delayed leader heartbeat does not force current leader to step down
  nt.Send({ConsMsg(1, 3, kMsgHeartbeat, {}, n1->term())});
  EXPECT_EQ(n1->state(), kStateLeader);
}

// When the leader receives a heartbeat tick, it should send a
// kMsgHeartbeat with m.index() = 0, m.log_term() = 0 and emtry entries.
TEST(ElectionTest, TestBcastBeat) {
  uint64_t offset = 1000;
  // make a state machine with log.offset = 1000
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  cs.add_voters(3);
  auto s = NewSnap(offset, 1, "", &cs);
  auto storage = NewMSWithEnts({});
  storage->ApplySnapshot(*s);
  auto sm = NewTestRaft(1, 10, 1, storage);
  sm->set_term(1);

  sm->BecomeCandidate();
  sm->BecomeLeader();
  for (uint64_t i = 0; i < 10; i++) {
    std::vector<Entry> ents;
    auto ent = ConsEnt(i + 1);
    ents.push_back(ent);
    EXPECT_TRUE(sm->AppendEntry(ents));
  }

  // slow follower
  auto pr = sm->tracker()->mutable_progress(2);
  pr->set_match(5);
  pr->set_next(6);
  // normal follower
  pr = sm->tracker()->mutable_progress(3);
  pr->set_match(sm->raft_log()->LastIndex());
  pr->set_next(sm->raft_log()->LastIndex() + 1);

  sm->Step({ConsMsg(0, 0, kMsgBeat)});
  std::vector<Message> msgs;
  sm->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 2);
  std::map<uint64_t, uint64_t> want_commit_map;
  want_commit_map[2] = std::min(sm->raft_log()->committed(),
                                sm->tracker()->mutable_progress(2)->match());
  want_commit_map[3] = std::min(sm->raft_log()->committed(),
                                sm->tracker()->mutable_progress(3)->match());

  int i = 0;
  for (const auto& m : msgs) {
    EXPECT_EQ(m.type(), kMsgHeartbeat) << "i: " << i;
    EXPECT_EQ(m.index(), 0) << "i: " << i;
    EXPECT_EQ(m.log_term(), 0) << "i: " << i;

    uint64_t to = 0;
    if (want_commit_map.find(m.to()) != want_commit_map.end()) {
      to = want_commit_map[m.to()];
    }
    if (to == 0) {
    }
    EXPECT_NE(to, 0) << "i: " << i;
    EXPECT_EQ(m.commit(), to) << "i: " << i;
    EXPECT_EQ(m.entries().size(), 0) << "i: " << i;

    i++;
  }
}

// tests the output of the state machine when receiving kMsgBeat
TEST(ElectionTest, TestRecvMsgBeat) {
  struct TestArgs {
    RaftStateType state;
    int wmsg;
  } tests[] = {
      {kStateLeader, 2},
      // candidate and follower should ignore kMsgBeat
      {kStateCandidate, 0},
      {kStateFollower, 0},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto ms = NewMSWithPeers({1, 2, 3});
    ms->Append({NewEnt(1, 0), NewEnt(2, 1)});
    auto sm = NewTestRaft(1, 10, 1, ms);
    sm->set_term(1);
    sm->set_state(tt.state);
    sm->set_step_func();
    sm->Step(ConsMsg(1, 1, kMsgBeat));

    std::vector<Message> msgs;
    sm->ReadMessages(&msgs);
    EXPECT_EQ(msgs.size(), tt.wmsg) << "i: " << i;
    for (const auto& m : msgs) {
      EXPECT_EQ(m.type(), kMsgHeartbeat) << "i: " << i;
    }

    i++;
  }
}

TEST(ElectionTest, TestLeaderIncreaseNext) {
  std::vector<EntryPtr> prev_ents({NewEnt(1, 1), NewEnt(1, 2), NewEnt(3, 1)});
  struct TestArgs {
    // progress
    ProgressState state;
    uint64_t next;

    uint64_t wnext;
  } tests[] = {
      // state replicate, optimistically increase next
      // previous entries + noop entry + propose + 1
      {kStateReplicate, 2, prev_ents.size() + 3},
      // state probe, not optimistically increase next
      {kStateProbe, 2, 2},
  };

  int i = 0;
  for (const auto& tt : tests) {
    auto sm = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2}));
    sm->raft_log()->Append(prev_ents);
    sm->BecomeCandidate();
    sm->BecomeLeader();
    sm->tracker()->mutable_progress(2)->set_state(tt.state);
    sm->tracker()->mutable_progress(2)->set_next(tt.next);
    sm->Step(ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")}));

    auto p = sm->tracker()->mutable_progress(2);
    EXPECT_EQ(p->next(), tt.wnext) << "i: " << i;

    i++;
  }
}

void TestCampaignWhileLeader(bool pre_vote) {
  auto cfg = ConsConfig(1, 5, 1, NewMSWithPeers({1}));
  cfg.pre_vote = pre_vote;
  auto r = NewTestRaft(cfg);
  EXPECT_EQ(r->state(), kStateFollower);

  // We don't call Campaign() directly because it comes after the check for our
  // current state.
  r->Step(ConsMsg(1, 1, kMsgHup));
  EXPECT_EQ(r->state(), kStateLeader);
  auto term = r->term();
  r->Step(ConsMsg(1, 1, kMsgHup));
  EXPECT_EQ(r->state(), kStateLeader);
  EXPECT_EQ(r->term(), term);
}

TEST(ElectionTest, TestCampaignWhileLeaderWithoutPrevote) {
  TestCampaignWhileLeader(false);
}

TEST(ElectionTest, TestCampaignWhileLeaderWithPrevote) {
  TestCampaignWhileLeader(true);
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario when a node
// taht has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away. Previously the
// cluster would come to a standstill when run with PreVote enabled.
TEST(ElectionTest, TestNodeWithSmallerTermCanCompleteElection) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  n1->set_pre_vote(true);
  n2->set_pre_vote(true);
  n3->set_pre_vote(true);

  // cause a network partition to isolate node 3
  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});
  nt.Cut(1, 3);
  nt.Cut(2, 3);

  nt.Send({ConsMsg(1, 1, kMsgHup)});

  auto sm = nt.peer(1)->raft();
  EXPECT_EQ(sm->state(), kStateLeader);

  sm = nt.peer(2)->raft();
  EXPECT_EQ(sm->state(), kStateFollower);

  nt.Send({ConsMsg(3, 3, kMsgHup)});
  sm = nt.peer(3)->raft();
  EXPECT_EQ(sm->state(), kStatePreCandidate);

  nt.Send({ConsMsg(2, 2, kMsgHup)});

  sm = nt.peer(1)->raft();
  EXPECT_EQ(sm->term(), 3);
  EXPECT_EQ(n2->term(), 3);
  EXPECT_EQ(n3->term(), 1);
  EXPECT_EQ(n1->state(), kStateFollower);
  EXPECT_EQ(n2->state(), kStateLeader);
  EXPECT_EQ(n3->state(), kStatePreCandidate);

  // recover the network then immediately isolate b which is currently the
  // leader, this is emulate the crash of b.
  nt.Recover();
  nt.Cut(2, 1);
  nt.Cut(2, 3);

  // call for election
  nt.Send({ConsMsg(3, 3, kMsgHup)});
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  // do we have a leader?
  EXPECT_FALSE(n1->state() != kStateLeader && n3->state() != kStateLeader);
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
TEST(ElectionTest, TestPreVoteWithSplitVote) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  n1->set_pre_vote(true);
  n2->set_pre_vote(true);
  n3->set_pre_vote(true);

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  // simulate leader down. followers start split vote.
  nt.Isolate(1);
  nt.Send({ConsMsg(2, 2, kMsgHup), ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(n2->term(), 3);
  EXPECT_EQ(n3->term(), 3);

  EXPECT_EQ(n2->state(), kStateCandidate);
  EXPECT_EQ(n3->state(), kStateCandidate);

  // node 2 election timeout first
  nt.Send({ConsMsg(2, 2, kMsgHup)});

  EXPECT_EQ(n2->term(), 4);
  EXPECT_EQ(n3->term(), 4);

  EXPECT_EQ(n2->state(), kStateLeader);
  EXPECT_EQ(n3->state(), kStateFollower);
}

// TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate
// it will check_quorum correctly.
TEST(ElectionTest, TestPreVoteWithCheckQuorum) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  n1->set_pre_vote(true);
  n2->set_pre_vote(true);
  n3->set_pre_vote(true);

  n1->set_check_quorum(true);
  n2->set_check_quorum(true);
  n3->set_check_quorum(true);

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  nt.Isolate(1);

  // check state
  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateFollower);

  // node 2 will ignore node 3's prevote
  nt.Send({ConsMsg(3, 3, kMsgHup)});
  nt.Send({ConsMsg(2, 2, kMsgHup)});

  // Do we have a leader?
  EXPECT_FALSE(n2->state() != kStateLeader && n3->state() == kStateFollower);
}

// TestLearnerCampaign verifies that a learner won't campaign even if it
// receives a kMsgHup or kMsgTimeoutNow
TEST(ElectionTest, TestLearnerCampaign) {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1}));
  n1->AddNode(2, true);
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1}));
  n2->AddNode(2, true);
  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2)});
  nt.Send({ConsMsg(2, 2, kMsgHup)});

  EXPECT_TRUE(n2->is_learner());
  EXPECT_EQ(n2->state(), kStateFollower);

  nt.Send({ConsMsg(1, 1, kMsgHup)});
  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n1->lead(), 1);

  // NB: TransferLeader already checks that the recipient is not a learner, but
  // the check could have happened by the time the recipient becomes a learner,
  // in which case, it will receive kMsgTimeoutNow as in this test case
  // and we verify that it's ignored.
  nt.Send({ConsMsg(1, 2, kMsgTimeoutNow)});

  EXPECT_EQ(n2->state(), kStateFollower);
}

// simulate rolling update a cluster for pre-vote. Cluster has 3 nodes
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
Network NewPreVoteMigrationCluster() {
  auto n1 = NewTestRaft(1, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n2 = NewTestRaft(2, 10, 1, NewMSWithPeers({1, 2, 3}));
  auto n3 = NewTestRaft(3, 10, 1, NewMSWithPeers({1, 2, 3}));

  n1->BecomeFollower(1, kNone);
  n2->BecomeFollower(1, kNone);
  n3->BecomeFollower(1, kNone);

  n1->set_pre_vote(true);
  n2->set_pre_vote(true);

  // We intentionally do not enable prevote for n3, this is done so in order
  // to simulate a rolling restart process where it's possible to have a mixed
  // version cluster with replicas with prevote enabled, and replicas without.

  Network nt({NewSMWithRaft(n1), NewSMWithRaft(n2), NewSMWithRaft(n3)});
  nt.Send({ConsMsg(1, 1, kMsgHup)});

  // cause a network partition to isolate n3.
  nt.Isolate(3);
  nt.Send({ConsMsg(1, 1, kMsgProp, {ConsEnt(0, 0, "somedata")})});
  nt.Send({ConsMsg(3, 3, kMsgHup)});
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStateCandidate);

  EXPECT_EQ(n1->term(), 2);
  EXPECT_EQ(n2->term(), 2);
  EXPECT_EQ(n3->term(), 4);

  n3->set_pre_vote(true);
  nt.Recover();

  return nt;
}

TEST(ElectionTest, TestPreVoteMigrationCanCompleteElection) {
  auto nt = NewPreVoteMigrationCluster();

  auto n2 = nt.peer(2)->raft();
  auto n3 = nt.peer(3)->raft();

  nt.Isolate(1);

  nt.Send({ConsMsg(3, 3, kMsgHup)});
  nt.Send({ConsMsg(2, 2, kMsgHup)});

  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStatePreCandidate);

  nt.Send({ConsMsg(3, 3, kMsgHup)});
  nt.Send({ConsMsg(2, 2, kMsgHup)});

  EXPECT_FALSE(n2->state() != kStateLeader && n3->state() != kStateFollower);
}

TEST(ElectionTest, TestPreVoteMigrationWithFreeStuckPreCandidate) {
  auto nt = NewPreVoteMigrationCluster();

  auto n1 = nt.peer(1)->raft();
  auto n2 = nt.peer(2)->raft();
  auto n3 = nt.peer(3)->raft();

  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStatePreCandidate);

  // pre-vote again for safety
  nt.Send({ConsMsg(3, 3, kMsgHup)});

  EXPECT_EQ(n1->state(), kStateLeader);
  EXPECT_EQ(n2->state(), kStateFollower);
  EXPECT_EQ(n3->state(), kStatePreCandidate);

  nt.Send({ConsMsg(1, 3, kMsgHeartbeat, {}, n1->term())});

  // disrupt the leader so that the stuck peer is freed
  EXPECT_EQ(n1->state(), kStateFollower);
  EXPECT_EQ(n3->term(), n1->term());
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

TEST(ElectionTest, TestConfChangeCheckBeforeCampaign) {
  Network nt({nullptr, nullptr, nullptr});
  auto n1 = nt.peer(1)->raft();
  auto n2 = nt.peer(2)->raft();
  nt.Send({ConsMsg(1, 1, kMsgHup)});
  EXPECT_EQ(n1->state(), kStateLeader);

  // begin to remove the third node
  ConfChange cc;
  cc.set_type(kConfChangeRemoveNode);
  cc.set_node_id(2);
  std::string ccdata;
  cc.SerializeToString(&ccdata);
  auto m = ConsMsg(1, 1, kMsgProp);
  auto e = m.add_entries();
  e->set_type(kEntryConfChange);
  e->set_data(ccdata);
  nt.Send({m});

  // trigger campaign in node 2
  for (int i = 0; i < n2->randomized_election_timeout(); i++) {
    n2->Tick();
  }
  // It's still follower because committed conf change is not applied
  EXPECT_EQ(n2->state(), kStateFollower);

  // Transfer leadership to peer 2.
  nt.Send({ConsMsg(2, 1, kMsgTransferLeader)});
  EXPECT_EQ(n1->state(), kStateLeader);
  // It's still follower because committed conf change is not applied
  EXPECT_EQ(n2->state(), kStateFollower);

  // abort transfer leader
  for (int i = 0; i < n1->election_timeout(); i++) {
    n1->Tick();
  }

  // advance apply
  std::vector<EntryPtr> ents;
  NextEnts(n2, n2->mutable_storage(), &ents);

  // transfer leadership to peer 2 again
  nt.Send({ConsMsg(2, 1, kMsgTransferLeader)});
  EXPECT_EQ(n1->state(), kStateFollower);
  EXPECT_EQ(n2->state(), kStateLeader);

  NextEnts(n1, n1->mutable_storage(), &ents);
  // Trigger campaign in node 2
  for (int i = 0; i < n1->randomized_election_timeout(); i++) {
    n1->Tick();
  }
  EXPECT_EQ(n1->state(), kStateCandidate);
}

}  // namespace jraft
