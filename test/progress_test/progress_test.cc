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
#include "tracker/progress.h"
#include "raft.h"

namespace jraft {

TEST(ProgressTest, TestProgressIsPaused) {
  struct TestArgs {
    ProgressState state;
    bool paused;

    bool w;
  } tests[] = {
      {
          kStateProbe,
          false,
          false,
      },
      {
          kStateProbe,
          true,
          true,
      },
      {
          kStateReplicate,
          false,
          false,
      },
      {
          kStateReplicate,
          true,
          true,
      },
      {
          kStateSnapshot,
          false,
          true,
      },
      {
          kStateSnapshot,
          true,
          true,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    Progress p(0, 1, 256);
    p.set_state(tt.state);
    p.set_msg_app_flow_paused(tt.paused);
    EXPECT_EQ(p.IsPaused(), tt.w) << "i: " << i;

    i++;
  }
}

// TestProgressResume ensures that MaybeDecrTo will reset probe_sent_.
TEST(ProgressTest, TestProgressResume) {
  Progress p;
  p.set_next(2);
  p.set_msg_app_flow_paused(true);
  p.MaybeDecrTo(1, 1);
  EXPECT_FALSE(p.msg_app_flow_paused());

  p.set_msg_app_flow_paused(true);
  p.MaybeUpdate(2);
  EXPECT_FALSE(p.msg_app_flow_paused());
}

TEST(ProgressTest, TestProgressBecomeProbe) {
  uint64_t match = 1;
  Progress p1(5, 256);
  p1.set_state(kStateReplicate);
  p1.set_match(match);
  Progress p2(5, 256);
  p2.set_state(kStateSnapshot);
  p2.set_match(match);
  p2.set_pending_snapshot(10);
  Progress p3(5, 256);
  p3.set_state(kStateSnapshot);
  p3.set_match(match);
  p3.set_pending_snapshot(0);
  struct TestArgs {
    Progress *p;

    uint64_t wnext;
  } tests[] = {
      {
          &p1,
          2,
      },
      {
          &p2,
          11,
      },
      {
          &p3,
          2,
      },
  };

  int i = 0;
  for (auto& tt : tests) {
    tt.p->BecomeProbe();
    EXPECT_EQ(tt.p->state(), kStateProbe) << "i: " << i;
    EXPECT_EQ(tt.p->match(), match) << "i: " << i;
    EXPECT_EQ(tt.p->next(), tt.wnext) << "i: " << i;

    i++;
  }
}

TEST(ProgressTest, TestProgressBecomeReplicate) {
  Progress p(5, 256);
  p.set_state(kStateProbe);
  p.set_match(1);
  p.BecomeReplicate();

  EXPECT_EQ(p.state(), kStateReplicate);
  EXPECT_EQ(p.match(), 1);
  EXPECT_EQ(p.match() + 1, p.next());
}

TEST(ProgressTest, TestProgressBecomeSnapshot) {
  Progress p(5, 256);
  p.set_state(kStateProbe);
  p.set_match(1);
  p.BecomeSnapshot(10);

  EXPECT_EQ(p.state(), kStateSnapshot);
  EXPECT_EQ(p.match(), 1);
  EXPECT_EQ(p.pending_snapshot(), 10);
}

TEST(ProgressTest, TestProgressUpdate) {
  uint64_t prev_m = 3;
  uint64_t prev_n = 5;
  struct TestArgs {
    uint64_t update;

    uint64_t wm;
    uint64_t wn;
    uint64_t wok;
  } tests[] = {
      {
          prev_m - 1,
          prev_m,
          prev_n,
          false,
      },  // do not decrease match, next
      {
          prev_m,
          prev_m,
          prev_n,
          false,
      },  // do not decrease next
      {
          prev_m + 1,
          prev_m + 1,
          prev_n,
          true,
      },  // increase match, do not decrease next
      {
          prev_m + 2,
          prev_m + 2,
          prev_n + 1,
          true,
      },  // increase match, next
  };

  int i = 0;
  for (const auto& tt : tests) {
    Progress p;
    p.set_match(prev_m);
    p.set_next(prev_n);

    bool ok = p.MaybeUpdate(tt.update);
    EXPECT_EQ(ok, tt.wok) << "i: " << i;
    EXPECT_EQ(p.match(), tt.wm) << "i: " << i;
    EXPECT_EQ(p.next(), tt.wn) << "i: " << i;

    i++;
  }
}

TEST(ProgressTest, TestProgressMaybeDecr) {
  struct TestArgs {
    ProgressState state;
    uint64_t m;
    uint64_t n;
    uint64_t rejected;
    uint64_t last;

    bool w;
    uint64_t wn;
  } tests[] = {
      // state replicate and rejected is not greater than match
      {
          kStateReplicate,
          5,
          10,
          5,
          5,
          false,
          10,
      },
      // state replicate and rejected is not greater than match
      {
          kStateReplicate,
          5,
          10,
          4,
          4,
          false,
          10,
      },
      // state replicate and rejected is greater than match
      // directly descrease to match + 1
      {
          kStateReplicate,
          5,
          10,
          9,
          9,
          true,
          6,
      },
      // next - 1 != rejected is always false
      {
          kStateProbe,
          0,
          0,
          0,
          0,
          false,
          0,
      },
      // next - 1 != rejected is always false
      {
          kStateProbe,
          0,
          10,
          5,
          5,
          false,
          10,
      },
      // next > 1 = decremented by 1
      {
          kStateProbe,
          0,
          10,
          9,
          9,
          true,
          9,
      },
      // next > 1 = decremented by 1
      {
          kStateProbe,
          0,
          2,
          1,
          1,
          true,
          1,
      },
      // next <= 1 = reset to 1
      {
          kStateProbe,
          0,
          1,
          0,
          0,
          true,
          1,
      },
      // decrease to min(rejected, last + 1)
      {
          kStateProbe,
          0,
          10,
          9,
          2,
          true,
          3,
      },
      // rejected < 1, reset to 1
      {
          kStateProbe,
          0,
          10,
          9,
          0,
          true,
          1,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    Progress p;
    p.set_state(tt.state);
    p.set_match(tt.m);
    p.set_next(tt.n);

    EXPECT_EQ(p.MaybeDecrTo(tt.rejected, tt.last), tt.w) << "i: " << i;
    EXPECT_EQ(p.match(), tt.m) << "i: " << i;
    EXPECT_EQ(p.next(), tt.wn) << "i: " << i;
  }
}

TEST(ProgressTest, TestProgressLeader) {
  auto ms = NewMSWithPeers({1, 2});
  RaftPtr r = NewTestRaft(1, 5, 1, ms);

  r->BecomeCandidate();
  r->BecomeLeader();
  r->tracker()->mutable_progress(2)->BecomeReplicate();

  // Send proposals to r1. The first 5 entries should be appended to the log.
  Message prop_msg;
  Entry e;
  e.set_data("foo");
  prop_msg = ConsMsg(1, 1, kMsgProp, {e});

  for (int i = 0; i < 5; i++) {
    auto pr = r->tracker()->mutable_progress(r->id());
    EXPECT_EQ(pr->state(), kStateReplicate) << "i: " << i;
    EXPECT_EQ(pr->match(), static_cast<uint64_t>(i + 1)) << "i: " << i;
    EXPECT_EQ(pr->next(), pr->match() + 1) << "i: " << i;
    EXPECT_EQ(r->Step(prop_msg), kOk) << "i: " << i;
  }
}

// TestProgressResumeByHeartbeatResp ensures Heartbeat reset progress.paused
// by heart response.
TEST(ProgressTest, TestProgressResumeByHeartbeatResp) {
  auto ms = NewMSWithPeers({1, 2});
  auto r = NewTestRaft(1, 5, 1, ms);

  auto tk = r->tracker();
  tk->mutable_progress(2)->set_msg_app_flow_paused(true);

  Message m1;
  m1 = ConsMsg(1, 1, kMsgBeat, {});
  r->Step(m1);
  EXPECT_TRUE(tk->mutable_progress(2)->msg_app_flow_paused());

  tk->mutable_progress(2)->BecomeReplicate();
  Message m2;
  m2 = ConsMsg(2, 1, kMsgHeartbeatResp, {});
  EXPECT_FALSE(tk->mutable_progress(2)->msg_app_flow_paused());
}

TEST(ProgressTest, TestProgressPaused) {
  auto ms = NewMSWithPeers({1, 2});
  auto r = NewTestRaft(1, 5, 1, ms);

  r->BecomeCandidate();
  r->BecomeLeader();
  Entry e;
  e.set_data("somedata");
  Message m1 = ConsMsg(1, 1, kMsgProp, {e});
  Message m2 = ConsMsg(1, 1, kMsgProp, {e});
  Message m3 = ConsMsg(1, 1, kMsgProp, {e});
  r->Step(m1);
  r->Step(m2);
  r->Step(m3);

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 1);
}

TEST(ProgressTest, TestProgressFlowControl) {
  auto ms = NewMSWithPeers({1, 2});
  Config c = ConsConfig(1, 5, 1, ms);
  c.max_inflight_msgs = 3;
  c.max_size_per_msg = 2048;
  Raft *r = new Raft(&c);

  r->BecomeCandidate();
  r->BecomeLeader();

  // Throw away all messages relating to the initial election.
  std::vector<Message> dummy;
  r->ReadMessages(&dummy);

  // While node 2 is in probe state, propose a bunch of entries.
  r->tracker()->mutable_progress(2)->BecomeProbe();
  std::string blob(1000, 'a');
  for (int i = 0; i < 10; i++) {
    Entry e;
    e.set_data(blob);
    Message m = ConsMsg(1, 1, kMsgProp, {e});
    r->Step(m);
  }

  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  // First append has two entries: the empty entry to confirm the election,
  // and the first proposal (only on proposal gets sent because we're in probe
  // state).
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].type(), kMsgApp);
  EXPECT_EQ(msgs[0].entries().size(), 2);
  EXPECT_TRUE(msgs[0].entries(0).data().empty());
  EXPECT_EQ(msgs[0].entries(1).data().size(), 1000);

  // When this Append is acked, we change to replicate state and can send
  // multiple messages at once.
  Message m = ConsMsg(2, 1, kMsgAppResp, {});
  m.set_index(msgs[0].entries(1).index());
  r->Step(m);
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 3);
  int i = 0;
  for (const auto& msg : msgs) {
    EXPECT_EQ(msg.type(), kMsgApp) << "i: " << i;
    EXPECT_EQ(msg.entries().size(), 2) << "i: " << i;
    i++;
  }

  // Ack all three of these messages together and get the last two messages
  // (containing three entries).
  Message m2 = ConsMsg(2, 1, kMsgAppResp, {});
  m2.set_index(msgs[2].entries(1).index());
  r->Step(m2);
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 2);
  i = 0;
  for (const auto& msg : msgs) {
    EXPECT_EQ(msg.type(), kMsgApp) << "i: " << i;
    i++;
  }
  EXPECT_EQ(msgs[0].entries().size(), 2);
  EXPECT_EQ(msgs[1].entries().size(), 1);

  delete r;
}

TEST(ProgressTest, TestUncommittedEntryLimit) {
  // Use a relatively large number of entries here to prevent regression of a
  // bug which computed the size before it was fixed. This test would fail with
  // the bug, either because we'd get dropped proposals earlier than we expect
  // them, or because the final tally sends up nonzero. (At the time of
  // writting, the former).
  const uint64_t max_entries = 1024;
  Entry test_entry;
  test_entry.set_data("testdata");
  uint64_t max_entry_size = max_entries * test_entry.data().size();

  auto ms = NewMSWithPeers({1, 2, 3});
  Config cfg = ConsConfig(1, 5, 1, ms);
  cfg.max_uncommitted_entries_size = max_entry_size;
  cfg.max_inflight_msgs = 2 * 1024;  // avoid interference
  auto r = NewTestRaft(cfg);

  r->BecomeCandidate();
  r->BecomeLeader();
  EXPECT_EQ(r->uncommitted_size(), 0);

  // Set the two followers to the replicate state. Commit to tail of log.
  const uint64_t num_followers = 2;
  auto tk = r->tracker();
  tk->mutable_progress(2)->BecomeReplicate();
  tk->mutable_progress(3)->BecomeReplicate();
  r->set_uncommitted_size(0);

  // Send proposals to r1. The first 5 entries should be appended to the log.
  std::vector<Entry> prop_ents(max_entries);
  Message prop_msg = ConsMsg(1, 1, kMsgProp, {test_entry});
  for (uint64_t i = 0; i < max_entries; i++) {
    EXPECT_EQ(r->Step(prop_msg), kOk) << "i: " << i;
    prop_ents[i] = test_entry;
  }

  // Send one more proposal to r1. It should be rejected.
  EXPECT_EQ(r->Step(prop_msg), kErrProposalDropped);

  // Read messages and reduce the uncommitted size as if we had committed
  // these entries.
  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), max_entries * num_followers);
  r->ReduceUncommittedSize(prop_ents);
  EXPECT_EQ(r->uncommitted_size(), 0);

  // Send a single large proposal to r1. Should be accepted even though it
  // pushes us above the limit because we were beneath it before the proposal.
  prop_ents.clear();
  prop_ents.resize(2 * max_entries);
  for (uint64_t i = 0; i < 2 * max_entries; i++) {
    prop_ents[i] = test_entry;
  }
  Message prop_msg_large = ConsMsg(1, 1, kMsgProp, prop_ents);
  EXPECT_EQ(r->Step(prop_msg_large), kOk);

  // Send one more proposal to r1. It should be rejected again.
  EXPECT_EQ(r->Step(prop_msg), kErrProposalDropped);

  // But we can always append an entry with no Data. This is used both for
  // the leader's first empty entry and for auto-transitioning out of joint
  // config states.
  Entry empty;
  Message m = ConsMsg(1, 1, kMsgProp, {empty});
  EXPECT_EQ(r->Step(m), kOk);

  // Read Messages and reduce the uncommitted size as if we had committed
  // these entries.
  r->ReadMessages(&msgs);
  EXPECT_EQ(msgs.size(), 2 * num_followers);
  r->ReduceUncommittedSize(prop_ents);
  EXPECT_EQ(r->uncommitted_size(), 0);
}

}  // namespace jraft
