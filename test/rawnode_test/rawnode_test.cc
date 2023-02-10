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
#include "rawnode.h"
#include "ready.h"

namespace jraft {

// TestRawNodeStep ensures that RawNode.Step ignore local message.
TEST(RawNodeTest, TestRawNodeStep) {
  for (int i = 0; i < 17; i++) {
    auto s = NewMSWithPeers({});
    HardState hs;
    hs.set_term(1);
    hs.set_commit(1);
    s->set_hard_state(hs);
    std::vector<EntryPtr> ents({NewEnt(1, 1)});
    s->Append(ents);
    ConfState cs;
    cs.add_voters(1);
    EXPECT_EQ(s->ApplySnapshot(NewSnap(1, 1, "", &cs)), kOk) << "i: " << i;
    // Append an empty entry to make sure the non-local message (like vote
    // requests) are ignored and don't trigger assertions.
    RawNode raw_node(ConsConfig(1, 10, 1, s));
    MessageType msgt = static_cast<MessageType>(i);
    ErrNum err = raw_node.Step(Msg(0, 0, msgt));
    // local_msg should be ignored
    if (IsLocalMsg(msgt)) {
      EXPECT_EQ(err, kErrStepLocalMsg);
    }
  }
}

// TestRawNodeProposeAndConfChange tests the configuration change mechanism.
// Each test case sends a configuration change which is either simple or joint,
// verifies that it applies and that the resulting ConfState matches
// expectations, and for joint configurations make sure that they are exited
// successfully.
TEST(RawNodeTest, TestRawNodeProposeAndConfChange) {
  auto s = NewMSWithPeers({1});
  RawNode raw_node(ConsConfig(1, 10, 1, s));
  auto rd = raw_node.GetReady();
  s->Append(rd->entries());
  raw_node.Advance(rd);

  auto d = raw_node.GetReady();
  EXPECT_TRUE(IsEmptyHardState(d->hard_state()));
  EXPECT_TRUE(d->entries().empty());

  raw_node.Campaign();
  rd = raw_node.GetReady();
  EXPECT_EQ(rd->soft_state().lead(), raw_node.raft().lead());

  // propose a command and a raft::ConfChange
  raw_node.Propose("somedata");
  ConfChange cc;
  cc.set_type(kConfChangeAddNode);
  cc.set_node_id(1);
  std::string ccdata;
  EXPECT_TRUE(cc.SerializeToString(&ccdata));
  raw_node.ProposeConfChange(cc);

  std::vector<EntryPtr> entries;
  raw_node.raft().raft_log()->AllEntries(&entries);
  EXPECT_GE(entries.size(), 3);
  EXPECT_EQ(entries[1]->data(), "somedata");
  EXPECT_EQ(entries[2]->type(), kEntryConfChange);
  EXPECT_EQ(entries[2]->data(), ccdata);
}

void ProposeConfChangeAndApply(RawNode* raw_node, const ConfChange& cc,
                               MemoryStoragePtr s) {
  raw_node->ProposeConfChange(cc);
  auto rd = raw_node->GetReady();
  s->Append(rd->entries());
  for (const auto& e : rd->committed_entries()) {
    if (e->type() == kEntryConfChange) {
      ConfChange cc;
      cc.ParseFromString(e->data());
      raw_node->ApplyConfChange(cc);
    }
  }
  raw_node->Advance(rd);
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same
// node should not affect the later proposal to add new node.
TEST(RawNodeTest, TestRawNodeProposeAddDuplicateNode) {
  auto s = NewMSWithPeers({1});
  RawNode raw_node(ConsConfig(1, 10, 1, s));
  auto rd = raw_node.GetReady();
  s->Append(rd->entries());
  raw_node.Advance(rd);

  raw_node.Campaign();
  while (1) {
    rd = raw_node.GetReady();
    s->Append(rd->entries());
    if (rd->soft_state().lead() == raw_node.raft().id()) {
      raw_node.Advance(rd);
      break;
    }
    raw_node.Advance(rd);
  }

  ConfChange cc1;
  cc1.set_type(kConfChangeAddNode);
  cc1.set_node_id(1);
  std::string cc1data;
  EXPECT_TRUE(cc1.SerializeToString(&cc1data));

  ConfChange cc2;
  cc2.set_type(kConfChangeAddNode);
  cc2.set_node_id(2);
  std::string cc2data;
  EXPECT_TRUE(cc2.SerializeToString(&cc2data));

  ProposeConfChangeAndApply(&raw_node, cc1, s);
  // try to add the same node again
  ProposeConfChangeAndApply(&raw_node, cc1, s);

  // the new node join should be ok
  ProposeConfChangeAndApply(&raw_node, cc2, s);

  uint64_t li = 0;
  EXPECT_EQ(s->LastIndex(&li), kOk);
  uint64_t fi = 0;
  EXPECT_EQ(s->FirstIndex(&fi), kOk);

  // the last three entries should be raft::ConfChange cc1, cc1, cc2
  std::vector<EntryPtr> ents;
  s->Entries(fi, li + 1, kNoLimit, &ents);
  size_t size = ents.size();
  EXPECT_GE(size, 3);
  EXPECT_EQ(ents[size - 3]->data(), cc1data);
  EXPECT_EQ(ents[size - 1]->data(), cc2data);
}

// TestRawNodeStart ensures that a node can be started correctly, and can accept
// and commit proposals.
TEST(RawNodeTest, TestRawNodeStart) {
  auto storage = NewMSWithPeers({1});
  RawNode raw_node(ConsConfig(1, 10, 1, storage));
  raw_node.Campaign();
  auto rd = raw_node.GetReady();
  storage->Append(rd->entries());
  raw_node.Advance(rd);

  raw_node.Propose("foo");
  rd = raw_node.GetReady();
  EXPECT_EQ(rd->entries().size(), rd->committed_entries().size());
  EXPECT_EQ(rd->entries().size(), 1);
  EXPECT_EQ(rd->entries()[0]->data(), rd->committed_entries()[0]->data());
  EXPECT_EQ(rd->entries()[0]->data(), "foo");
  storage->Append(rd->entries());
  raw_node.Advance(rd);
  EXPECT_FALSE(raw_node.HasReady());
}

TEST(RawNodeTest, TestRawNodeRestartFromSnapshot) {
  ConfState cs;
  cs.add_voters(1);
  cs.add_voters(2);
  auto snap = NewSnap(2, 1, "", &cs);
  std::vector<EntryPtr> entries({NewEnt(3, 1, "foo")});
  HardState hs;
  hs.set_term(1);
  hs.set_commit(3);

  Ready want;
  want.set_committed_entries(entries);

  auto s = NewMSWithPeers({});
  s->set_hard_state(hs);
  s->ApplySnapshot(snap);
  s->Append(entries);
  RawNode raw_node(ConsConfig(1, 10, 1, s));
  auto rd = raw_node.GetReady();

  raw_node.Advance(rd);
  EXPECT_FALSE(raw_node.HasReady());
}

}  // namespace jraft
