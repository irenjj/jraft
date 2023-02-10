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
#include "test_util.h"

#include "common/util.h"

namespace jraft {

void PrevoteConfig(Config& c) { c.pre_vote = true; }

bool EntryDeepEqual(const std::vector<EntryPtr>& e1,
                    const std::vector<EntryPtr>& e2) {
  if (e1.size() != e2.size()) {
    return false;
  }

  for (size_t i = 0; i < e1.size(); i++) {
    if (e1[i]->type() != e2[i]->type()) {
      return false;
    }
    if (e1[i]->term() != e2[i]->term()) {
      return false;
    }
    if (e1[i]->index() != e2[i]->index()) {
      return false;
    }
    if (e1[i]->data() != e2[i]->data()) {
      return false;
    }
  }

  return true;
}

bool EntryDeepEqual(const std::vector<Entry>& e1,
                    const std::vector<Entry>& e2) {
  if (e1.size() != e2.size()) {
    return false;
  }

  for (size_t i = 0; i < e1.size(); i++) {
    if (e1[i].type() != e2[i].type()) {
      return false;
    }
    if (e1[i].term() != e2[i].term()) {
      return false;
    }
    if (e1[i].index() != e2[i].index()) {
      return false;
    }
    if (e1[i].data() != e2[i].data()) {
      return false;
    }
  }

  return true;
}

bool EntryDeepEqual(const std::vector<EntryPtr>& e1,
                    const std::vector<Entry>& e2) {
  if (e1.size() != e2.size()) {
    return false;
  }

  for (size_t i = 0; i < e1.size(); i++) {
    if (e1[i]->type() != e2[i].type()) {
      return false;
    }
    if (e1[i]->term() != e2[i].term()) {
      return false;
    }
    if (e1[i]->index() != e2[i].index()) {
      return false;
    }
    if (e1[i]->data() != e2[i].data()) {
      return false;
    }
  }

  return true;
}

bool EntryDeepEqual(const std::vector<Entry>& e1,
                    const std::vector<EntryPtr>& e2) {
  if (e1.size() != e2.size()) {
    return false;
  }

  for (size_t i = 0; i < e1.size(); i++) {
    if (e1[i].type() != e2[i]->type()) {
      return false;
    }
    if (e1[i].term() != e2[i]->term()) {
      return false;
    }
    if (e1[i].index() != e2[i]->index()) {
      return false;
    }
    if (e1[i].data() != e2[i]->data()) {
      return false;
    }
  }

  return true;
}

bool SnapshotDeepEqual(const SnapshotPtr& s1, const SnapshotPtr& s2) {
  if (s1 == nullptr && s2 == nullptr) {
    return true;
  }
  if (s1 == nullptr && s2 != nullptr || s1 != nullptr && s2 == nullptr) {
    return false;
  }

  auto snap_meta1 = s1->metadata();
  auto snap_meta2 = s2->metadata();
  if (snap_meta1.index() != snap_meta2.index()) {
    return false;
  }
  if (snap_meta1.term() != snap_meta2.term()) {
    return false;
  }
  if (s1->data() != s2->data()) {
    return false;
  }

  auto voters1 = snap_meta1.conf_state().voters();
  auto voters2 = snap_meta2.conf_state().voters();
  if (voters1.size() != voters2.size()) {
    return false;
  }
  for (int i = 0; i < voters1.size(); i++) {
    if (voters1[i] != voters2[i]) {
      return false;
    }
  }

  return true;
}

bool InflightsDeepEqual(const Inflights& in1, const Inflights& in2) {
  if (in1.start() != in2.start() || in1.count() != in2.count() ||
      in1.bytes() != in2.bytes() || in1.max_size() != in2.max_size() ||
      in1.max_bytes() != in2.max_bytes()) {
    return false;
  }

  std::vector<Inflight> b1 = in2.buffer();
  std::vector<Inflight> b2 = in2.buffer();
  if (b1.size() != b2.size()) {
    return false;
  }

  for (size_t i = 0; i < b1.size(); i++) {
    if (b1[i].index != b2[i].index || b1[i].bytes != b2[i].bytes) {
      return false;
    }
  }

  return true;
}

bool MsgDeepEqual(const std::vector<Message>& ms1,
                  const std::vector<Message>& ms2) {
  if (ms1.size() != ms2.size()) {
    return false;
  }

  for (size_t i = 0; i < ms1.size(); i++) {
    Message m1 = ms1[i];
    Message m2 = ms2[i];
    if (m1.type() != m2.type()) {
      JLOG_INFO << "type not same";
      return false;
    }
    if (m1.to() != m2.to()) {
      JLOG_INFO << "to not same";
      return false;
    }
    if (m1.from() != m2.from()) {
      JLOG_INFO << "from not same";
      return false;
    }
    if (m1.term() != m2.term()) {
      return false;
    }
    if (m1.log_term() != m2.log_term()) {
      return false;
    }
    if (m1.index() != m2.index()) {
      return false;
    }
    if (m1.entries().size() != m2.entries().size()) {
      return false;
    }
    for (int j = 0; j < m1.entries().size(); j++) {
      Entry e1 = m1.entries(j);
      Entry e2 = m2.entries(j);
      if (e1.type() != e2.type()) {
        return false;
      }
      if (e1.term() != e2.term()) {
        return false;
      }
      if (e1.index() != e2.index()) {
        return false;
      }
      if (e1.data() != e2.data()) {
        return false;
      }
    }
    if (m1.commit() != m2.commit()) {
      return false;
    }
    if (m1.reject() != m2.reject()) {
      return false;
    }
    if (m1.reject_hint() != m2.reject_hint()) {
      return false;
    }
    if (m1.context() != m2.context()) {
      return false;
    }
    if (m1.snapshot().data() != m2.snapshot().data()) {
      return false;
    }
    auto meta1 = m1.snapshot().metadata();
    auto meta2 = m2.snapshot().metadata();
    auto cs1 = meta1.conf_state();
    auto cs2 = meta2.conf_state();
    if (cs1.voters().size() != cs2.voters().size()) {
      return false;
    }
    for (size_t j = 0; j < cs1.voters().size(); j++) {
      if (cs1.voters(j) != cs2.voters(j)) {
        return false;
      }
    }
    if (cs1.learners().size() != cs2.learners().size()) {
      return false;
    }
    for (size_t j = 0; j < cs1.learners().size(); j++) {
      if (cs1.learners(j) != cs2.learners(j)) {
        return false;
      }
    }
    if (cs1.voters_outgoing().size() != cs2.voters_outgoing().size()) {
      return false;
    }
    for (size_t j = 0; j < cs1.voters_outgoing().size(); j++) {
      if (cs1.voters_outgoing(j) != cs2.voters_outgoing(j)) {
        return false;
      }
    }
    if (cs1.auto_leave() != cs2.auto_leave()) {
      return false;
    }
    if (meta1.index() != meta2.index()) {
      return false;
    }
    if (meta1.term() != meta2.term()) {
      return false;
    }
  }

  return true;
}

bool RaftLogDeepEqual(const RaftLog& l1, const RaftLog& l2) {
  if (l1.committed() != l2.committed()) {
    return false;
  }
  if (l1.applied() != l2.applied()) {
    return false;
  }
  if (l1.max_next_ents_size() != l2.max_next_ents_size()) {
    return false;
  }
  std::vector<EntryPtr> ents1;
  l1.AllEntries(&ents1);
  std::vector<EntryPtr> ents2;
  l2.AllEntries(&ents2);
  if (ents1.size() != ents2.size()) {
    return false;
  }
  for (size_t i = 0; i < ents1.size(); i++) {
    auto e1 = ents1[i];
    auto e2 = ents2[i];
    if (e1->index() != e2->index()) {
      return false;
    }
    if (e1->term() != e2->term()) {
      return false;
    }
    if (e1->type() != e2->type()) {
      return false;
    }
    if (e1->data() != e2->data()) {
      return false;
    }
  }

  return true;
}

void IdsBySize(uint64_t size, std::vector<uint64_t>* ids) {
  ids->clear();
  ids->resize(size);
  for (int i = 0; i < size; i++) {
    (*ids)[i] = i + 1;
  }
}

std::vector<uint64_t> IdsBySize(uint64_t size) {
  std::vector<uint64_t> ids;
  ids.resize(size);
  for (int i = 0; i < size; i++) {
    ids[i] = i + 1;
  }

  return ids;
}

MemoryStoragePtr NewMSWithPeers(const std::vector<uint64_t>& peers) {
  auto ms = std::make_shared<MemoryStorage>();
  auto mutable_voters = ms->mutable_snapshot()
                            ->mutable_metadata()
                            ->mutable_conf_state()
                            ->mutable_voters();
  for (const auto& p : peers) {
    mutable_voters->Add(p);
  }

  return ms;
}

RaftPtr NewTestRaft(uint64_t id, int election, int heartbeat,
                    const StoragePtr& storage) {
  auto c = ConsConfig(id, election, heartbeat, storage);
  auto r = std::make_shared<Raft>(&c);

  return r;
}

RaftPtr NewTestRaft(Config c) {
  auto r = std::make_shared<Raft>(&c);

  return r;
}

StateMachinePtr NewSMWithRaft(RaftPtr r) {
  return std::make_shared<StateMachine>(kRaftType, r);
}

StateMachinePtr NewBlackHole() {
  auto sm = std::make_shared<StateMachine>();
  return sm;
}

StateMachinePtr NewSMWithEnts(const std::vector<uint64_t>& terms,
                              const ConfigFunc& cfg_func) {
  auto ms = std::make_shared<MemoryStorage>();
  for (int i = 0; i < terms.size(); i++) {
    auto e = NewEnt(static_cast<uint64_t>(i + 1), terms[i]);
    ms->Append({e});
  }
  auto cfg = ConsConfig(1, 5, 1, ms);
  if (cfg_func != nullptr) {
    cfg_func(cfg);
  }
  auto r = std::make_shared<Raft>(&cfg);
  r->Reset(terms.back());
  auto sm = std::make_shared<StateMachine>(kRaftType, r);
  return sm;
}

MemoryStoragePtr NewMSWithPL(const std::vector<uint64_t>& peers,
                             const std::vector<uint64_t>& learners) {
  auto ms = std::make_shared<MemoryStorage>();
  auto mutable_voters = ms->mutable_snapshot()
                            ->mutable_metadata()
                            ->mutable_conf_state()
                            ->mutable_voters();
  auto mutable_learners = ms->mutable_snapshot()
                              ->mutable_metadata()
                              ->mutable_conf_state()
                              ->mutable_learners();

  for (const auto& p : peers) {
    mutable_voters->Add(p);
  }
  for (const auto& l : learners) {
    mutable_learners->Add(l);
  }

  return ms;
}

MemoryStoragePtr NewMSWithEnts(const std::vector<EntryPtr>& ents) {
  auto ms = std::make_shared<MemoryStorage>();
  ms->Append(ents);

  return ms;
}

Config ConsConfig(uint64_t id, int election, int heartbeat,
                  const StoragePtr& storage) {
  Config c;
  c.id = id;
  c.election_tick = election;
  c.heartbeat_tick = heartbeat;
  c.storage = storage;
  c.max_size_per_msg = kNoLimit;
  c.max_inflight_msgs = 256;

  return c;
}

Message ConsMsg(uint64_t from, uint64_t to, MessageType type,
                const std::vector<Entry>& ents, uint64_t term,
                uint64_t log_term, uint64_t index, uint64_t commit,
                const std::string& ctx) {
  Message m;
  m.set_from(from);
  m.set_to(to);
  m.set_type(type);
  for (const auto& e : ents) {
    *(m.add_entries()) = e;
  }
  m.set_term(term);
  m.set_log_term(log_term);
  m.set_index(index);
  m.set_commit(commit);
  m.set_context(ctx);

  return m;
}

void CommitNoopEntry(const RaftPtr& r, const MemoryStoragePtr& s) {
  if (r->state() != kStateLeader) {
    JLOG_FATAL << "it should only be used when it is the leader";
  }

  r->BcastAppend();
  // simulate the response of kMsgApp
  std::vector<Message> msgs;
  r->ReadMessages(&msgs);
  for (const auto& m : msgs) {
    if (m.type() != kMsgApp || m.entries().size() != 1 ||
        !m.entries(0).data().empty()) {
      JLOG_FATAL << "not a message to append noop entry";
    }
    r->Step(AcceptAndReply(m));
  }

  // ignore further messages to refresh follower's commit index
  r->ReadMessages();
  std::vector<EntryPtr> unstable_ents;
  r->raft_log()->UnstableEntries(&unstable_ents);
  s->Append(unstable_ents);
  r->raft_log()->AppliedTo(r->raft_log()->committed());
  r->raft_log()->StableTo(r->raft_log()->LastIndex(),
                          r->raft_log()->LastTerm());
}

Message AcceptAndReply(const Message& m) {
  if (m.type() != kMsgApp) {
    JLOG_FATAL << "type should be raft::kMsgApp";
  }

  Message resp_msg = ConsMsg(m.to(), m.from(), kMsgAppResp, {}, m.term(), 0,
                             m.index() + m.entries_size());
  return resp_msg;
}

}  // namespace jraft
