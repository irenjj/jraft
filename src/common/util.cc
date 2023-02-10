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
#include "util.h"

#include <numeric>

#include "soft_state.h"

namespace jraft {

void LimitSize(uint64_t max_size, std::vector<EntryPtr>* entries) {
  if (entries->empty()) {
    return;
  }

  uint64_t size = (*entries)[0]->ByteSizeLong();
  int limit = 1;
  for (; limit < entries->size(); limit++) {
    size += (*entries)[limit]->ByteSizeLong();
    if (size > max_size) {
      break;
    }
  }
  entries->erase(entries->begin() + limit, entries->end());
}

EntryPtr NewEnt(uint64_t index, uint64_t term, const std::string& data,
                EntryType type) {
  auto e = std::make_shared<Entry>();
  e->set_index(index);
  e->set_term(term);
  e->set_data(data);
  e->set_type(type);

  return e;
}

SnapshotPtr NewSnap(uint64_t index, uint64_t term, const std::string& data,
                    ConfState* cs) {
  auto s = std::make_shared<Snapshot>();
  auto snap_meta = s->mutable_metadata();

  snap_meta->set_index(index);
  snap_meta->set_term(term);

  if (!data.empty()) {
    s->set_data(data);
  }

  if (cs != nullptr) {
    *(snap_meta->mutable_conf_state()) = *cs;
  }

  return s;
}

Inflights ConsIn(size_t start, size_t count, uint64_t bytes, size_t max_size,
                 size_t max_bytes, const std::vector<Inflight>& buffer) {
  Inflights ins(max_size, max_bytes);
  ins.set_start(start);
  ins.set_count(count);
  ins.set_bytes(bytes);
  ins.set_max_size(max_size);
  ins.set_max_bytes(max_bytes);
  ins.set_buffer(buffer);

  return ins;
}

Entry ConsEnt(uint64_t index, uint64_t term, const std::string& data,
              EntryType type) {
  Entry e;
  e.set_index(index);
  e.set_term(term);
  e.set_data(data);
  e.set_type(type);

  return e;
}

Message Msg(uint64_t from, uint64_t to, MessageType type,
            const std::vector<Entry>& ents) {
  Message m;
  m.set_from(from);
  m.set_to(to);
  m.set_type(type);
  for (const auto& e : ents) {
    *(m.add_entries()) = e;
  }

  return m;
}

bool IsSoftStateEqual(const SoftState& st1, const SoftState& st2) {
  return st1.lead() == st2.lead() && st1.state() == st2.state();
}

bool IsHardStateEqual(const HardState& hs1, const HardState& hs2) {
  return hs1.term() == hs2.term() && hs1.vote() == hs2.vote() &&
         hs1.commit() == hs2.commit();
}

bool IsEmptySoftState(const SoftState& st) {
  return st.lead() == 0 && st.state() == kStateFollower;
}

bool IsEmptyHardState(const HardState& hs) {
  HardState empty_hs;
  empty_hs.set_term(0);
  empty_hs.set_vote(0);
  empty_hs.set_commit(0);
  return IsHardStateEqual(hs, empty_hs);
}

bool IsEmptySnap(const Snapshot& sp) { return sp.metadata().index() == 0; }

bool IsEmptySnap(const SnapshotPtr& sp) {
  return sp != nullptr && sp->metadata().index() == 0;
}

MessageType VoteRespMsgType(MessageType msgt) {
  if (msgt == kMsgVote) {
    return kMsgVoteResp;
  }

  if (msgt == kMsgPreVote) {
    return kMsgPreVoteResp;
  }

  JLOG_FATAL << "not a vote message";

  return msgt;
}

uint64_t PayloadsSize(const std::vector<EntryPtr>& entries) {
  return std::accumulate(
      entries.begin(), entries.end(), 0,
      [](int t1, const EntryPtr& p2) { return t1 + p2->data().size(); });
}

bool IsLocalMsg(MessageType type) {
  return type == kMsgHup || type == kMsgBeat || type == kMsgUnreachable ||
         type == kMsgSnapStatus || type == kMsgCheckQuorum;
}

bool IsResponseMsg(MessageType type) {
  return type == kMsgAppResp || type == kMsgVoteResp ||
         type == kMsgHeartbeatResp || type == kMsgUnreachable ||
         type == kMsgPreVoteResp;
}

}  // namespace jraft
