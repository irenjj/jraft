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
#pragma once

#include <vector>

#include "common/conf.h"
#include "memory_storage.h"
#include "raft.h"
#include "state_machine.h"
#include "tracker/inflights.h"

namespace jraft {

void PrevoteConfig(Config& c);

using ConfigFunc = std::function<void(Config& cfg)>;

bool EntryDeepEqual(const std::vector<EntryPtr>& e1,
                    const std::vector<EntryPtr>& e2);
bool EntryDeepEqual(const std::vector<Entry>& e1, const std::vector<Entry>& e2);
bool EntryDeepEqual(const std::vector<EntryPtr>& e1,
                    const std::vector<Entry>& e2);
bool EntryDeepEqual(const std::vector<Entry>& e1,
                    const std::vector<EntryPtr>& e2);
bool SnapshotDeepEqual(const SnapshotPtr& s1, const SnapshotPtr& s2);
bool InflightsDeepEqual(const Inflights& in1, const Inflights& in2);
bool MsgDeepEqual(const std::vector<Message>& ms1,
                  const std::vector<Message>& ms2);
bool RaftLogDeepEqual(const RaftLog& l1, const RaftLog& l2);

void IdsBySize(uint64_t size, std::vector<uint64_t>* ids);
std::vector<uint64_t> IdsBySize(uint64_t size);

MemoryStoragePtr NewMSWithPeers(const std::vector<uint64_t>& peers = {});
RaftPtr NewTestRaft(uint64_t id, int election, int heartbeat,
                    const StoragePtr& storage);
RaftPtr NewTestRaft(Config c);
StateMachinePtr NewSMWithRaft(RaftPtr r);
StateMachinePtr NewBlackHole();
StateMachinePtr NewSMWithEnts(const std::vector<uint64_t>& terms,
                              const ConfigFunc& cfg_func);
MemoryStoragePtr NewMSWithPL(const std::vector<uint64_t>& peers,
                             const std::vector<uint64_t>& learners);
MemoryStoragePtr NewMSWithEnts(const std::vector<EntryPtr>& ents);

Config ConsConfig(uint64_t id, int election, int heartbeat,
                  const StoragePtr& storage);
Message ConsMsg(uint64_t from = 0, uint64_t to = 0, MessageType type = kMsgHup,
                const std::vector<Entry>& ents = {}, uint64_t term = 0,
                uint64_t log_term = 0, uint64_t index = 0, uint64_t commit = 0,
                const std::string& ctx = "");

void CommitNoopEntry(const RaftPtr& r, const MemoryStoragePtr& s);
Message AcceptAndReply(const Message& m);

}  // namespace jraft
