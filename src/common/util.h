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

#include "conf.h"
#include "tracker/inflights.h"

namespace jraft {

class SoftState;

void LimitSize(uint64_t max_size, std::vector<EntryPtr>* entries);

EntryPtr NewEnt(uint64_t index = 0, uint64_t term = 0,
                const std::string& data = "", EntryType type = kEntryNormal);
SnapshotPtr NewSnap(uint64_t index = 0, uint64_t term = 0,
                    const std::string& data = "", ConfState* cs = nullptr);

Inflights ConsIn(size_t start, size_t count, uint64_t bytes, size_t max_size,
                 uint64_t max_bytes, const std::vector<Inflight>& buffer);
Entry ConsEnt(uint64_t index = 0, uint64_t term = 0,
              const std::string& data = "", EntryType type = kEntryNormal);
Message Msg(uint64_t from, uint64_t to, MessageType type,
            const std::vector<Entry>& ents = {});

bool IsSoftStateEqual(const SoftState& st1, const SoftState& st2);
bool IsHardStateEqual(const HardState& hs1, const HardState& hs2);

bool IsEmptySoftState(const SoftState& st);
bool IsEmptyHardState(const HardState& hs);
bool IsEmptySnap(const Snapshot& sp);
bool IsEmptySnap(const SnapshotPtr& sp);

MessageType VoteRespMsgType(MessageType msgt);

uint64_t PayloadsSize(const std::vector<EntryPtr>& entries);

bool IsLocalMsg(MessageType type);
bool IsResponseMsg(MessageType type);

}  // namespace jraft
