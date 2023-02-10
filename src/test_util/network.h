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

#include <cstdint>
#include <functional>
#include <map>
#include <unordered_map>
#include <vector>

#include "common/util.h"
#include "eraft.pb.h"
#include "raft.h"
#include "state_machine.h"

namespace jraft {

struct Connem {
  uint64_t from;
  uint64_t to;
};

struct ConnemHash {
  uint64_t operator()(const Connem& connem) const {
    return std::hash<uint64_t>()(connem.from) * 10 +
           std::hash<uint64_t>()(connem.to);
  }
};

struct ConnemEqualKey {
  bool operator()(const Connem& le, const Connem& ri) const {
    return le.from == ri.from && le.to == ri.to;
  }
};

struct MessageTypeHash {
  uint64_t operator()(const MessageType& t) const {
    return std::hash<uint64_t>()(t);
  }
};

struct MessageTypeEqualKey {
  bool operator()(MessageType le, MessageType ri) const { return le == ri; }
};

class Network {
 public:
  typedef std::function<bool(const Message& msg)> MsgHookFunc;
  typedef std::function<void(Config& cfg)> ConfigFunc;

  explicit Network(const std::vector<StateMachinePtr>& peers,
                   const ConfigFunc& = nullptr);
  ~Network() = default;

  void Send(const std::vector<Message>& msgs);
  void Drop(uint64_t from, uint64_t to, double perc);
  void Cut(uint64_t one, uint64_t other);
  void Isolate(uint64_t id);
  void Ignore(MessageType t);
  void Recover();
  void Filter(const std::vector<Message>& msgs, std::vector<Message>* res);

  const std::map<uint64_t, StateMachinePtr>& peers() const { return peers_; }
  std::map<uint64_t, StateMachinePtr>& mutable_peers() { return peers_; }

  StateMachinePtr peer(uint64_t id) { return peers_[id]; }

  void set_msg_hook(MsgHookFunc func) { msg_hook_ = func; }

 private:
  std::map<uint64_t, StateMachinePtr> peers_;
  std::unordered_map<Connem, double, ConnemHash, ConnemEqualKey> dropm_;
  std::unordered_map<MessageType, bool, MessageTypeHash, MessageTypeEqualKey>
      ignorem_;
  // MsgHook is called for each message sent. It may inspect the message
  // and return true to send it or false to drop it.
  MsgHookFunc msg_hook_;
};

bool MsgHook(const Message& msg);

typedef std::shared_ptr<Network> NetworkPtr;

}  // namespace jraft
