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
#include "network.h"

#include <deque>
#include <random>

#include "test_util.h"

namespace jraft {

// initializes a network from peers. A nullptr node will be replaced with a
// new StateMachine, A StateMachine will get its k, id. When using
// StateMachine, the address list is always [1, n].
Network::Network(const std::vector<StateMachinePtr>& peers,
                 const ConfigFunc& cfg_func)
    : msg_hook_(nullptr) {
  size_t size = peers.size();
  std::vector<uint64_t> peer_addrs;
  IdsBySize(size, &peer_addrs);

  for (int i = 0; i < size; i++) {
    uint64_t id = peer_addrs[i];
    const std::shared_ptr<StateMachine>& p = peers[i];

    if (p == nullptr) {
      auto ms = NewMSWithPeers(peer_addrs);

      auto cfg = ConsConfig(id, 10, 1, ms);
      if (cfg_func != nullptr) {
        cfg_func(cfg);
      }
      RaftPtr r = std::make_shared<Raft>(&cfg);
      peers_[id] = std::make_shared<StateMachine>(kRaftType, r);
    } else if (p->type() == kRaftType) {
      auto r = p->raft();
      r->set_id(id);
      auto tk = r->tracker();
      for (uint64_t j = 0; j < size; j++) {
        bool is_learner = false;
        auto pr = tk->mutable_progress(peer_addrs[j]);
        if (pr != nullptr && pr->is_learner()) {
          is_learner = true;
        }
        tk->ResetVotes();
        tk->SetProgress(peer_addrs[j], 0, 1, is_learner);
      }

      r->Reset(r->term());
      peers_[id] = p;
    } else {
      peers_[id] = p;
    }
  }
}

void Network::Send(const std::vector<Message>& msgs) {
  std::deque<Message> queue(msgs.begin(), msgs.end());
  while (!queue.empty()) {
    Message m = queue.front();
    queue.pop_front();
    auto p = peers_[m.to()];
    p->Step(m);
    std::vector<Message> read_msgs;
    p->ReadMessages(&read_msgs);
    std::vector<Message> new_msgs;
    Filter(read_msgs, &new_msgs);
    queue.insert(queue.end(), new_msgs.begin(), new_msgs.end());
  }
}

void Network::Drop(uint64_t from, uint64_t to, double perc) {
  dropm_[Connem{from, to}] = perc;
}

void Network::Cut(uint64_t one, uint64_t other) {
  Drop(one, other, 2.0);  // always drop
  Drop(other, one, 2.0);  // always drop
}

void Network::Isolate(uint64_t id) {
  for (int i = 0; i < peers_.size(); i++) {
    uint64_t nid = static_cast<uint64_t>(i) + 1;
    if (nid != id) {
      Drop(id, nid, 1.0);  // always drop
      Drop(nid, id, 1.0);  // always drop
    }
  }
}

void Network::Ignore(MessageType t) { ignorem_[t] = true; }

void Network::Recover() {
  dropm_.clear();
  ignorem_.clear();
}

void Network::Filter(const std::vector<Message>& msgs,
                     std::vector<Message>* res) {
  res->clear();
  for (const auto& m : msgs) {
    if (ignorem_[m.type()]) {
      continue;
    }

    auto type = m.type();
    if (type == kMsgHup) {
      // hups never go over the network, so don't drop them but panic
      JLOG_FATAL << "unexpected kMsgHup";
    } else {
      auto perc = dropm_[Connem{m.from(), m.to()}];
      std::random_device rd;
      std::default_random_engine eng(rd());
      std::uniform_real_distribution<double> distr(0, 1.0);
      if (distr(eng) <= perc) {
        JLOG_INFO << "drop message: from " << m.from() << " to " << m.to()
                  << " type " << MsgStr[m.type()];
        continue;
      }
    }

    if (msg_hook_ != nullptr) {
      if (!msg_hook_(m)) {
        continue;
      }
    }
    res->push_back(m);
  }
}

}  // namespace jraft
