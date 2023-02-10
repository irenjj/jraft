// Copyright (c) renjj - All Rights Reserved

// Copyright 2016 The etcd Authors
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
#include "read_only.h"

#include <jrpc/base/logging/logging.h>

namespace jraft {

ReadOnly::ReadOnly(ReadOnlyOption option) : option_(option) {}

// AddRequest adds a read only request into ReadOnly.
// + 'index' is the commit index of the raft state machine when it received
//   the read only request.
// + 'm' is the original read only request message from the local or remote
//   node.
void ReadOnly::AddRequest(uint64_t index, const Message& m) {
  std::string s = m.entries(0).data();
  if (pending_read_index_.find(s) != pending_read_index_.end()) {
    return;
  }

  auto rs = std::make_shared<ReadIndexStatus>();
  rs->req = m;
  rs->index = index;
  pending_read_index_[s] = rs;
  read_index_queue_.push_back(s);
}

// RecvAck notifies the readonly struct that the raft state machine received
// an acknowledged of heartbeat that attached with the read only request
// context.
size_t ReadOnly::RecvAck(uint64_t id, const std::string& context) {
  auto iter = pending_read_index_.find(context);
  if (iter == pending_read_index_.end()) {
    return 0;
  }

  auto rs = iter->second;
  rs->acks[id] = true;
  return rs->acks.size() + 1;
}

// Advance advances the read only request queue by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given 'm'.
void ReadOnly::Advance(const Message& m, std::vector<ReadIndexStatusPtr>* rss) {
  size_t i;
  bool found = false;
  const std::string& ctx = m.context();

  for (i = 0; i < read_index_queue_.size(); i++) {
    auto iter = pending_read_index_.find(ctx);
    if (iter == pending_read_index_.end()) {
      JLOG_FATAL << "cannot find corresponding read state from pending map";
    }

    auto rs = iter->second;
    rss->push_back(rs);
    if (ctx == read_index_queue_[i]) {
      found = true;
      break;
    }
  }

  if (found) {
    ++i;
    read_index_queue_.erase(read_index_queue_.begin(),
                            read_index_queue_.begin() + i);
    for (i = 0; i < rss->size(); i++) {
      pending_read_index_.erase((*rss)[i]->req.entries(0).data());
    }
  }
}

// LastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
std::string ReadOnly::LastPendingRequestCtx() const {
  if (read_index_queue_.empty()) {
    return "";
  }

  return read_index_queue_[read_index_queue_.size() - 1];
}

void ReadOnly::Reset(ReadOnlyOption option) {
  pending_read_index_.clear();
  read_index_queue_.clear();
  option_ = option;
}

}  // namespace jraft
