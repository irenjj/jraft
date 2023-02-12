// Copyright (c) renjj - All Rights Reserved

// Copyright 2019 The etcd Authors
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

#include <map>

#include "progress.h"

namespace jraft {

enum VoteResult {
  // kVotePending indicates that the decision of the vote depends on future
  // votes, i.e. neither "yes" nor "no" has reached quorum yet.
  kVotePending = 1,
  // kVoteLost indicates that the quorum has voted "no".
  kVoteLost = 2,
  // kVoteWon indicates that the quorum has voted "yes".
  kVoteWon = 3,
};

class ProgressTracker {
 public:
  ProgressTracker(size_t max_inflight_size, uint64_t max_inflight_bytes);
  ~ProgressTracker() = default;

  bool QuorumActive() const;
  bool IsSingleton(uint64_t id);
  std::vector<uint64_t> VoterNodes() const;
  std::vector<uint64_t> LearnerNodes() const;

  void ClearProgressMap();
  void ResetVotes();
  void RecordVote(uint64_t id, bool v);
  void TallyVotes(int* granted, int* rejected, VoteResult* result);
  void InsertProgress(uint64_t id, uint64_t match, uint64_t next,
                      bool is_learner);
  void SetProgress(uint64_t id, uint64_t match, uint64_t next, bool is_learner);
  void DelProgress(uint64_t id);

  uint64_t MaybeCommit() const;

  size_t max_inflight_size() const { return max_inflight_size_; }
  ProgressPtr mutable_progress(uint64_t id) {
    auto iter = progress_map_.find(id);
    if (iter == progress_map_.end()) {
      return nullptr;
    }
    return iter->second;
  }
  std::map<uint64_t, ProgressPtr> progress_map() const { return progress_map_; }

  bool votes(uint64_t id) { return votes_[id]; }

 private:
  std::map<uint64_t, ProgressPtr> progress_map_;
  std::map<uint64_t, bool> votes_;
  size_t max_inflight_size_;
  uint64_t max_inflight_bytes_;
};

}  // namespace jraft
