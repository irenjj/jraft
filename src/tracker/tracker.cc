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
#include "tracker.h"

#include <jrpc/base/logging/logging.h>

namespace jraft {

ProgressTracker::ProgressTracker(size_t max_inflight_size,
                                 uint64_t max_inflight_bytes)
    : max_inflight_size_(max_inflight_size),
      max_inflight_bytes_(max_inflight_bytes) {}

// Returns true if the quorum is active from the view of the local raft state
// machine. Otherwise, it returns false.
bool ProgressTracker::QuorumActive() const {
  int active = 0;
  for (const auto& iter : progress_map_) {
    if (iter.second->is_learner() || !iter.second->recent_active()) {
      continue;
    }
    active++;
  }

  return active >= progress_map_.size() / 2 + 1;
}

// Returns true if (and only if) there is only one voting member
// (i.e. the leader) in the current configuration.
bool ProgressTracker::IsSingleton(uint64_t id) {
  if (votes_.find(id) != votes_.end() && votes_.size() == 1) {
    return true;
  }

  return false;
}

// Returns a sorted slice of voters.
std::vector<uint64_t> ProgressTracker::VoterNodes() const {
  std::vector<uint64_t> voters;
  for (const auto& n : progress_map_) {
    if (!n.second->is_learner()) {
      voters.push_back(n.first);
    }
  }

  return voters;
}

// Returns a sorted slice of learners.
std::vector<uint64_t> ProgressTracker::LearnerNodes() const {
  std::vector<uint64_t> learners;
  for (const auto& n : progress_map_) {
    if (n.second->is_learner()) {
      learners.push_back(n.first);
    }
  }

  return learners;
}

void ProgressTracker::ClearProgressMap() { progress_map_.clear(); }

// Prepares for a new round of vote counting via RecordVote().
void ProgressTracker::ResetVotes() { votes_.clear(); }

// Records that the node with the given id voted for this Raft instance
// if v == true (and declined it otherwise).
void ProgressTracker::RecordVote(uint64_t id, bool v) {
  auto iter = votes_.find(id);
  if (iter == votes_.end()) {
    votes_[id] = v;
  }
}

// Returns the number of granted and rejected Votes, and whether the election
// outcome is known.
void ProgressTracker::TallyVotes(int* granted, int* rejected,
                                 VoteResult* result) {
  // Make sure to populate granted/rejected correctly even if the Votes slice
  // contains members no longer part of the configuration. This doesn't really
  // matter in the way the numbers are used (they're informational), but might
  // as well get it right.
  *granted = 0;
  *rejected = 0;
  int all_node_cnt = 0;
  for (const auto& iter : progress_map_) {
    if (iter.second->is_learner()) {
      continue;
    }

    all_node_cnt++;

    auto votes_iter = votes_.find(iter.first);
    if (votes_iter == votes_.end()) {
      continue;
    }

    if (votes_iter->second) {
      (*granted)++;
    } else {
      (*rejected)++;
    }
  }

  uint64_t quorum = all_node_cnt / 2 + 1;
  if (*granted >= quorum) {
    *result = kVoteWon;
  } else if (*granted + all_node_cnt - votes_.size() < quorum) {
    *result = kVoteLost;
  } else {
    *result = kVotePending;
  }
}

void ProgressTracker::InsertProgress(uint64_t id, uint64_t match, uint64_t next,
                                     bool is_learner) {
  auto iter = progress_map_.find(id);
  if (iter != progress_map_.end()) {
    JLOG_FATAL << "shouldn't exist progress " << id;
  }

  progress_map_[id] = std::make_shared<Progress>(
      match, next, max_inflight_size_, max_inflight_bytes_);
  progress_map_[id]->set_is_learner(is_learner);
}

void ProgressTracker::SetProgress(uint64_t id, uint64_t match, uint64_t next,
                                  bool is_learner) {
  auto iter = progress_map_.find(id);
  if (iter != progress_map_.end()) {
    auto pr = iter->second;
    pr->Clear();
    pr->set_match(match);
    pr->set_next(next);
    pr->set_is_learner(is_learner);
  } else {
    progress_map_[id] = std::make_shared<Progress>(
        match, next, max_inflight_size_, max_inflight_bytes_);
    progress_map_[id]->set_is_learner(is_learner);
  }
}

void ProgressTracker::DelProgress(uint64_t id) {
  auto iter = progress_map_.find(id);
  if (iter != progress_map_.end()) {
    progress_map_.erase(id);
    votes_.erase(id);
  }
}

}  // namespace jraft
