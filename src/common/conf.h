// Copyright (c) renjj - All Rights Reserved
#pragma once

#include "eraft.pb.h"

namespace jraft {

using SnapshotPtr = std::shared_ptr<Snapshot>;
using EntryPtr = std::shared_ptr<Entry>;

// kNone is a placeholder node ID used when there is no leader.
const uint64_t kNone = 0;
const uint64_t kNoIndex = 0;
const uint64_t kNoTerm = 0;
const uint64_t kNoLimit = UINT64_MAX;
const size_t kMaxSize = SIZE_MAX;

// possible value for RaftStateType
enum RaftStateType {
  kStateFollower = 0,
  kStateCandidate = 1,
  kStateLeader = 2,
  kStatePreCandidate = 3,
  kNumStates = 4,
};

static const std::string RaftStateTypeStr[] = {
    "kStateFollower",     // 0
    "kStateCandidate",    // 1
    "kStateLeader",       // 2
    "kStatePreCandidate"  // 3
};

// CampaignType represents the type of campaigning.
enum CampaignType {
  // kCampaignPreElection represents the first phase of a normal election when
  // Config.PreVote is true.
  kCampaignPreElection = 0,
  // kCampaignElection represents a normal (time-based) election (the second
  // phase of the election when Config.Prevote is true).
  kCampaignElection = 1,
  // kCampaignTransfer represents the type of leader transfer.
  kCampaignTransfer = 2,
};

static const std::string CampaignTypeStr[] = {
    "kCampaignPreElection",  // 0
    "kCampaignElection",     // 1
    "kCampaignTransfer",     // 2
};

static const std::string MsgStr[] = {
    "kMsgHup",           "kMsgBeat",           "kMsgProp",
    "kMsgApp",           "kMsgAppResp",        "kMsgVote",
    "kMsgVoteResp",      "kMsgSnap",           "kMsgHeartbeat",
    "kMsgHeartbeatResp", "kMsgUnreachable",    "kMsgSnapStatus",
    "kMsgCheckQuorum",   "kMsgTransferLeader", "kMsgTimeoutNow",
    "kMsgReadIndex",     "kMsgReadIndexResp",  "kMsgPreVote",
    "kMsgPreVoteResp",
};

}  // namespace jraft
