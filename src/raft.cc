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
#include "raft.h"

#include <numeric>

#include "common/util.h"
#include "raft_log.h"
#include "ready.h"
#include "soft_state.h"

namespace jraft {

void Validate(Config* c) {
  if (c->id == kNone) {
    JLOG_FATAL << "id is none";
  }

  if (c->heartbeat_tick <= 0) {
    JLOG_FATAL << "heartbeat_tick <= 0";
  }

  if (c->election_tick <= c->heartbeat_tick) {
    JLOG_FATAL << "election_tick <= heartbeat_tick";
  }

  if (c->storage == nullptr) {
    JLOG_FATAL << "storage == nullptr";
  }

  if (c->max_uncommitted_entries_size == 0) {
    c->max_uncommitted_entries_size = kNoLimit;
  }

  // default max_committed_size_per_ready to max_size_per_msg they were
  // previously the same parameter.
  if (c->max_committed_size_per_ready == 0) {
    c->max_committed_size_per_ready = c->max_size_per_msg;
  }

  if (c->max_inflight_msgs == 0) {
    JLOG_FATAL << "max_inflight_msgs == 0";
  }
  if (c->max_inflight_bytes == 0) {
    c->max_inflight_bytes = kNoLimit;
  } else if (c->max_inflight_bytes < c->max_size_per_msg) {
    JLOG_FATAL << "max inflight bytes must be >= max message size";
  }

  if (c->read_only_option == kReadOnlyLeaseBased && !c->check_quorum) {
    JLOG_FATAL << "read_only_option == kReadOnlyLeaseBased and !check_quorum";
  }
}

Raft::Raft(Config* c)
    : id_(c->id),
      term_(kNoTerm),
      vote_(kNone),
      max_msg_size_(c->max_size_per_msg),
      state_(kStateFollower),
      is_learner_(false),
      lead_(kNone),
      leader_transferee_(kNone),
      pending_conf_index_(kNoIndex),
      uncommitted_size_(0),
      election_elapsed_(0),
      heartbeat_elapsed_(0),
      check_quorum_(c->check_quorum),
      pre_vote_(c->pre_vote),
      heartbeat_timeout_(c->heartbeat_tick),
      election_timeout_(c->election_tick),
      disable_proposal_forwarding_(c->disable_proposal_forwarding) {
  Validate(c);

  max_uncommitted_size_ = c->max_uncommitted_entries_size;

  raft_log_ = new RaftLog(c->storage, c->max_committed_size_per_ready);
  tracker_ = new ProgressTracker(c->max_inflight_msgs, c->max_inflight_bytes);
  read_only_ = new ReadOnly(c->read_only_option);

  HardState hs;
  ConfState cs;
  ErrNum err = c->storage->InitialState(&hs, &cs);
  if (err != kOk) {
    JLOG_FATAL << "initial state fail";
  }

  for (const auto& id : cs.voters()) {
    tracker_->InsertProgress(id, 0, 1, false);
  }
  for (const auto& id : cs.learners()) {
    tracker_->InsertProgress(id, 0, 1, true);
    if (id_ == id) {
      is_learner_ = true;
    }
  }

  if (!IsEmptyHardState(hs)) {
    LoadState(hs);
  }
  if (c->applied > 0) {
    raft_log_->AppliedTo(c->applied);
  }
  BecomeFollower(term_, kNone);

  std::string peers;
  for (const auto& p : tracker_->progress_map()) {
    if (p.second->is_learner()) {
      peers += " learner " + std::to_string(p.first);
    } else {
      peers += " voter " + std::to_string(p.first);
    }
  }
  JLOG_INFO << "new raft " << id_ << ", term: " << term_
            << ", commit: " << raft_log_->committed()
            << ", applied: " << raft_log_->applied()
            << ", last index: " << raft_log_->LastIndex()
            << ", last term: " << raft_log_->LastTerm() << ", peers: " << peers;
}

Raft::~Raft() {
  if (raft_log_ != nullptr) {
    delete raft_log_;
  }
  if (read_only_ != nullptr) {
    delete read_only_;
  }
  if (tracker_ != nullptr) {
    delete tracker_;
  }
}

bool Raft::HasLeader() const { return lead_ != kNone; }

SoftState Raft::ConstructSoftState() {
  SoftState cs(lead_, state_);
  return cs;
}

HardState Raft::ConstructHardState() {
  HardState hs;
  hs.set_term(term_);
  hs.set_vote(vote_);
  hs.set_commit(raft_log_->committed());
  return hs;
}

ConfState Raft::ConstructConfState() const {
  ConfState cs;
  for (const auto& n : tracker_->progress_map()) {
    if (n.second->is_learner()) {
      cs.add_learners(n.first);
    } else {
      cs.add_voters(n.first);
    }
  }
  return cs;
}

// Send schedules persisting state to a stable storage and AFTER that sending
// the message (as part of next Ready message processing).
void Raft::Send(Message m) {
  if (m.from() == kNone) {
    m.set_from(id_);
  }

  if (m.type() == kMsgVote || m.type() == kMsgVoteResp ||
      m.type() == kMsgPreVote || m.type() == kMsgPreVoteResp) {
    if (m.term() == 0) {
      // All {pre-} campaign messages need to have the term set when sending.
      // - kMsgVote: m->term() is the term the node is campaigning for,
      //   non-zero as we increment the term when campaigning.
      // - kMsgVoteResp: m->term() is the new t.term() if the kMsgVote was
      //   granted, non-zero for the same reason kMsgVote is.
      // - kMsgPreVote: m->term() is the term the node will campaign,
      //   non-zero as we use m->term() to indicate the next term we'll be
      //   campaigning for.
      // - kMsgPreVoteResp: m->term() is the term received in the original
      //   kMsgPreVote if the pre-vote was granted, non-zero for the same
      //   reason kMsgPreVote is.
      JLOG_FATAL << "term should be set when sending " << MsgStr[m.type()];
    }
  } else {
    if (m.term() != 0) {
      JLOG_FATAL << "term: " << m.term() << " should not be set when sending "
                 << MsgStr[m.type()];
    }

    // do not attach term to kMsgProp, kMsgReadIndex
    // proposals are a way to forward to the leader and should be treated
    // as local message.
    // kMsgReadIndex is also forwarded to leader.
    if (m.type() != kMsgProp && m.type() != kMsgReadIndex) {
      m.set_term(term_);
    }
  }
  msgs_.push_back(m);
}

// SendAppend sends an append RPC with new entries (if any) and the current
// commit index to the given peer.
void Raft::SendAppend(uint64_t to) { MaybeSendAppend(to, true); }

// MaybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The send_if_empty
// argument controls whether messages with no entries will be sent
// ("empty" message are useful to convey updated Commit indexes, but are
// undesirable when we're sending multiple message in a batch).
bool Raft::MaybeSendAppend(uint64_t to, bool send_if_empty) {
  auto pr = tracker_->mutable_progress(to);
  if (pr->IsPaused()) {
    return false;
  }
  Message m;
  m.set_to(to);

  uint64_t term;
  std::vector<EntryPtr> entries;
  ErrNum errt = raft_log_->Term(pr->next() - 1, &term);
  ErrNum erre = kOk;
  // In a throttled kStateReplicate only send empty kMsgApp, to ensure progress.
  // Otherwise, if we had a full Inflights and all inflight messages were in
  // fact dropped, replication to that follower would stall. Instead, an empty
  // kMsgApp will eventually reach the follower (heartbeats responses prompt the
  // leader to send an append), allowing it to be acked or rejected, both of
  // which will clear out Inflights.
  if (pr->state() != kStateReplicate || !pr->mutable_inflights()->Full()) {
    erre = raft_log_->Entries(pr->next(), max_msg_size_, &entries);
  }

  if (entries.empty() && !send_if_empty) {
    return false;
  }

  // send snapshot if we failed to get term or entries
  if (errt != kOk || erre != kOk) {
    if (!pr->recent_active()) {
      JLOG_DEBUG << "ignore sending snapshot to " << to << " since it is not"
                 << " recently active";
      return false;
    }

    m.set_type(kMsgSnap);
    SnapshotPtr snapshot = std::make_shared<Snapshot>();
    ErrNum errsnap = raft_log_->GetSnapshot(snapshot);
    if (errsnap != kOk) {
      if (errsnap == kErrSnapshotTemporarilyUnavailable) {
        JLOG_DEBUG << id_ << " failed to send snapshot to " << to
                   << " because snapshot is temporarily unavailable";
        return false;
      }
      JLOG_FATAL << "panic";
    }

    if (IsEmptySnap(*snapshot)) {
      printf("empty snap\n");
      JLOG_FATAL << "need non-empty snapshot";
    }

    Snapshot* s = m.mutable_snapshot();
    s->CopyFrom(*snapshot);
    uint64_t sindex = snapshot->metadata().index();
    uint64_t sterm = snapshot->metadata().term();
    JLOG_DEBUG << id_ << " [first index: " << raft_log_->FirstIndex()
               << ", committed: " << raft_log_->committed() << "] sent snapshot"
               << "[index: " << sindex << ", term: " << sterm << "] to " << to;
    pr->BecomeSnapshot(sindex);
    JLOG_DEBUG << id_ << " paused sending replication messages to " << to;
  } else {
    m.set_type(kMsgApp);
    m.set_index(pr->next() - 1);
    m.set_log_term(term);
    m.set_commit(raft_log_->committed());
    for (auto& ent : entries) {
      Entry* add_ent = m.add_entries();
      add_ent->CopyFrom(*ent);
    }

    // Send the actual kMsgApp otherwise, and update the progress accordingly.
    if (!pr->UpdateOnEntriesSend(entries.size(), PayloadsSize(entries),
                                 pr->next())) {
      JLOG_FATAL << id() << " failed to update";
    }
  }

  Send(m);

  return true;
}

// SendHeartbeat sends a heartbeat RPC to the given peer.
void Raft::SendHeartbeat(uint64_t to, const std::string& ctx) {
  // Attach the commit as min(to.matched_, r.committed_).
  // When the leader sends out heartbeat message,
  // the receiver (follower) might not be matched with the leader,
  // or it might not have all the committed entries.
  // The leader MUST NOT forward the follower's commit to an
  // unmatched index.
  uint64_t commit =
      std::min(tracker_->mutable_progress(to)->match(), raft_log_->committed());
  Message m;
  m.set_to(to);
  m.set_type(kMsgHeartbeat);
  m.set_commit(commit);
  m.set_context(ctx);

  Send(m);
}

// BcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in tracker_.
void Raft::BcastAppend() {
  auto tracker_map = tracker_->progress_map();
  for (auto& iter : tracker_map) {
    if (iter.first != id_) {
      SendAppend(iter.first);
    }
  }
}

// BCastHeartbeat sends RPC, without entries to all the peers.
void Raft::BcastHeartbeat() {
  auto last_ctx = read_only_->LastPendingRequestCtx();
  BcastHeartbeatWithCtx(last_ctx);
}

void Raft::BcastHeartbeatWithCtx(const std::string& ctx) {
  auto tracker_map = tracker_->progress_map();
  for (auto& iter : tracker_map) {
    if (iter.first != id_) {
      SendHeartbeat(iter.first, ctx);
    }
  }
}

void Raft::Advance(const ReadyPtr& rd) {
  ReduceUncommittedSize(rd->committed_entries());

  // If entries were applied (or a snapshot), update our cursor for the next
  // Ready. Note that if the current HardState contains a new commit
  // index, this does not mean that we're also applying all the new entries
  // due to commit pagination by size.
  uint64_t new_applied = rd->AppliedCursor();
  if (new_applied > 0) {
    raft_log_->AppliedTo(new_applied);

    // TODO: finish confchange
  }

  if (!rd->entries().empty()) {
    auto e = rd->entries().back();
    if (id_ == lead_) {
      // The leader needs to self-ack the entries just appended (since it
      // doesn't send an kMsgApp to itself). This is roughly equivalent to:
      //
      //  r.prs.Progress[r.id].MaybeUpdate(e.Index)
      //  if r.maybeCommit() {
      //  	r.bcastAppend()
      //  }
      Message m;
      m.set_from(id_);
      m.set_type(kMsgAppResp);
      m.set_index(e->index());
      Step(m);
    }
    // NB: it's important for performance that this call happens after
    // r.Step above on the leader. This is because r.Step can then use
    // a fast-path for `r.raftLog.term()`.
    raft_log_->StableTo(e->index(), e->term());
  }
  if (!IsEmptySnap(rd->mutable_snapshot())) {
    raft_log_->StableSnapTo(rd->mutable_snapshot()->metadata().index());
  }
}

// MaybeCommit attempts to advance the commit index. Returns true if the commit
// index changed (in which case the caller should call BcastAppend).
bool Raft::MaybeCommit() {
  auto tracker_map = tracker_->progress_map();
  std::vector<uint64_t> mis(tracker_map.size());
  int i = 0;
  for (auto& iter : tracker_map) {
    mis[i++] = iter.second->match();
  }
  std::sort(mis.begin(), mis.end());
  auto mci = mis[mis.size() - Quorum()];

  return raft_log_->MaybeCommit(mci, term_);
}

void Raft::Reset(uint64_t term) {
  if (term_ != term) {
    term_ = term;
    vote_ = kNone;
  }
  lead_ = kNone;

  election_elapsed_ = 0;
  heartbeat_elapsed_ = 0;
  ResetRandomizedElectionTimeout();

  AbortLeaderTransfer();

  tracker_->ResetVotes();
  auto progress_map = tracker_->progress_map();
  for (auto& iter : progress_map) {
    uint64_t id = iter.first;
    bool is_learner = iter.second->is_learner();
    uint64_t match = 0;
    if (id == id_) {
      match = raft_log_->LastIndex();
      is_learner_ = is_learner;
    }
    tracker_->SetProgress(id, match, raft_log_->LastIndex() + 1, is_learner);
  }

  pending_conf_index_ = 0;
  uncommitted_size_ = 0;
  auto read_only_option = read_only_->option();
  read_only_->Reset(read_only_option);
}

bool Raft::AppendEntry(std::vector<Entry>& es) {
  uint64_t li = raft_log_->LastIndex();
  size_t n = es.size();
  std::vector<EntryPtr> ents(n);
  for (size_t i = 0; i < n; i++) {
    es[i].set_term(term_);
    es[i].set_index(li + i + 1);
    ents[i] = std::make_shared<Entry>(std::move(es[i]));
  }

  // Track the size of this uncommitted proposal.
  if (!IncreaseUncommittedSize(ents)) {
    JLOG_WARN << id_ << " appending new entries to log would exceed "
              << "uncommitted entry size limit; dropping proposal";
    // Drop the proposal
    return false;
  }

  // use latest "last" index after truncate/append
  li = raft_log_->Append(ents);
  tracker_->mutable_progress(id_)->MaybeUpdate(li);
  // Regardless of MaybeCommit's return, out caller will call BcastAppend
  MaybeCommit();
  return true;
}

void Raft::BecomeFollower(uint64_t term, uint64_t lead) {
  step_func_ = std::bind(&Raft::StepFollower, this, std::placeholders::_1);
  Reset(term);
  tick_func_ = std::bind(&Raft::TickElection, this);
  lead_ = lead;
  state_ = kStateFollower;
  JLOG_INFO << id_ << " become follower at term " << term_;
}

void Raft::BecomeCandidate() {
  if (state_ == kStateLeader) {
    JLOG_FATAL << "invalid transaction [leader -> candidate]";
  }

  step_func_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
  Reset(term_ + 1);
  tick_func_ = std::bind(&Raft::TickElection, this);
  vote_ = id_;
  state_ = kStateCandidate;
  JLOG_INFO << id_ << " became candidate at term " << term_;
}

void Raft::BecomePreCandidate() {
  if (state_ == kStateLeader) {
    JLOG_FATAL << "invalid transaction [leader -> pre-candidate]";
  }

  // Becoming a pre-candidate changes out step functions and state, but doesn't
  // change anything else. In particular, it does not increase term_ or change
  // vote_.
  step_func_ = std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
  tracker_->ResetVotes();
  tick_func_ = std::bind(&Raft::TickElection, this);
  lead_ = kNone;
  state_ = kStatePreCandidate;
  JLOG_INFO << id_ << " became pre-candidate at term " << term_;
}

void Raft::BecomeLeader() {
  if (state_ == kStateFollower) {
    JLOG_FATAL << "invalid transaction [follower -> leader]";
  }

  step_func_ = std::bind(&Raft::StepLeader, this, std::placeholders::_1);
  Reset(term_);
  tick_func_ = std::bind(&Raft::TickHeartbeat, this);
  lead_ = id_;
  state_ = kStateLeader;
  // Followers enter replicate mode when they've been successfully probed
  // (perhaps after having received a snapshot as a result). The leader is
  // trivially in this state. Note that Reset() has initialized this progress
  // with the last index already.
  ProgressPtr pr = tracker_->mutable_progress(id_);
  pr->BecomeReplicate();
  // The leader always has recent_active == true; kMsgCheckQuorum makes sure to
  // preserve this.
  pr->set_recent_active(true);

  // Conservatively set the pending_conf_index_ to the last index in the log.
  // There may or may not be a pending config change, but it's safe to delay
  // any future proposals until we commit all our pending log entries, and
  // scanning the entire tail of the log could be expensive.
  pending_conf_index_ = raft_log_->LastIndex();

  Entry empty_ent;
  std::vector<Entry> ents({empty_ent});
  if (!AppendEntry(ents)) {
    // This won't happen because we just called Reset() above.
    JLOG_FATAL << "empty entry was dropped";
  }

  JLOG_INFO << id_ << " became leader at term " << term_;
}

void Raft::Hup(CampaignType t) {
  if (state_ == kStateLeader) {
    JLOG_DEBUG << id_ << " ignoring kMsgHup because already leader";
    return;
  }

  if (!Promotable()) {
    JLOG_WARN << id_ << " is unpromotable and can not campaign";
    return;
  }

  std::vector<EntryPtr> ents;
  ErrNum err = raft_log_->Slice(raft_log_->applied() + 1,
                                raft_log_->committed() + 1, kNoLimit, &ents);
  if (err != kOk) {
    JLOG_FATAL << "unexpected error getting un unapplied entries " << err;
  }
  uint64_t n = NumOfPendingConf(ents);
  if (n != 0 && raft_log_->committed() > raft_log_->applied()) {
    JLOG_WARN << id_ << " cannot campaign at term " << term_
              << " since there are still " << n << " pending.";
    return;
  }

  JLOG_INFO << id_ << " is starting a new election at term " << term_;
  Campaign(t);
}

// Campaign transitions the raft instance to candidate state. This must only
// be called after verifying that this is a legitimate transition.
void Raft::Campaign(CampaignType t) {
  if (!Promotable()) {
    // this path should not be hit (callers are supposed to check), but better
    // safe than sorry.
    JLOG_WARN << id_ << " is unpromotable; Campaign() should have been called";
  }

  uint64_t term;
  MessageType vote_msg;
  if (t == kCampaignPreElection) {
    BecomePreCandidate();
    vote_msg = kMsgPreVote;
    // PreVote RPCs are sent for the next term before we've incremented term_.
    term = term_ + 1;
  } else {
    BecomeCandidate();
    vote_msg = kMsgVote;
    term = term_;
  }

  int granted = 0;
  int rejected = 0;
  VoteResult res;
  Poll(id_, VoteRespMsgType(vote_msg), true, &granted, &rejected, &res);
  if (res == kVoteWon) {
    // We won the election after voting for ourselves (which must mean that
    // this is a single-node cluster). Advance to the next state.
    if (t == kCampaignPreElection) {
      Campaign(kCampaignElection);
    } else {
      BecomeLeader();
    }

    return;
  }

  auto prs_map = tracker_->progress_map();
  for (auto& iter : prs_map) {
    uint64_t id = iter.first;
    if (id == id_) {
      continue;
    }

    JLOG_INFO << id_ << " [log_term: " << raft_log_->LastTerm()
              << ", index: " << raft_log_->LastIndex() << "] sent "
              << MsgStr[vote_msg] << " request to " << id << " at term "
              << term_;
    std::string ctx;
    if (t == kCampaignTransfer) {
      ctx = CampaignTypeStr[t];
    }
    Message m;
    m.set_term(term);
    m.set_to(id);
    m.set_type(vote_msg);
    m.set_index(raft_log_->LastIndex());
    m.set_log_term(raft_log_->LastTerm());
    m.set_context(ctx);
    Send(m);
  }
}

void Raft::Poll(uint64_t id, MessageType t, bool v, int* granted, int* rejected,
                VoteResult* vote_result) {
  if (v) {
    JLOG_INFO << id_ << " received " << MsgStr[t] << " from " << id
              << " at term " << term_;
  } else {
    JLOG_INFO << id_ << " received " << MsgStr[t] << " rejection from " << id
              << " at term " << term_;
  }

  tracker_->RecordVote(id, v);
  tracker_->TallyVotes(granted, rejected, vote_result);
}

ErrNum Raft::Step(const Message& m) {
  // Handle the message term, which may result in out stepping down to a
  // follower.
  if (m.term() == 0) {
    // local message
  } else if (m.term() > term_) {
    if (m.type() == kMsgVote || m.type() == kMsgPreVote) {
      bool force = (m.context() == "kCampaignTransfer");
      bool in_lease = check_quorum_ && lead_ != kNone &&
                      election_elapsed_ < election_timeout_;
      if (!force && in_lease) {
        // If a server receives a RequestVote request within the minimum
        // election timeout of hearing from a current leader, it does
        // not update its term or grant its vote
        JLOG_INFO << id_ << " [log_term: " << raft_log_->LastTerm()
                  << ", index: " << raft_log_->LastIndex()
                  << ", vote: " << vote_ << "] ignored " << MsgStr[m.type()]
                  << " from " << m.from() << " [log_term: " << m.log_term()
                  << ", index: " << m.index() << m.index() << "] at term "
                  << term_ << ": lease is not expired (remaining ticks: "
                  << election_timeout_ - election_elapsed_ << ")";
        return kOk;
      }
    }

    if (m.type() == kMsgPreVote) {
      // Never change out term in response to a PreVote.
    } else if (m.type() == kMsgPreVoteResp && !m.reject()) {
      // We send pre-vote requests with a term in out future. if the pre-vote
      // is granted, we will increment out term when we get a quorum.
      // If it is not, the term comes from the node that rejected our vote, so
      // we should become a follower at the new term.
    } else {
      JLOG_INFO << id_ << " [term: " << term_ << "] received a "
                << MsgStr[m.type()] << " message with higher term from "
                << m.from() << " [term: " << m.term() << "]";
      if (m.type() == kMsgApp || m.type() == kMsgHeartbeat ||
          m.type() == kMsgSnap) {
        BecomeFollower(m.term(), m.from());
      } else {
        BecomeFollower(m.term(), kNone);
      }
    }
  } else if (m.term() < term_) {
    if ((check_quorum_ || pre_vote_) &&
        (m.type() == kMsgHeartbeat || m.type() == kMsgApp)) {
      // We have received messages from a leader at a lower term. It is
      // possible that these message were simply delayed in the network, but
      // this could also mean that this node has advanced its term number
      // during a network partition, and it is now unable to either win an
      // election or to rejoin the majority on the old term. If check_quorum_
      // is false, this will be handled by incrementing term number in response
      // to kMsgVote with a higher term, but if check_quorum_ is true we may
      // not advance the term on kMsgVote and must generate other message to
      // advance the term on kMsgVote and must generate other messages to
      // advance the term. The net result of these two features is to minimize
      // the disruption caused by nodes that have been removed from the
      // cluster's configuration: a removed node will send kMsgVotes
      // (or kMsgPreVotes) which will be ignored, but it will not receive
      // kMsgApp or kMsgHeartbeat, so it will not create disruptive term
      // increases, by notifying leader of this node's activeness.
      // The above comments also true for Pre-Vote
      //
      // When follower gets isolated, it soon starts an election ending
      // up with a higher term than leader, although it won't receive enough
      // votes to win the election. When it regains connectivity, this response
      // with "kMsgAppResp" of higher term would force leader to step down.
      // However, this disruption is inevitable to free this stuck node with
      // fresh election. This can be prevented with Pre-Vote phase.
      Message resp_msg;
      resp_msg.set_to(m.from());
      resp_msg.set_type(kMsgAppResp);
      Send(resp_msg);
    } else if (m.type() == kMsgPreVote) {
      // Before Pre-Vote enable, there may have candidate with higher term,
      // but less log. After update to Pre-Vote, the cluster may deadlock if
      // we drop messages with a lower term.
      JLOG_INFO << id_ << "[log_term: " << raft_log_->LastTerm()
                << ", index: " << raft_log_->LastIndex() << ", vote: " << vote_
                << "] rejected " << MsgStr[m.type()] << " from " << m.from()
                << "[log_term: " << m.log_term() << ", index: " << m.index()
                << "] at term " << term_;
      Message resp_msg;
      resp_msg.set_to(m.from());
      resp_msg.set_term(term_);
      resp_msg.set_type(kMsgPreVoteResp);
      resp_msg.set_reject(true);
      Send(resp_msg);
    } else {
      // ignore other cases
      JLOG_INFO << id_ << " [term: " << term_ << "] ignored a "
                << MsgStr[m.type()] << " message with lower term from "
                << m.from() << "[term: " << m.term() << "]";
    }
    return kOk;
  }

  if (m.type() == kMsgHup) {
    if (pre_vote_) {
      Hup(kCampaignPreElection);
    } else {
      Hup(kCampaignElection);
    }
  } else if (m.type() == kMsgVote || m.type() == kMsgPreVote) {
    // We can vote it this a repeat of a vote we've already cast...
    bool can_vote = ((vote_ == m.from())
                     // ...we haven't voted, and we don't think there's a leader
                     // yet in this term...
                     || (vote_ == kNone && lead_ == kNone)
                     // ... or this is a PreVote for a future term...
                     || (m.type() == kMsgPreVote && m.term() > term_));
    // ... and we believe the candidate is up-to-date.
    if (can_vote && raft_log_->IsUpToDate(m.index(), m.log_term())) {
      // Note: it turns out that learners must be allowed to cast votes.
      // This seems counter-intuitive but is necessary in the situation
      // in which a learner has been promoted (i.e. is now a voter) but
      // has not learned about this yet.
      // For example, consider a group in which id = 1 is learner and
      // id = 2 and id = 3 are voters. A configuration change promoting
      // 1 can be committed on the quorum '{2, 3}' without the config
      // change being appended to the learner's log. If the leader (say 2)
      // fails, there are de facto two voters remaining. Only 3 can win
      // an election (due to its log containing all committed entries),
      // but to do so it will need 1 to vote. But 1 considers itself a
      // learner and will continue to do so until 3 has stepped up as leader
      // replicates the conf change to 1, and 1 applies it. Ultimately, by
      // receiving a request to the conf change to 1, and 1 applies it.
      // Ultimately, by receiving a request to vote, the learner realizes
      // that the candidate believes it to be a voter, and that it should
      // act accordingly. The candidate's config may be stale, too; but
      // in that case it won't win the election, at least in the absence of
      // the bug discussed in:
      // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
      JLOG_INFO << id_ << " [log_term: " << raft_log_->LastTerm()
                << ", index: " << raft_log_->LastIndex() << ", vote: " << vote_
                << "] cast " << MsgStr[m.type()] << " for " << m.from()
                << " [log_term: " << m.log_term() << ", index: " << m.index()
                << "] at term " << term_;
      // When responding to kMsg{Pre,}Vote messages we include the term from
      // the message, not the local term. To see why, consider the case
      // where a single node was previously partitioned away, and it's local
      // term is now out of date. If we include the local term (recall that
      // for pre-votes we don't update the local term), the (pre-)campaigning
      // node on the other end will proceed to ignore the message (it ignores
      // all out of date messages).
      // The term in the original message and current local term are the same
      // in the case of regular votes, but different for pre-votes.
      Message resp_msg;
      resp_msg.set_to(m.from());
      resp_msg.set_term(m.term());
      resp_msg.set_type(VoteRespMsgType(m.type()));
      Send(resp_msg);
      if (m.type() == kMsgVote) {
        election_elapsed_ = 0;
        vote_ = m.from();
      }
    } else {
      JLOG_INFO << id_ << " [log_term: " << raft_log_->LastTerm()
                << ", index: " << raft_log_->LastIndex() << ", vote: " << vote_
                << "] rejected " << MsgStr[m.type()] << " from " << m.from()
                << " [log_term: " << m.log_term() << ", index: " << m.index()
                << " at term " << term_;
      Message resp_msg;
      resp_msg.set_to(m.from());
      resp_msg.set_term(term_);
      resp_msg.set_type(VoteRespMsgType(m.type()));
      resp_msg.set_reject(true);
      Send(resp_msg);
    }
  } else {
    return step_func_(m);
  }

  return kOk;
}

ErrNum Raft::StepLeader(const Message& m) {
  // These message types do not require any progress for m.from().
  auto type = m.type();
  if (type == kMsgBeat) {
    BcastHeartbeat();
    return kOk;
  } else if (type == kMsgCheckQuorum) {
    // The leader should always see itself as active. As a precaution, handle
    // the case in which the leader isn't in the configuration anymore (for
    // example if it just removed itself).
    auto tracker = mutable_tracker();
    auto pr = tracker->mutable_progress(id_);
    if (pr != nullptr) {
      pr->set_recent_active(true);
    }
    if (!tracker->QuorumActive()) {
      JLOG_WARN << id_ << " stepped down to follower since quorum is not"
                << " active";
      BecomeFollower(term_, kNone);
    }
    // Mark everyone (but ourselves) as inactive in preparation for the next
    // CheckQuorum
    for (const auto& p : tracker->progress_map()) {
      if (p.first != id_) {
        p.second->set_recent_active(false);
      }
    }
    return kOk;
  } else if (type == kMsgProp) {
    if (m.entries().empty()) {
      JLOG_FATAL << id_ << " stepped empty kMsgProp";
    }
    auto pr = tracker_->mutable_progress(id_);
    if (pr == nullptr) {
      return kErrProposalDropped;
    }
    if (leader_transferee_ != kNone) {
      JLOG_INFO << id_ << " [term " << term_ << "] transfer leadership to "
                << leader_transferee_;
      return kErrProposalDropped;
    }

    Message tmp_msg(m);
    for (size_t i = 0; i < tmp_msg.entries().size(); i++) {
      auto e = tmp_msg.entries(i);
      if (e.type() == kEntryConfChange) {
        if (pending_conf_index_ > raft_log_->applied()) {
          JLOG_INFO << "propose conf ignored since pending unapplied "
                    << "configuration [index " << pending_conf_index_
                    << ", applied " << raft_log_->applied();
          tmp_msg.mutable_entries(i)->CopyFrom(ConsEnt());
        } else {
          pending_conf_index_ = raft_log_->LastIndex() + i + 1;
        }
      }
    }

    std::vector<Entry> ents(tmp_msg.entries().size());
    int i = 0;
    for (const auto& e : tmp_msg.entries()) {
      ents[i++] = e;
    }
    if (!AppendEntry(ents)) {
      return kErrProposalDropped;
    }
    BcastAppend();
    return kOk;
  } else if (type == kMsgReadIndex) {
    // only one voting member (the leader) in the cluster
    if (tracker_->IsSingleton(id_)) {
      auto resp = ResponseToReadIndexReq(m, raft_log_->committed());
      if (resp.to() != kNone) {
        Send(resp);
      }
      return kOk;
    }

    // Postpone read only request when this leader has not committed
    // any log entry at its term.
    if (!CommittedEntryInCurrentTerm()) {
      AppendPendingReadIndexMessages(m);
      return kOk;
    }

    SendMsgReadIndexResponse(m);

    return kOk;
  }

  // All other message types require progress for m.from()
  auto pr = tracker_->mutable_progress(m.from());
  if (pr == nullptr) {
    JLOG_DEBUG << id_ << " no progress available for " << m.from();
    return kOk;
  }

  if (type == kMsgAppResp) {
    // NB: this code path is also hit from Advance(), where the leader steps
    // an kMsgAppResp to acknowledge the appended entries in the last Ready.

    pr->set_recent_active(true);

    if (m.reject()) {
      // RejectHint is the suggested next common entry for appending (i.e.
      // we try to append entry reject_hint+1 next), and log_term is the
      // term that the follower has at index reject_hint. Older versions
      // of this library did not populate log_term for rejections, and it is
      // zero for followers with empty log.
      //
      // Under normal circumstances, the leader's log is longer than the
      // follower's and the follower's log is a prefix of the leader's
      // (i.e. there is no divergent uncommitted suffix of the log on the
      // follower). In that case, the first probe reveals where the follower's
      // log ends (reject_hint = follower's last index) and the subsequent
      // probe succeeds.
      //
      // However, when networks are partitioned or systems overloaded, large
      // divergent log tails can occur. The naive attempt, probing entry by
      // entry in decreasing order, will be the product of the length of the
      // diverging tails and the network round-trip latency, which can easily
      // result in hours of time spent probing and can even cause outright
      // outages. The probes are thus optimized as described below.
      JLOG_DEBUG << id_ << " received kMsgAppResp(rejected, hint: ("
                 << "index " << m.reject_hint() << ", term " << m.log_term()
                 << ")) from " << m.from() << " for index " << m.index();
      auto next_probe_index = m.reject_hint();
      if (m.log_term() > 0) {
        // If the follower has an uncommitted log tail, we would end up probing
        // one by one until we hit the common prefix.
        //
        // For example, if the leader has:
        //
        //
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 3 3 3 5 5 5 5 5
        //   term (F)   1 1 1 1 2 2
        //
        // Then, after sending an append anchored at (idx=9, term=5) we would
        // receive a reject_hint of 6 and log_term of 2. Without the code
        // below, we would try an append at index 6, which would fail again.
        //
        // However, looking only at what the leader knows about its own log
        // and the rejection hint, it is clear that a probe at index
        // 6, 5, 4, 3 and 2 must fail as well:
        //
        // For all of these indexes, the leader's log term is larger than the
        // rejection's log term. If a probe at one of these indexes
        // succeeded, its log term at that index would match the leader's,
        // i.e. 3 or 5 in this example. But the follower already told the
        // leader that it is still at term 2 at index 9, and since the
        // log term only ever goes up (within a log), this is a contradiction.
        //
        // At index 1, however, the leader can draw no such conclusion, as
        // its term 1 is not larger than the term 2 from the follower's
        // rejection. We thus probe at 1, which will succeed in this example.
        // In general, with this approach we probe at most once per term found
        // in the leader's log.
        //
        // There is a similar mechanism on the follower (implemented in
        // HandleAppendEntries via a call to FindConflictByTerm) that is useful
        // if the follower has a large divergent uncommitted log tail[1],
        // as in this example:
        //
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 3 3 3 3 3 3 3 7
        //   term (F)   1 3 3 4 4 5 5 5 6
        //
        // Naively, the leader would probe at idx=9, receive a rejection
        // revealing the log term of 6 at the follower. Since the leader's
        // term at the previous index is already smaller than 6, the leader-
        // side optimization discussed above is ineffective. The leader thus
        // probes at index 8 and, naively, receives a rejection for the same
        // index and log term 5. Again, the leader optimization does not improve
        // over linear probing as term 5 is above the leader's term 3 for that
        // and many preceding indexes; the leader would have to probe linearly
        // until it would finally hit index 3, where the probe would succeed.
        //
        // Instead, we apply a similar optimization on the follower. When the
        // follower receives the probe at index 8 (log term 3), it concludes
        // that all the leader's log preceding that index has log terms of
        // 3 or below. The largest index in the follower's log with a log term
        // of 3 or below is index 3. The follower will thus return a rejection
        // for index=3, log term=3 instead. The leader's next probe will then
        // succeed at that index.
        //
        // [1]: more precisely, if the log terms in the large uncommitted
        // tail on the follower are larger than the leader's. At first,
        // it may seem unintuitive that a follower could even have such
        // a large tail, but it can happen:
        //
        // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
        //   idx        1 2 3 4 5 6 7 8 9
        //              -----------------
        //   term (L)   1 2 2     [crashes]
        //   term (F)   1
        //   term (F)   1
        //
        // 2. a follower becomes leader and appends entries at term 3.
        //              -----------------
        //   term (x)   1 2 2     [down]
        //   term (F)   1 3 3 3 3
        //   term (F)   1
        //
        // 3. term 3 leader goes down, term 2 leader returns as term 4
        //    leader. It commits the log & entries at term 4.
        //
        //              -----------------
        //   term (L)   1 2 2 2
        //   term (x)   1 3 3 3 3 [down]
        //   term (F)   1
        //              -----------------
        //   term (L)   1 2 2 2 4 4 4
        //   term (F)   1 3 3 3 3 [gets probed]
        //   term (F)   1 2 2 2 4 4 4
        //
        // 4. the leader will now probe the returning follower at index
        //    7, the rejection points it at the end of the follower's log
        //    which is at a higher log term than the actually committed
        //    log.
        next_probe_index =
            raft_log_->FindConflictByTerm(m.reject_hint(), m.log_term());
      }
      if (pr->MaybeDecrTo(m.index(), next_probe_index)) {
        JLOG_DEBUG << id_ << " decreased progress of " << m.from() << " to "
                   << m.from();
        if (pr->state() == kStateReplicate) {
          pr->BecomeProbe();
        }
        SendAppend(m.from());
      }
    } else {
      bool old_paused = pr->IsPaused();
      if (pr->MaybeUpdate(m.index())) {
        auto st = pr->state();
        if (st == kStateProbe) {
          pr->BecomeReplicate();
        } else if (st == kStateSnapshot &&
                   pr->match() >= pr->pending_snapshot()) {
          // TODO: we should also entry this branch if a snapshot is received
          // that is below pr.pending_snapshot() but which makes it possible
          // to use the log again.
          JLOG_DEBUG << id_ << " recovered from needing snapshot, resumed "
                     << "sending replication messages to " << m.from();
          // Transition back to replicating state via probing state (which
          // takes the snapshot into account). If we didn't move to
          // replicating state, that would only happen with the next round
          // appends (but there may not be a next round for a while,
          // exposing an inconsistent RaftStatus).
          pr->BecomeProbe();
          pr->BecomeReplicate();
        } else if (st == kStateReplicate) {
          pr->mutable_inflights()->FreeLE(m.index());
        }

        if (MaybeCommit()) {
          // committed index has progressed for the term, so it is safe to
          // respond to pending read index requests.
          ReleasePendingReadIndexMessages();
          BcastAppend();
        } else if (old_paused) {
          // If we were paused before, this node may be missing the latest
          // commit index, so send it.
          SendAppend(m.from());
        }

        // We've updated flow control information above, which may allow us
        // to send multiple (size-limited) in-flight messages at once (such
        // as when transitioning from probe to replicate, or when FreeTo()
        // covers multiple messages). If we have more entries to send, send
        // as many messages as we can (without sending empty messages for
        // the commit index)
        if (id_ != m.from()) {
          while (MaybeSendAppend(m.from(), false)) {
          }
        }

        // Transfer leadership is in progress
        if (m.from() == leader_transferee_ &&
            pr->match() == raft_log_->LastIndex()) {
          JLOG_INFO << id_ << " send kMsgTimeoutNow to " << m.from()
                    << " after received kMsgAppResp";
          SendTimeoutNow(m.from());
        }
      }
    }
  } else if (type == kMsgHeartbeatResp) {
    pr->set_recent_active(true);
    pr->set_msg_app_flow_paused(false);

    // NB: if the follower is paused (full Inflights), this will still send an
    // empty append, allowing it to recover from situations in which all the
    // messages that filled up Inflights in the first place were dropped. Note
    // also that the outgoing heartbeat already communicated the commit index.
    if (pr->match() < raft_log_->LastIndex()) {
      SendAppend(m.from());
    }

    auto read_only = read_only_;
    if (read_only->option() != kReadOnlySafe || m.context().empty()) {
      return kOk;
    }

    if (read_only->RecvAck(m.from(), m.context()) < Quorum()) {
      return kOk;
    }

    std::vector<ReadIndexStatusPtr> rss;
    read_only->Advance(m, &rss);
    for (const auto& rs : rss) {
      auto resp = ResponseToReadIndexReq(rs->req, rs->index);
      if (resp.to() != kNone) {
        Send(resp);
      }
    }
  } else if (type == kMsgSnapStatus) {
    if (pr->state() != kStateSnapshot) {
      return kOk;
    }

    // TODO: this code is very similar to the snapshot handling in kMsgAppResp
    // above. In fact, the code there is more correct than the code here and
    // should likely be updated to match (or even better, the logic pulled
    // into a newly created Progress state machine handler).
    if (!m.reject()) {
      pr->BecomeProbe();
      JLOG_INFO << id_ << " snapshot succeeded, resume sending "
                << "replication message to " << m.from();
    } else {
      // NB: the ordered here matters or we'll be probing erroneously from
      // the snapshot index, but the snapshot never applied.
      pr->set_pending_snapshot(0);
      pr->BecomeProbe();
      JLOG_DEBUG << id_ << " snapshot failed, resumed sending "
                 << "replication messages to " << m.from();
    }
    // If snapshot finish, wait for the kMsgAppResp from the remote node
    // before sending out the next kMsgApp.
    // If snapshot failure, wait for a heartbeat interval before next try
    pr->set_msg_app_flow_paused(true);
  } else if (type == kMsgUnreachable) {
    // During optimistic replication, if the remote becomes unreachable,
    // there is huge probability that a kMsgApp is lost.
    if (pr->state() == kStateReplicate) {
      pr->BecomeProbe();
    }
    JLOG_DEBUG << id_ << " failed to send message to " << m.from()
               << " because it is unreachable";
  } else if (type == kMsgTransferLeader) {
    if (is_learner_) {
      JLOG_DEBUG << id_ << " is learner. Ignored transferring leadership";
      return kOk;
    }
    uint64_t leader_transferee = m.from();
    uint64_t last_leader_transferee = leader_transferee_;
    if (last_leader_transferee != kNone) {
      if (leader_transferee == last_leader_transferee) {
        JLOG_INFO << id_ << " [term " << term_ << "] transfer "
                  << "leadership to " << leader_transferee << " is in progress"
                  << ", ignores request to same node " << leader_transferee;
        return kOk;
      }
      AbortLeaderTransfer();
      JLOG_INFO << id_ << " [term " << term_ << "] abort previous"
                << " transferring leadership to " << last_leader_transferee;
    }
    if (leader_transferee == id_) {
      JLOG_INFO << id_ << " is already leader. Ignored transferring "
                << "leadership to " << last_leader_transferee;
      return kOk;
    }

    // Transfer leadership to third party
    JLOG_INFO << id_ << " [term " << term_ << "] starts to transfer "
              << "leadership to " << leader_transferee;
    // Transfer leadership should be finished in one election_timeout, so
    // reset election_elapsed.
    set_election_elapsed(0);
    set_leader_transferee(leader_transferee);
    if (pr->match() == raft_log_->LastIndex()) {
      SendTimeoutNow(leader_transferee);
      JLOG_INFO << id_ << " sends kMsgTimeoutNow to " << leader_transferee
                << " immediately as " << leader_transferee << " already "
                << "has up-to-date log";
    } else {
      SendAppend(leader_transferee);
    }
  }
  return kOk;
}

// StepCandidate is shared by kStateCandidate and kStatePreCandidate; the
// difference is whether they respond to kMsgVoteResp or kMsgPreVoteResp.
ErrNum Raft::StepCandidate(const Message& m) {
  // Only handle vote responses corresponding to out candidacy (while in
  // kStateCandidate, we may get stable kMsgPreVoteResp messages in this
  // term from out pre-candidate state).
  MessageType my_vote_resp_type;
  if (state_ == kStatePreCandidate) {
    my_vote_resp_type = kMsgPreVoteResp;
  } else {
    my_vote_resp_type = kMsgVoteResp;
  }

  auto type = m.type();
  if (type == kMsgProp) {
    JLOG_INFO << id_ << " no leader at term " << term_ << "; dropping proposal";
    return kErrProposalDropped;
  } else if (type == kMsgApp) {
    BecomeFollower(m.term(), m.from());  // always m.term() == term_
    HandleAppendEntries(m);
  } else if (type == kMsgHeartbeat) {
    BecomeFollower(m.term(), m.from());  // always m.term() == term_
    HandleHeartbeat(m);
  } else if (type == kMsgSnap) {
    BecomeFollower(m.term(), m.from());  // always m.term() == term_
    HandleSnapshot(m);
  } else if (type == my_vote_resp_type) {
    int gr = 0;
    int rj = 0;
    VoteResult res;
    Poll(m.from(), m.type(), !m.reject(), &gr, &rj, &res);
    JLOG_INFO << id_ << " has received " << gr << " " << MsgStr[type]
              << " votes and " << rj << " vote rejections";
    if (res == kVoteWon) {
      if (state_ == kStatePreCandidate) {
        Campaign(kCampaignElection);
      } else {
        BecomeLeader();
        BcastAppend();
      }
    } else if (res == kVoteLost) {
      // kMsgPreVoteResp contains future term of pre-candidate
      // m.term() > term_, reuse r.term()
      BecomeFollower(term_, kNone);
    }
  } else if (type == kMsgTimeoutNow) {
    JLOG_INFO << id_ << " [term " << term_ << " state " << state_
              << "] ignored kMsgTimeoutNow from " << m.from();
  }

  return kOk;
}

ErrNum Raft::StepFollower(const Message& m) {
  MessageType type = m.type();
  if (type == kMsgProp) {
    if (lead_ == kNone) {
      JLOG_INFO << id_ << " no leader at term " << term_
                << "; dropping proposal";
      return kErrProposalDropped;
    } else if (disable_proposal_forwarding_) {
      JLOG_INFO << id_ << " not forwarding to leader " << lead_ << " at term "
                << term_ << "; dropping proposal";
      return kErrProposalDropped;
    }
    Message new_m = m;
    new_m.set_to(lead_);
    Send(new_m);
  } else if (type == kMsgApp) {
    set_election_elapsed(0);
    set_lead(m.from());
    HandleAppendEntries(m);
  } else if (type == kMsgHeartbeat) {
    set_election_elapsed(0);
    set_lead(m.from());
    HandleHeartbeat(m);
  } else if (type == kMsgSnap) {
    set_election_elapsed(0);
    set_lead(m.from());
    HandleSnapshot(m);
  } else if (type == kMsgTransferLeader) {
    if (lead_ == kNone) {
      JLOG_INFO << id_ << " no leader at term " << term_
                << "; dropping leader transfer msg";
      return kOk;
    }
    Message new_m = m;
    new_m.set_to(lead_);
    Send(new_m);
  } else if (type == kMsgTimeoutNow) {
    JLOG_INFO << id_ << " [term " << term_ << "] received kMsgTimeoutNow from "
              << m.from() << " and starts an election to get leadership.";
    // Leadership transfers never use pre-vote even if pre_vote_ is true;
    // we know we are not recovering from a partition so there is no need
    // for the extra round trip.
    Hup(kCampaignTransfer);
  } else if (type == kMsgReadIndex) {
    if (lead_ == kNone) {
      JLOG_INFO << id_ << " no leader at term " << term_
                << "; dropping index reading msg";
      return kOk;
    }
    Message new_m = m;
    new_m.set_to(lead_);
    Send(new_m);
  } else if (type == kMsgReadIndexResp) {
    if (m.entries().size() != 1) {
      JLOG_ERROR << id_ << " invalid format of kMsgReadIndexResp from "
                 << m.from() << ", entries count: " << m.entries().size();
      return kOk;
    }
    read_states_.emplace_back(ReadState{m.index(), m.entries()[0].data()});
  }

  return kOk;
}

void Raft::Tick() {
  if (tick_func_ != nullptr) {
    tick_func_();
  } else {
    JLOG_WARN << "tick function is not set";
  }
}

// TickElection is run by followers and candidates after election_timeout_.
void Raft::TickElection() {
  election_elapsed_++;

  if (Promotable() && PastElectionTimeout()) {
    election_elapsed_ = 0;
    Message m;
    m.set_from(id_);
    m.set_type(kMsgHup);
    Step(m);
  }
}

// TickHeartbeat is run by leaders to send a kMsgBeat after heartbeat_timeout_
void Raft::TickHeartbeat() {
  heartbeat_elapsed_++;
  election_elapsed_++;

  if (election_elapsed_ >= election_timeout_) {
    election_elapsed_ = 0;
    if (check_quorum_) {
      Message m;
      m.set_from(id_);
      m.set_type(kMsgCheckQuorum);
      Step(m);
    }
    // If current leader cannot transfer leadership in election_timeout_,
    // it becomes leader again.
    if (state_ == kStateLeader && leader_transferee_ != kNone) {
      AbortLeaderTransfer();
    }
  }

  if (state_ != kStateLeader) {
    return;
  }

  if (heartbeat_elapsed_ >= heartbeat_timeout_) {
    heartbeat_elapsed_ = 0;
    Message m;
    m.set_from(id_);
    m.set_type(kMsgBeat);
    Step(m);
  }
}

void Raft::HandleAppendEntries(const Message& m) {
  if (m.index() < raft_log_->committed()) {
    Message resp_m;
    resp_m.set_to(m.from());
    resp_m.set_type(kMsgAppResp);
    resp_m.set_index(raft_log_->committed());
    Send(resp_m);
    return;
  }

  uint64_t last_i = 0;
  std::vector<EntryPtr> ents(m.entries().size());
  int i = 0;
  for (const auto& ent : m.entries()) {
    ents[i++] = std::make_shared<Entry>(ent);
  }
  bool ok = raft_log_->MaybeAppend(m.index(), m.log_term(), m.commit(), ents,
                                   &last_i);
  if (ok) {
    Message resp_m;
    resp_m.set_to(m.from());
    resp_m.set_type(kMsgAppResp);
    resp_m.set_index(last_i);
    Send(resp_m);
  } else {
    uint64_t t;
    ErrNum err = raft_log_->Term(m.index(), &t);
    JLOG_DEBUG << id_ << " [log_term "
               << raft_log_->ZeroTermOnErrCompacted(t, err) << ", index "
               << m.index() << "] rejected kMsgApp [log_term: " << m.log_term()
               << ", index " << m.index() << "] from " << m.from();

    // Return a hint to the leader about the maximum index and the term that
    // the two logs could be divergent at. Do this by searching through the
    // follower's log for the maximum (index, term) pair with a term <= the
    // kMsgApp's log_term and an index <= the kMsgApp's index. This can help
    // skip all indexes in the follower's uncommitted tail with terms greater
    // than kMsgApp's log_term;
    //
    // See the other caller for FindConflictByTerm (in StepLeader) for a much
    // more detailed explanation of this mechanism.
    uint64_t hint_index = std::min(m.index(), raft_log_->LastIndex());
    hint_index = raft_log_->FindConflictByTerm(hint_index, m.log_term());
    uint64_t hint_term = 0;
    err = raft_log_->Term(hint_index, &hint_term);
    if (err != kOk) {
      JLOG_FATAL << "term " << hint_index << " must be valid, but got " << err;
    }
    Message resp_m;
    resp_m.set_to(m.from());
    resp_m.set_type(kMsgAppResp);
    resp_m.set_index(m.index());
    resp_m.set_reject(true);
    resp_m.set_reject_hint(hint_index);
    resp_m.set_log_term(hint_term);
    Send(resp_m);
  }
}

void Raft::HandleHeartbeat(const Message& m) {
  raft_log_->CommitTo(m.commit());
  Message resp_m;
  resp_m.set_to(m.from());
  resp_m.set_type(kMsgHeartbeatResp);
  resp_m.set_context(m.context());
  Send(resp_m);
}

void Raft::HandleSnapshot(const Message& m) {
  // kMsgSnap should always carry a non-nil Snapshot, but err on the
  // side of safety and treat a nil Snapshot as a zero-valued Snapshot.
  uint64_t sindex = m.snapshot().metadata().index();
  uint64_t sterm = m.snapshot().metadata().term();
  if (Restore(m.snapshot())) {
    JLOG_INFO << id_ << " [commit: " << raft_log_->committed()
              << "] restored snapshot [index " << sindex << ", term " << sterm
              << "]";
    Message resp_m;
    resp_m.set_to(m.from());
    resp_m.set_type(kMsgAppResp);
    resp_m.set_index(raft_log_->LastIndex());
    Send(resp_m);
  } else {
    JLOG_INFO << id_ << " [commit: " << raft_log_->committed()
              << "] ignored snapshot [index " << sindex << ", term " << sterm
              << "]";
    Message resp_m;
    resp_m.set_to(m.from());
    resp_m.set_type(kMsgAppResp);
    resp_m.set_index(raft_log_->committed());
  }
}

// Restore recovers the state machine from a snapshot. It restores the log
// and the configuration of state machine. If this method returns false, the
// snapshot was ignored, either because it was obsolete or because of an err.
bool Raft::Restore(const Snapshot& s) {
  if (s.metadata().index() <= raft_log_->committed()) {
    return false;
  }

  if (state_ != kStateFollower) {
    // This is defense-in-depth: if the leader somehow ended up applying a
    // snapshot, it could move into a new term without moving into a follower
    // state. This should never fire, but if it did, we'd have prevented
    // damage by returning early, so log only a loud warning.
    //
    // At the time of writing, the instance is guaranteed to be in follower
    // state when this method is called.
    JLOG_WARN << id_ << " attempted to restore_test snapshot as leader;"
              << " should never happen";
    BecomeFollower(term_ + 1, kNone);
    return false;
  }

  // More defense-in-depth: throw away snapshot if recipient is not in the
  // config. This shouldn't ever happen (at the time of writing) but lots of
  // code here and there assumes that id_ is in the progress tracker.
  bool found = false;
  auto cs = s.metadata().conf_state();
  size_t n = cs.voters_size();
  for (size_t i = 0; i < n; i++) {
    uint64_t node = cs.voters(i);
    if (node == id_) {
      found = true;
      break;
    }
  }
  size_t m = cs.learners_size();
  for (size_t i = 0; i < m; i++) {
    uint64_t node = cs.learners(i);
    if (node == id_) {
      found = true;
      break;
    }
  }
  size_t k = cs.voters_outgoing_size();
  for (size_t i = 0; i < k; i++) {
    uint64_t node = cs.voters_outgoing(i);
    if (node == id_) {
      found = true;
      break;
    }
  }

  if (!found) {
    JLOG_WARN << id_ << " attempted to restore_test snapshot but it is in the"
              << " ConfState ; should never happened";
    return false;
  }

  // Now go ahead and actually restore_test.
  if (raft_log_->MatchTerm(s.metadata().index(), s.metadata().term())) {
    JLOG_INFO << id_ << " [commit: " << raft_log_->committed()
              << ", last_index: " << raft_log_->LastIndex()
              << ", last_term: " << raft_log_->LastTerm()
              << " fast-forwarded commit to snapshot"
              << " [index: " << s.metadata().index()
              << " ,term: " << s.metadata().term();
    raft_log_->CommitTo(s.metadata().index());
    return false;
  }

  SnapshotPtr snap = std::make_shared<Snapshot>(s);
  raft_log_->Restore(snap);

  // Reset the configuration and add the (potentially updated) peers in new.
  tracker_->ClearProgressMap();
  for (size_t i = 0; i < n; i++) {
    uint64_t node = cs.voters(i);
    uint64_t match = 0;
    uint64_t next = raft_log_->LastIndex() + 1;
    if (node == id_) {
      match = next - 1;
      is_learner_ = false;
    }
    tracker_->SetProgress(node, match, next, false);
  }
  for (size_t i = 0; i < m; i++) {
    uint64_t node = cs.learners(i);
    uint64_t match = 0;
    uint64_t next = raft_log_->LastIndex() + 1;
    if (node == id_) {
      match = next - 1;
      is_learner_ = true;
    }
    tracker_->SetProgress(node, match, next, true);
  }
  for (size_t i = 0; i < k; i++) {
    uint64_t node = cs.voters_outgoing(i);
    uint64_t match = 0;
    uint64_t next = raft_log_->LastIndex() + 1;
    if (node == id_) {
      match = next - 1;
      is_learner_ = false;
    }
    tracker_->SetProgress(node, match, next, false);
  }

  JLOG_INFO << id_ << " [commit: " << raft_log_->committed()
            << ", last_index: " << raft_log_->LastIndex()
            << ", last_term: " << raft_log_->LastTerm()
            << "] restored snapshot [index: " << s.metadata().index()
            << ", term: " << s.metadata().term() << "]";

  return true;
}

// Promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
bool Raft::Promotable() {
  auto pr = tracker_->mutable_progress(id_);
  return pr != nullptr && !is_learner_ && !raft_log_->HasPendingSnapshot();
}

void Raft::LoadState(const HardState& hs) {
  if (hs.commit() < raft_log_->committed() ||
      hs.commit() > raft_log_->LastIndex()) {
    JLOG_FATAL << id_ << "hs.commit() " << hs.commit() << " is out of range "
               << " [" << raft_log_->committed() << ", "
               << raft_log_->LastIndex() << "]";
  }
  raft_log_->set_committed(hs.commit());
  term_ = hs.term();
  vote_ = hs.vote();
}

void Raft::AddNode(uint64_t id, bool is_learner) {
  auto pr = tracker_->mutable_progress(id);
  if (pr == nullptr) {
    tracker_->InsertProgress(id, 0, raft_log_->LastIndex() + 1, is_learner);
    pr = tracker_->mutable_progress(id);
  } else {
    // if (is_learner && !pr->is_learner()) {
    //   // can only change learner to voter
    //   JLOG_INFO << id_ << " ignored add learner: don't support changing " <<
    //   id
    //             << " from voter to learner";
    //   return;
    // }

    // if (is_learner == pr->is_learner()) {
    //   // ignore any redundant add node calls (which can happen because the
    //   // initial bootstrapping entries are applied twice).
    //   return;
    // }

    // change learner to voter, use origin learner progress
    pr->set_is_learner(is_learner);
  }

  if (id_ == id) {
    is_learner_ = is_learner;
  }

  // when a node is first added, we should mark it as recently active.
  // Otherwise, CheckQuorum may cause us to step down if it is invoked before
  // the added node has a chance to communicate with us.
  pr->set_recent_active(true);
}

void Raft::RemoveNode(uint64_t id) {
  tracker_->DelProgress(id);
  // pending_conf_index_ = false;

  // do not try to commit or abort transferring if there is no nodes in the
  // cluster
  if (tracker_->progress_map().empty()) {
    JLOG_FATAL << "remove the last node in cluster";
    return;
  }

  // The quorum size is now smaller, so see if any pending entries can be
  // committed.
  if (MaybeCommit()) {
    BcastAppend();
  }

  // If the removed node is the lead_transferee, then abort the leadership
  // transferring.
  if (state_ == kStateLeader && leader_transferee_ == id) {
    AbortLeaderTransfer();
  }
}

// PastElectionTimeout return true iff election_elapsed_ is greater than or
// equal to the randomized election timeout in
// [election_timeout_, 2 * election_timeout_ - 1].
bool Raft::PastElectionTimeout() const {
  return election_elapsed_ >= randomized_election_timeout_;
}

void Raft::ResetRandomizedElectionTimeout() {
  randomized_election_timeout_ = election_timeout_ + rand() % election_timeout_;
}

void Raft::SendTimeoutNow(uint64_t to) {
  Message m;
  m.set_to(to);
  m.set_type(kMsgTimeoutNow);
  Send(m);
}

void Raft::AbortLeaderTransfer() { leader_transferee_ = kNone; }

// CommittedEntryInCurrentTerm return true if the peer has committed an entry
// in its term.
bool Raft::CommittedEntryInCurrentTerm() {
  uint64_t term = 0;
  ErrNum err = raft_log_->Term(raft_log_->committed(), &term);
  return raft_log_->ZeroTermOnErrCompacted(term, err) == term_;
}

// ResponseToReadIndexReq constructs a response for 'req'. If 'req' comes from
// the peer itself, a blank value will be returned.
Message Raft::ResponseToReadIndexReq(const Message& req, uint64_t read_index) {
  Message m;
  if (req.from() == kNone || req.from() == id_) {
    ReadState rs{read_index, req.entries()[0].data()};
    read_states_.push_back(rs);
    return m;
  }

  m.set_type(kMsgReadIndexResp);
  m.set_to(m.from());
  m.set_index(read_index);
  auto ents = m.mutable_entries();
  ents->CopyFrom(req.entries());
  return m;
}

// IncreaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its max_uncommitted_size_
// limit. If the new entries would exceed the limit, the method returns false.
// If not, the increase in uncommitted entry is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, and leaving a joint configuration.
bool Raft::IncreaseUncommittedSize(const std::vector<EntryPtr>& ents) {
  uint64_t s = PayloadsSize(ents);

  if (uncommitted_size_ > 0 && s > 0 &&
      uncommitted_size_ + s > max_uncommitted_size_) {
    return false;
  }
  uncommitted_size_ += s;
  return true;
}

// ReduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
void Raft::ReduceUncommittedSize(const std::vector<EntryPtr>& ents) {
  if (uncommitted_size_ == 0) {
    return;
  }

  uint64_t s = PayloadsSize(ents);
  if (s > uncommitted_size_) {
    // uncommitted_size_ may underestimate the size of the uncommitted Raft log
    // tail but will never overestimate it. Saturate at 0 instead of allowing
    // overflow.
    uncommitted_size_ = 0;
  } else {
    uncommitted_size_ -= s;
  }
}

void Raft::ReduceUncommittedSize(const std::vector<Entry>& ents) {
  if (uncommitted_size_ == 0) {
    return;
  }

  uint64_t s = std::accumulate(
      ents.begin(), ents.end(), 0,
      [](uint64_t a, const Entry& e) { return a + e.data().size(); });

  if (s > uncommitted_size_) {
    // uncommitted_size_ may underestimate the size of the uncommitted Raft log
    // tail but will never overestimate it. Saturate at 0 instead of allowing
    // overflow.
    uncommitted_size_ = 0;
  } else {
    uncommitted_size_ -= s;
  }
}

int Raft::NumOfPendingConf(const std::vector<EntryPtr>& ents) {
  return std::count_if(ents.begin(), ents.end(), [](const EntryPtr& e) {
    return e->type() == kEntryConfChange;
  });
}

void Raft::ReleasePendingReadIndexMessages() {
  if (!CommittedEntryInCurrentTerm()) {
    JLOG_ERROR << "pending MsgReadIndex should be released only after first"
               << "commit in current term";
    return;
  }

  for (const auto& m : pending_read_index_message_) {
    SendMsgReadIndexResponse(m);
  }

  pending_read_index_message_.clear();
}

void Raft::SendMsgReadIndexResponse(const Message& m) {
  // Thinking: use an internally defined context instead of the user given
  // context. We can express this in terms of the term and index instead of
  // a user-supplied value. This would allow multiple reads to piggyback
  // on the same message.
  // If more than the local vote is needed, go through a full broadcast.
  if (read_only_->option() == kReadOnlySafe) {
    read_only_->AddRequest(raft_log_->committed(), m);
    // The local node automatically acks the request.
    read_only_->RecvAck(id_, m.entries(0).data());
    BcastHeartbeatWithCtx(m.entries(0).data());
  } else if (read_only_->option() == kReadOnlyLeaseBased) {
    auto resp = ResponseToReadIndexReq(m, raft_log_->committed());
    if (resp.to() != kNone) {
      Send(resp);
    }
  }
}

void Raft::AppendPendingReadIndexMessages(const Message& m) {
  pending_read_index_message_.push_back(m);
}

size_t Raft::Quorum() const { return tracker_->progress_map().size() / 2 + 1; }

void Raft::AppendReadState(const ReadState& rs) { read_states_.push_back(rs); }

void Raft::ReadMessages(std::vector<Message>* msgs) {
  if (msgs != nullptr) {
    *msgs = msgs_;
  }
  msgs_.clear();
}

void Raft::EraseReadStatus(size_t i) {
  if (pending_read_index_message_.size() <= i) {
    return;
  }
  pending_read_index_message_.erase(pending_read_index_message_.begin() + i);
}

void Raft::ClearReadStates() { read_states_.clear(); }

}  // namespace jraft
