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

#include <jrpc/base/logging/logging.h>

#include <functional>

#include "common/conf.h"
#include "errnum.pb.h"
#include "raft_log.h"
#include "read_only.h"
#include "tracker/tracker.h"

namespace jraft {

class Ready;
using ReadyPtr = std::shared_ptr<Ready>;
class Storage;
using StoragePtr = std::shared_ptr<Storage>;
class ReadState;

// Config contains the parameters to start a raft.
struct Config {
  // id is the identity of the local raft, id cannot be 0.
  uint64_t id = kNone;

  // election_tick is the number of Node.Tick() invocations that must pass
  // between elections. That is, if a follower does not receive any message
  // from the leader of current term before election_tick has elapsed, it
  // will become candidate and start an election. election_tick must be
  // greater than heartbeat_tick. We suggest
  // election_tick = 10 * heartbeat_tick to avoid unnecessary leader
  // switching.
  int election_tick = 0;
  // heartbeat_tick is the number of Node.Tick() invocations that must pass
  // between heartbeats. That is, a leader sends heartbeat messages to
  // maintain its leadership every heartbeat_tick ticks.
  int heartbeat_tick = 0;

  // storage is the storage for raft. raft generates entries and states to
  // be stored in storage. raft reads the persisted entries and states out
  // of storage when it needs. raft reads out the previous state and
  // configuration out of storage when restarting.
  StoragePtr storage = nullptr;
  // applied is the last applied index. It should only be set when restarting
  // raft. Raft will not return entries to the application smaller or equal
  // to applied. If applied is unset when restarting, raft might return
  // previous applied entries. This is a very application dependent
  // configuration.
  uint64_t applied = 0;

  // max_size_per_msg limits the max byte size of each append message. Smaller
  // value lowers the raft recovery cost (initial probing and message lost
  // during normal operation). On the other side, it might affect the
  // throughput during normal replication. Note: UINT64_MAX for unlimited,
  // 0 for at most one entry per message.
  uint64_t max_size_per_msg = UINT64_MAX;
  // max_committed_size_per_ready limits the size of the committed entries
  // which can be applied.
  uint64_t max_committed_size_per_ready = UINT64_MAX;
  // max_uncommitted_entries_size limits the aggregate byte size of the
  // uncommitted entries that may be appended to a leader's log. Once this
  // limit is exceeded. proposals will begin to return kErrProposalDrapped
  // errors. Note: 0 for no limit.
  uint64_t max_uncommitted_entries_size = 0;
  // max_inflight_msgs limits the max number of in-flight append messages
  // during optimistic replication phase. The application transportation
  // layer usually has its own sending buffer over TCP/UDP. Setting
  // max_inflight_msgs to avoid overflowing that sending buffer.
  size_t max_inflight_msgs = SIZE_MAX;
  // max_inflight_bytes limits the number of in-flight bytes in append
  // messages. Complements max_inflight_msgs. Ignore if zero.
  //
  // This effectively bounds the bandwidth-delay product. Note that especially
  // in high-latency deployments setting this too low can lead to dramatic
  // reduction in throughout. For example, with a peer that has a round-trip
  // latency of 100ms to the leader and this setting is set to 1 MB, there is
  // a throughput limit of 10 MB/s for this group. With RTT of 400ms, this
  // drops to 2.5 MB/s. See Little's law to understand the maths behind.
  uint64_t max_inflight_bytes = 0;

  // check_quorum specifies if the leader should check quorum activity.
  // Leader steps down when quorum is not active for an election_timeout.
  bool check_quorum = false;

  // pre_vote enables the Pre-Vote algorithm described in raft thesis
  // section 9.6. This prevents disruption when a node that has been
  // partitioned away rejoins the cluster.
  bool pre_vote = false;

  // ReadOnlyOption specifies how the read only request is processed.
  //
  // kReadOnlySafe guarantees the linearizability of the read only request by
  // communicating with the quorum. It is the default and suggested option.
  //
  // kReadOnlyLeaseBased ensures linearizability of the read only request by
  // relying on the leader lease. It can be affected by clock drift.
  // If the clock drift is unbounded, leader might keep the lease longer than
  // it should (clock can move backward/pause without any bound). ReadIndex is
  // not safe in that case.
  ReadOnlyOption read_only_option = kReadOnlySafe;

  // disable_proposal_forwarding set to true means that followers will drop
  // proposals, rather than forwarding them to the leader. One use case for
  // this feature would be in a situation where the Raft leader is used to
  // compute the date of a proposal, for example, adding a timestamp from a
  // hybrid logical clock data in a monotonically increasing way. Forwarding
  // should be disabled to prevent a follower with an inaccurate hybrid logical
  // clock from assigning the timestamp and then forwarding the data to the
  // leader.
  bool disable_proposal_forwarding = false;
};

class SoftState;
class ProgressTracker;
class Message;

class Raft : public Noncopyable {
 public:
  typedef std::function<ErrNum(const Message&)> StepFunc;
  typedef std::function<void()> TickFunc;

  explicit Raft(Config* c);
  ~Raft();

  bool HasLeader() const;

  SoftState ConstructSoftState();
  HardState ConstructHardState();
  ConfState ConstructConfState() const;

  void Send(Message m);
  void SendAppend(uint64_t to);
  bool MaybeSendAppend(uint64_t to, bool send_if_empty);
  void SendHeartbeat(uint64_t to, const std::string& ctx);

  void BcastAppend();
  void BcastHeartbeat();
  void BcastHeartbeatWithCtx(const std::string& ctx);

  void Advance(const ReadyPtr& rd);

  bool MaybeCommit();

  void Reset(uint64_t term);

  bool AppendEntry(std::vector<Entry>& es);

  void BecomeFollower(uint64_t term, uint64_t lead);
  void BecomeCandidate();
  void BecomePreCandidate();
  void BecomeLeader();

  void Hup(CampaignType t);

  void Campaign(CampaignType t);

  void Poll(uint64_t id, MessageType t, bool v, int* granted, int* rejected,
            VoteResult* vote_result);

  ErrNum Step(const Message& m);

  void Tick();

  void HandleAppendEntries(const Message& m);
  void HandleHeartbeat(const Message& m);
  void HandleSnapshot(const Message& m);

  bool Restore(const Snapshot& s);

  bool Promotable();

  // TODO: add SwitchToConfig
  void AddNode(uint64_t id, bool is_learner = false);
  void RemoveNode(uint64_t id);

  void LoadState(const HardState& hs);

  bool PastElectionTimeout() const;
  void ResetRandomizedElectionTimeout();
  void SendTimeoutNow(uint64_t to);

  void AbortLeaderTransfer();

  bool CommittedEntryInCurrentTerm();
  Message ResponseToReadIndexReq(const Message& req, uint64_t read_index);
  bool IncreaseUncommittedSize(const std::vector<EntryPtr>& ents);
  void ReduceUncommittedSize(const std::vector<EntryPtr>& ents);
  void ReduceUncommittedSize(const std::vector<Entry>& ents);
  static int NumOfPendingConf(const std::vector<EntryPtr>& ents);

  void ReleasePendingReadIndexMessages();
  void SendMsgReadIndexResponse(const Message& m);
  void AppendPendingReadIndexMessages(const Message& m);
  size_t Quorum() const;
  void AppendReadState(const ReadState& rs);
  void ReadMessages(std::vector<Message>* msgs = nullptr);
  void EraseReadStatus(size_t i);
  void ClearReadStates();

  uint64_t id() const { return id_; }
  void set_id(uint64_t id) { id_ = id; }

  uint64_t term() const { return term_; }
  void set_term(uint64_t term) { term_ = term; }

  uint64_t vote() const { return vote_; }
  void set_vote(uint64_t vote) { vote_ = vote; }

  const std::vector<ReadState>& read_states() const { return read_states_; }
  std::vector<ReadState>& mutable_read_states() { return read_states_; }

  RaftLog* raft_log() const { return raft_log_; }
  RaftLog* mutable_raft_log() { return raft_log_; }
  void set_raft_log(RaftLog* raft_log) {
    if (raft_log_ != nullptr) {
      delete raft_log_;
    }
    raft_log_ = raft_log;
  }

  uint64_t max_msg_size() const { return max_msg_size_; }
  void set_max_msg_size(uint64_t max_msg_size) { max_msg_size_ = max_msg_size; }

  uint64_t max_uncommitted_size() const { return max_uncommitted_size_; }
  void set_max_uncommitted_size(uint64_t max_uncommitted_size) {
    max_uncommitted_size_ = max_uncommitted_size;
  }

  ProgressTracker* tracker() const { return tracker_; }
  ProgressTracker* mutable_tracker() { return tracker_; }

  const RaftStateType& state() const { return state_; }
  void set_state(const RaftStateType& state) { state_ = state; }

  bool is_learner() const { return is_learner_; }
  void set_is_learner(bool is_learner) { is_learner_ = is_learner; }

  const std::vector<Message>& msgs() const { return msgs_; }
  std::vector<Message>& mutable_msgs() { return msgs_; }
  void set_msgs(const std::vector<Message>& msgs) { msgs_ = msgs; }

  uint64_t lead() const { return lead_; }
  void set_lead(uint64_t lead) { lead_ = lead; }

  uint64_t leader_transferee() const { return leader_transferee_; }
  void set_leader_transferee(uint64_t leader_transferee) {
    leader_transferee_ = leader_transferee;
  }

  uint64_t pending_conf_index() const { return pending_conf_index_; }
  void set_pending_conf_index(uint64_t pending_conf_index) {
    pending_conf_index_ = pending_conf_index;
  }

  uint64_t uncommitted_size() const { return uncommitted_size_; }
  void set_uncommitted_size(uint64_t uncommitted_size) {
    uncommitted_size_ = uncommitted_size;
  }

  ReadOnly* read_only() const { return read_only_; }
  ReadOnly* mutable_read_only() const { return read_only_; }

  int election_elapsed() const { return election_elapsed_; }
  void set_election_elapsed(int election_elapsed) {
    election_elapsed_ = election_elapsed;
  }

  int heartbeat_elapsed() const { return heartbeat_elapsed_; }
  void set_heartbeat_elapsed(int heartbeat_elapsed) {
    heartbeat_elapsed_ = heartbeat_elapsed;
  }

  bool check_quorum() const { return check_quorum_; }
  void set_check_quorum(bool check_quorum) { check_quorum_ = check_quorum; }

  bool pre_vote() const { return pre_vote_; }
  void set_pre_vote(bool pre_vote) { pre_vote_ = pre_vote; }

  int heartbeat_timeout() const { return heartbeat_timeout_; }
  void set_heartbeat_timeout(int heartbeat_timeout) {
    heartbeat_timeout_ = heartbeat_timeout;
  }

  int election_timeout() const { return election_timeout_; }
  void set_election_timeout(int election_timeout) {
    election_timeout_ = election_timeout;
  }

  int randomized_election_timeout() const {
    return randomized_election_timeout_;
  }
  void set_randomized_election_timeout(int randomized_election_timeout) {
    randomized_election_timeout_ = randomized_election_timeout;
  }

  bool disable_proposal_forwarding() const {
    return disable_proposal_forwarding_;
  }
  void set_disable_proposal_forwarding(bool disable_proposal_forwarding) {
    disable_proposal_forwarding_ = disable_proposal_forwarding;
  }

  const std::vector<Message>& pending_read_index_message() const {
    return pending_read_index_message_;
  }
  void set_pending_read_index_message(
      const std::vector<Message>& pending_read_index_message) {
    pending_read_index_message_ = pending_read_index_message;
  }

  StoragePtr mutable_storage() { return raft_log_->mutable_storage(); }

  StepFunc step_func() const { return step_func_; }
  void set_step_func() {
    switch (state_) {
      case kStateCandidate:
      case kStatePreCandidate:
        step_func_ =
            std::bind(&Raft::StepCandidate, this, std::placeholders::_1);
        break;
      case kStateFollower:
        step_func_ =
            std::bind(&Raft::StepFollower, this, std::placeholders::_1);
        break;
      case kStateLeader:
        step_func_ = std::bind(&Raft::StepLeader, this, std::placeholders::_1);
        break;
      default:
        JLOG_FATAL << "shouldn't come here";
        break;
    }
  }

 private:
  void TickElection();
  void TickHeartbeat();

  ErrNum StepLeader(const Message& m);
  ErrNum StepCandidate(const Message& m);
  ErrNum StepFollower(const Message& m);

  uint64_t id_;

  uint64_t term_;
  uint64_t vote_;

  std::vector<ReadState> read_states_;

  RaftLog* raft_log_;

  uint64_t max_msg_size_;
  uint64_t max_uncommitted_size_;
  ProgressTracker* tracker_;

  RaftStateType state_;

  // is_learner_ is true if the local node is a learner.
  bool is_learner_;

  std::vector<Message> msgs_;

  // the leader id
  uint64_t lead_;
  // leader_transferee_ is id of the leader transfer target when its value is
  // not zero. Follow the procedure defined in raft thesis 3.10.
  uint64_t leader_transferee_;
  // Only one conf change may be pending (in the log, but not yet applied)
  // at a time. This is enforced via pending_conf_index_, which is set to
  // a value >= the log index of the latest pending configuration change
  // (if any). Config changes are only allowed to be proposed if the leader's
  // applied index is greater than this value.
  uint64_t pending_conf_index_;
  // an estimate of the size of the uncommitted tail of the Raft log. Used to
  // prevent unbounded log growth. Only maintained by the leader. Reset on
  // term changes.
  uint64_t uncommitted_size_;

  ReadOnly* read_only_;

  // number of ticks since it reached last election_timeout_ when it is leader
  // or candidate.
  // number of ticks since it reached last election_timeout_ or received a
  // valid message from current leader when it is a follower.
  int election_elapsed_;

  // number of ticks since it reached last heartbeat_timeout_ only leader
  // keeps heartbeat_elapsed_.
  int heartbeat_elapsed_;

  bool check_quorum_;
  bool pre_vote_;

  int heartbeat_timeout_;
  int election_timeout_;
  // randomized_election_timeout_ is a random number between
  // [election_timeout_, 2 * election_timeout_ - 1]. It gets reset when raft
  // changes its state to follower or candidate.
  int randomized_election_timeout_ = 0;
  bool disable_proposal_forwarding_;

  // pending_read_index_messages_ is used to store messages of type
  // kMsgReadIndex that can't be answered as new leader didn't commit any log in
  // current term. Those will be handled as fast as first log is committed in
  // current term.
  std::vector<Message> pending_read_index_message_;

  StepFunc step_func_;
  TickFunc tick_func_;
};

using RaftPtr = std::shared_ptr<Raft>;

}  // namespace jraft
