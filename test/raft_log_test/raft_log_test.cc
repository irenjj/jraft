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
#include <gtest/gtest.h>

#include "common/util.h"
#include "test_util/test_util.h"
#include "memory_storage.h"
#include "raft_log.h"

namespace jraft {

TEST(LogTest, TestFindConflict) {
  std::vector<EntryPtr> prev_ents = {NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)};
  struct TestArgs {
    std::vector<EntryPtr> ents;

    uint64_t wconflict;
  } tests[] = {
      // no conflict, empty ent
      {{}, 0},
      // no conflict
      {{NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)}, 0},
      {{NewEnt(2, 2), NewEnt(3, 3)}, 0},
      {{NewEnt(3, 3)}, 0},
      // no conflict, but has new entries
      {{NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 4)},
       4},
      {{NewEnt(2, 2), NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 4)}, 4},
      {{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 4)}, 4},
      {{NewEnt(4, 4), NewEnt(5, 4)}, 4},
      // conflicts with existing entreis
      {{NewEnt(1, 4), NewEnt(2, 4)}, 1},
      {{NewEnt(2, 1), NewEnt(3, 4), NewEnt(4, 4)}, 2},
      {{NewEnt(3, 1), NewEnt(4, 2), NewEnt(5, 4), NewEnt(6, 4)}, 3},
  };

  int i = 0;
  for (const auto& tt : tests) {
    StoragePtr ms = std::make_shared<MemoryStorage>();
    RaftLog* raft_log = new RaftLog(ms);
    raft_log->Append(prev_ents);

    uint64_t gconflict = raft_log->FindConflict(tt.ents);
    EXPECT_EQ(gconflict, tt.wconflict) << "i: " << i;

    delete raft_log;

    i++;
  }
}

TEST(LogTest, TestIsUpToDate) {
  std::vector<EntryPtr> prev_ents = {NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)};
  StoragePtr ms = std::make_shared<MemoryStorage>();

  RaftLog* raft_log = new RaftLog(ms);
  raft_log->Append(prev_ents);
  struct TestArgs {
    uint64_t last_index;
    uint64_t term;

    bool w_uptodate;
  } tests[] = {
      // greater term, ignore last_index
      {raft_log->LastIndex() - 1, 4, true},
      {raft_log->LastIndex(), 4, true},
      {raft_log->LastIndex() + 1, 4, true},
      // smaller term, ignore last_index
      {raft_log->LastIndex() - 1, 2, false},
      {raft_log->LastIndex(), 2, false},
      {raft_log->LastIndex() + 1, 2, false},
      // equal term, equal or larger last_index wins
      {raft_log->LastIndex() - 1, 3, false},
      {raft_log->LastIndex(), 3, true},
      {raft_log->LastIndex() + 1, 3, true},
  };

  int i = 0;
  for (const auto& tt : tests) {
    bool g_uptodate = raft_log->IsUpToDate(tt.last_index, tt.term);
    EXPECT_EQ(g_uptodate, tt.w_uptodate) << "i: " << i;
    i++;
  }

  delete raft_log;
}

TEST(LogTest, TestAppend) {
  std::vector<EntryPtr> prev_ents = {NewEnt(1, 1), NewEnt(2, 2)};
  struct TestArgs {
    std::vector<EntryPtr> ents;

    uint64_t windex;
    std::vector<EntryPtr> wents;
    uint64_t wunstable;
  } tests[] = {
      {
          {},
          2,
          {NewEnt(1, 1), NewEnt(2, 2)},
          3,
      },
      {
          {NewEnt(3, 2)},
          3,
          {NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 2)},
          3,
      },
      // conflict with index 1
      {
          {NewEnt(1, 2)},
          1,
          {NewEnt(1, 2)},
          1,
      },
      // conflict with index 2
      {
          {NewEnt(2, 3), NewEnt(3, 3)},
          3,
          {NewEnt(1, 1), NewEnt(2, 3), NewEnt(3, 3)},
          2,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();
    ms->Append(prev_ents);
    RaftLog* raft_log = new RaftLog(ms);

    uint64_t index = raft_log->Append(tt.ents);
    EXPECT_EQ(index, tt.windex) << "i: " << i;

    std::vector<EntryPtr> g;
    ErrNum err = raft_log->Entries(1, kNoLimit, &g);
    EXPECT_EQ(err, kOk) << "i: " << i;
    EXPECT_TRUE(EntryDeepEqual(g, tt.wents)) << "i: " << i;

    EXPECT_EQ(raft_log->unstable().offset(), tt.wunstable) << "i: " << i;

    delete raft_log;

    i++;
  }
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
//  1. If an existing entry conflicts with a new one (small
//     index but different terms), delete the existing entry
//     and all that follow it
//  2. Append any new entries not already in the log
// If the given (index, term) does not match with the existing
//  logs: return false
TEST(LogTest, TestLogMaybeAppend) {
  std::vector<EntryPtr> prev_ents = {NewEnt(1, 1), NewEnt(2, 2), NewEnt(3, 3)};
  uint64_t last_index = 3;
  uint64_t last_term = 3;
  uint64_t commit = 1;

  struct TestArgs {
    uint64_t log_term;
    uint64_t index;
    uint64_t committed;
    std::vector<EntryPtr> ents;

    uint64_t wlasti;
    uint64_t wappend;
    uint64_t wcommit;
    uint64_t wpanic;
  } tests[] = {
      // not match: term is different
      {
          last_term - 1,
          last_index,
          last_index,
          {NewEnt(last_index + 1, 4)},
          0,
          false,
          commit,
          false,
      },
      // not match: index out of bound
      {
          last_term,
          last_index + 1,
          last_index,
          {NewEnt(last_index + 2, 4)},
          0,
          false,
          commit,
          false,
      },
      // match with the last existing entry
      {
          last_term,
          last_index,
          last_index,
          {},
          last_index,
          true,
          last_index,
          false,
      },
      {
          last_term,
          last_index,
          last_index,
          {},
          last_index,
          true,
          last_index,
          false,
      },
      {
          last_term,
          last_index,
          last_index - 1,
          {},
          last_index,
          true,
          last_index - 1,
          false,
      },
      {
          last_term,
          last_index,
          0,
          {},
          last_index,
          true,
          commit,
          false,
      },
      {
          0,
          0,
          last_index,
          {},
          0,
          true,
          commit,
          false,
      },
      {
          last_term,
          last_index,
          last_index,
          {NewEnt(last_index + 1, 4)},
          last_index + 1,
          true,
          last_index,
          false,
      },
      {
          last_term,
          last_index,
          last_index + 1,
          {NewEnt(last_index + 1, 4)},
          last_index + 1,
          true,
          last_index + 1,
          false,
      },
      {
          last_term,
          last_index,
          last_index + 2,
          {NewEnt(last_index + 1, 4)},
          last_index + 1,
          true,
          last_index + 1,
          false,
      },
      {
          last_term,
          last_index,
          last_index + 2,
          {NewEnt(last_index + 1, 4), NewEnt(last_index + 2, 4)},
          last_index + 2,
          true,
          last_index + 2,
          false,
      },
      // match with the entry in the middle
      {
          last_term - 1,
          last_index - 1,
          last_index,
          {NewEnt(last_index, 4)},
          last_index,
          true,
          last_index,
          false,
      },
      {
          last_term - 2,
          last_index - 2,
          last_index,
          {NewEnt(last_index - 1, 4)},
          last_index - 1,
          true,
          last_index - 1,
          false,
      },
      // {
      //   last_term - 3, last_index - 3, last_index,
      //   { NewEnt(last_index - 2, 4) },
      //   last_index - 2, true, last_index - 2, true,
      // },
      {
          last_term - 2,
          last_index - 2,
          last_index,
          {NewEnt(last_index - 1, 4), NewEnt(last_index, 4)},
          last_index,
          true,
          last_index,
          false,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    StoragePtr ms = std::make_shared<MemoryStorage>();

    RaftLog* raft_log = new RaftLog(ms);
    raft_log->Append(prev_ents);
    raft_log->set_committed(commit);

    uint64_t g_lasti;
    bool g_append = raft_log->MaybeAppend(tt.index, tt.log_term, tt.committed,
                                          tt.ents, &g_lasti);
    uint64_t g_commit = raft_log->committed();

    EXPECT_EQ(g_lasti, tt.wlasti) << "i: " << i;
    EXPECT_EQ(g_append, tt.wappend) << "i: " << i;
    EXPECT_EQ(g_commit, tt.wcommit) << "i: " << i;
    if (g_append && tt.ents.size() != 0) {
      std::vector<EntryPtr> gents;
      ErrNum err = raft_log->Slice(raft_log->LastIndex() - tt.ents.size() + 1,
                                   raft_log->LastIndex() + 1, kNoLimit, &gents);
      EXPECT_EQ(err, kOk) << "i: " << i;
      EXPECT_TRUE(EntryDeepEqual(gents, tt.ents)) << "i: " << i;
    }

    delete raft_log;

    i++;
  }
}

// TestCompactionSideEffects ensures that all the log related functionally
// works correctly after a compaction.
TEST(LogTest, TestCompactionSideEffects) {
  uint64_t i;
  uint64_t j;
  // populate the log with 1000 entries, 750 in stable storage
  // and 250 in unstable
  uint64_t last_index = 1000;
  uint64_t unstable_index = 750;
  uint64_t last_term = last_index;
  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();
  for (i = 1; i <= unstable_index; i++) {
    ms->Append({NewEnt(i, i)});
  }

  RaftLog* raft_log = new RaftLog(ms);
  for (i = unstable_index; i < last_index; i++) {
    raft_log->Append({NewEnt(i + 1, i + 1)});
  }

  EXPECT_TRUE(raft_log->MaybeCommit(last_index, last_term));
  raft_log->AppliedTo(raft_log->committed());

  uint64_t offset = 500;
  ms->Compact(offset);

  EXPECT_EQ(raft_log->LastIndex(), last_index);

  for (j = offset; j < raft_log->LastIndex(); j++) {
    uint64_t term;
    ErrNum err = raft_log->Term(j, &term);
    EXPECT_EQ(err, kOk);
    EXPECT_EQ(term, j);
  }

  for (j = offset; j < raft_log->LastIndex(); j++) {
    EXPECT_TRUE(raft_log->MatchTerm(j, j));
  }

  std::vector<EntryPtr> unstable_ents;
  raft_log->UnstableEntries(&unstable_ents);
  EXPECT_EQ(unstable_ents.size(), 250);
  EXPECT_EQ(unstable_ents[0]->index(), 751);

  uint64_t prev = raft_log->LastIndex();
  raft_log->Append(
      {NewEnt(raft_log->LastIndex() + 1, raft_log->LastIndex() + 1)});
  EXPECT_EQ(raft_log->LastIndex(), prev + 1);

  std::vector<EntryPtr> ents;
  ErrNum err = raft_log->Entries(raft_log->LastIndex(), kNoLimit, &ents);
  EXPECT_EQ(err, kOk);
  EXPECT_EQ(ents.size(), 1);

  delete raft_log;
}

TEST(LogTest, TestHasNextEnts) {
  auto snap = NewSnap(3, 1);
  std::vector<EntryPtr> ents = {
      NewEnt(4, 1),
      NewEnt(5, 1),
      NewEnt(6, 1),
  };
  struct TestArgs {
    uint64_t applied;
    bool has_next;
  } tests[] = {
      {
          0,
          true,
      },
      {
          3,
          true,
      },
      {
          4,
          true,
      },
      {
          5,
          false,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();
    ms->ApplySnapshot(snap);
    RaftLog raft_log(ms);
    raft_log.Append(ents);
    raft_log.MaybeCommit(5, 1);
    raft_log.AppliedTo(tt.applied);

    EXPECT_EQ(raft_log.HasNextEnts(), tt.has_next) << "i: " << i;

    i++;
  }
}

TEST(LogTest, TestNextEnts) {
  auto snap = NewSnap(3, 1);
  std::vector<EntryPtr> ents = {
      NewEnt(4, 1),
      NewEnt(5, 1),
      NewEnt(6, 1),
  };
  struct TestArgs {
    uint64_t applied;

    std::vector<EntryPtr> wents;
  } tests[] = {
      {
          0,
          std::vector<EntryPtr>(ents.begin(), ents.begin() + 2),
      },
      {
          3,
          std::vector<EntryPtr>(ents.begin(), ents.begin() + 2),
      },
      {
          4,
          std::vector<EntryPtr>(ents.begin() + 1, ents.begin() + 2),
      },
      {
          5,
          {},
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();
    ms->ApplySnapshot(snap);
    RaftLog* raft_log = new RaftLog(ms);
    raft_log->Append(ents);
    raft_log->MaybeCommit(5, 1);
    raft_log->AppliedTo(tt.applied);

    std::vector<EntryPtr> nents;
    raft_log->NextEnts(&nents);
    EXPECT_TRUE(EntryDeepEqual(nents, tt.wents)) << "i: " << i;

    delete raft_log;

    i++;
  }
}

// TestUnstableEnts ensures unstable entries returns the unstable
// part of the entries correctly.
TEST(LogTest, TestUnstableEnts) {
  std::vector<EntryPtr> prev_ents = {
      NewEnt(1, 1),
      NewEnt(2, 2),
  };
  struct TestArgs {
    uint64_t unstable;

    std::vector<EntryPtr> wents;
  } tests[] = {
      {3, {}},
      {
          1,
          prev_ents,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    // append stable entries to storage
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

    ms->Append(std::vector<EntryPtr>(prev_ents.begin(),
                                     prev_ents.begin() + tt.unstable - 1));

    // append unstable entries to raft_log
    auto raft_log = new RaftLog(ms);
    raft_log->Append(std::vector<EntryPtr>(prev_ents.begin() + tt.unstable - 1,
                                           prev_ents.end()));

    std::vector<EntryPtr> ents;
    raft_log->UnstableEntries(&ents);
    if (ents.size() > 0) {
      raft_log->StableTo(ents.back()->index(), ents.back()->term());
    }
    EXPECT_TRUE(EntryDeepEqual(ents, tt.wents)) << "i: " << i;
    uint64_t w = prev_ents.back()->index() + 1;
    EXPECT_EQ(raft_log->unstable().offset(), w) << "i: " << i;

    delete raft_log;

    i++;
  }
}

TEST(LogTest, TestCommitTo) {
  std::vector<EntryPtr> prev_ents = {
      NewEnt(1, 1),
      NewEnt(2, 2),
      NewEnt(3, 3),
  };
  uint64_t commit = 2;
  struct TestArgs {
    uint64_t commit;

    uint64_t wcommit;
    bool wpanic;
  } tests[] = {
      {3, 3, false}, {1, 2, false},
      // { 4, 0, true },
  };

  int i = 0;
  for (const auto& tt : tests) {
    StoragePtr ms = std::make_shared<MemoryStorage>();

    auto raft_log = new RaftLog(ms);
    raft_log->Append(prev_ents);
    raft_log->set_committed(commit);
    raft_log->CommitTo(tt.commit);
    EXPECT_EQ(raft_log->committed(), tt.wcommit) << "i: " << i;

    delete raft_log;

    i++;
  }
}

TEST(LogTest, TestStableTo) {
  struct TestArgs {
    uint64_t stablei;
    uint64_t stablet;

    uint64_t wunstable;
  } tests[] = {
      {
          1,
          1,
          2,
      },
      {
          2,
          2,
          3,
      },
      {
          2,
          1,
          1,
      },  // bad term
      {
          3,
          1,
          1,
      },  // bad index
  };

  int i = 0;
  for (const auto& tt : tests) {
    StoragePtr ms = std::make_shared<MemoryStorage>();

    auto raft_log = new RaftLog(ms);
    raft_log->Append({NewEnt(1, 1), NewEnt(2, 2)});
    raft_log->StableTo(tt.stablei, tt.stablet);
    EXPECT_EQ(raft_log->unstable().offset(), tt.wunstable) << "i: " << i;

    delete raft_log;

    i++;
  }
}

TEST(LogTest, TestStableToWithSnap) {
  uint64_t snapi = 5;
  uint64_t snapt = 2;
  struct TestArgs {
    uint64_t stablei;
    uint64_t stablet;
    std::vector<EntryPtr> new_ents;

    uint64_t wunstable;
  } tests[] = {
      {snapi + 1, snapt, {}, snapi + 1},
      {snapi, snapt, {}, snapi + 1},
      {snapi - 1, snapt, {}, snapi + 1},

      {snapi + 1, snapt + 1, {}, snapi + 1},
      {snapi, snapt + 1, {}, snapi + 1},
      {snapi - 1, snapt + 1, {}, snapi + 1},

      {
          snapi + 1,
          snapt,
          {NewEnt(snapi + 1, snapt)},
          snapi + 2,
      },
      {
          snapi,
          snapt,
          {NewEnt(snapi + 1, snapt)},
          snapi + 1,
      },
      {
          snapi - 1,
          snapt,
          {NewEnt(snapi + 1, snapt)},
          snapi + 1,
      },

      {
          snapi + 1,
          snapt + 1,
          {NewEnt(snapi + 1, snapt)},
          snapi + 1,
      },
      {
          snapi,
          snapt + 1,
          {NewEnt(snapi + 1, snapt)},
          snapi + 1,
      },
      {
          snapi - 1,
          snapt + 1,
          {NewEnt(snapi + 1, snapt)},
          snapi + 1,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

    auto snap = NewSnap(snapi, snapt);
    ms->ApplySnapshot(snap);
    auto raft_log = new RaftLog(ms);
    raft_log->Append(tt.new_ents);
    raft_log->StableTo(tt.stablei, tt.stablet);
    EXPECT_EQ(raft_log->unstable().offset(), tt.wunstable) << "i: " << i;

    delete raft_log;

    i++;
  }
}

// TestCompaction ensures that the number of log entries is correct
// compaction.
TEST(LogTest, TestCompaction) {
  struct TestArgs {
    uint64_t last_index;
    std::vector<uint64_t> compact;

    std::vector<int> wleft;
    bool wallow;
  } tests[] = {
      // out of upper bound
      // { 1000, {1001, 1}, {-1}, false },
      {1000, {300, 500, 800, 900}, {700, 500, 200, 100}, true},
      // out of lower bound
      {1000, {300, 299}, {700, -1}, false},
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

    for (uint64_t j = 0; j <= tt.last_index; j++) {
      ms->Append({NewEnt(j)});
    }
    auto raft_log = new RaftLog(ms);
    raft_log->MaybeCommit(tt.last_index, 0);
    raft_log->AppliedTo(raft_log->committed());

    for (uint64_t j = 0; j < tt.compact.size(); j++) {
      ErrNum err = ms->Compact(tt.compact[j]);
      if (err != kOk) {
        EXPECT_FALSE(tt.wallow) << "i: " << i;
        continue;
      }

      std::vector<EntryPtr> all_ents;
      raft_log->AllEntries(&all_ents);
      EXPECT_EQ(all_ents.size(), tt.wleft[j]) << "i: " << i;
    }

    delete raft_log;

    i++;
  }
}

TEST(LogTest, TestLogRestore) {
  uint64_t index = 1000;
  uint64_t term = 1000;
  auto snap = NewSnap(index, term);
  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

  ms->ApplySnapshot(snap);
  auto raft_log = new RaftLog(ms);

  std::vector<EntryPtr> all_ents;
  raft_log->AllEntries(&all_ents);
  EXPECT_EQ(all_ents.size(), 0);
  EXPECT_EQ(raft_log->FirstIndex(), index + 1);
  EXPECT_EQ(raft_log->committed(), index);
  EXPECT_EQ(raft_log->unstable().offset(), index + 1);
  uint64_t t;
  ErrNum err = raft_log->Term(index, &t);
  EXPECT_EQ(err, kOk);
  EXPECT_EQ(t, term);

  delete raft_log;
}

TEST(LogTest, TestIsOutOfBounds) {
  uint64_t offset = 100;
  uint64_t num = 100;
  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

  auto snap = NewSnap(offset);
  ms->ApplySnapshot(snap);
  auto l = new RaftLog(ms);
  for (uint64_t i = 1; i <= num; i++) {
    l->Append({NewEnt(i + offset)});
  }

  uint64_t first = offset + 1;
  struct TestArgs {
    uint64_t lo;
    uint64_t hi;

    bool wpanic;
    bool wErrCompacted;
  } tests[] = {
      {
          first - 2,
          first + 1,
          false,
          true,
      },
      {
          first - 1,
          first + 1,
          false,
          true,
      },
      {
          first,
          first,
          false,
          false,
      },
      {
          first + num / 2,
          first + num / 2,
          false,
          false,
      },
      {
          first + num - 1,
          first + num - 1,
          false,
          false,
      },
      {
          first + num,
          first + num,
          false,
          false,
      },
      // { first + num, first + num + 1, true, false, },
      // { first + num + 1, first + num + 1, true, false, },
  };

  int i = 0;
  for (const auto& tt : tests) {
    ErrNum err = l->MustCheckOutOfBounds(tt.lo, tt.hi);
    EXPECT_FALSE(tt.wErrCompacted && err != kErrCompacted) << "i: " << i;
    EXPECT_FALSE(!tt.wErrCompacted && err != kOk) << "i: " << i;

    i++;
  }

  delete l;
}

TEST(LogTest, TestTerm) {
  uint64_t i;
  uint64_t offset = 100;
  uint64_t num = 100;

  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

  auto snap = NewSnap(offset, 1);
  ms->ApplySnapshot(snap);
  auto l = new RaftLog(ms);
  for (i = 1; i < num; i++) {
    l->Append({NewEnt(offset + i, i)});
  }

  struct TestArgs {
    uint64_t index;

    uint64_t w;
  } tests[] = {
      {
          offset - 1,
          0,
      },
      {
          offset,
          1,
      },
      {
          offset + num / 2,
          num / 2,
      },
      {
          offset + num - 1,
          num - 1,
      },
      {
          offset + num,
          0,
      },
  };

  int j = 0;
  for (const auto& tt : tests) {
    uint64_t tm = 0;
    ErrNum err = l->Term(tt.index, &tm);
    EXPECT_EQ(err, kOk) << "j: " << j;
    EXPECT_EQ(tm, tt.w) << "j: " << j;

    j++;
  }

  delete l;
}

TEST(LogTest, TestTermWithUnstableSnapshot) {
  uint64_t storagesnapi = 100;
  uint64_t unstablesnapi = 100 + 5;

  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

  auto snap = NewSnap(storagesnapi, 1);
  ms->ApplySnapshot(snap);
  auto l = new RaftLog(ms);
  auto unstable_snap = NewSnap(unstablesnapi, 1);
  l->Restore(unstable_snap);

  struct TestArgs {
    uint64_t index;

    uint64_t w;
  } tests[] = {
      // cannot get term from storage
      {
          storagesnapi,
          0,
      },
      // cannot get term from the gap between storage ents
      // and unstable snapshot
      {
          storagesnapi + 1,
          0,
      },
      {
          unstablesnapi - 1,
          0,
      },
      // get term from unstable snapshot index
      {
          unstablesnapi,
          1,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    uint64_t tm = 0;
    ErrNum err = l->Term(tt.index, &tm);
    EXPECT_EQ(err, kOk) << "i: " << i;
    EXPECT_EQ(tm, tt.w) << "i: " << i;

    i++;
  }

  delete l;
}

TEST(LogTest, TestSlice) {
  uint64_t i;
  uint64_t offset = 100;
  uint64_t num = 100;
  uint64_t last = offset + num;
  uint64_t half = offset + num / 2;
  auto halfe = NewEnt(half, half);

  MemoryStoragePtr ms = std::make_shared<MemoryStorage>();

  auto snap = NewSnap(offset);
  ms->ApplySnapshot(snap);
  for (i = 1; i < num / 2; i++) {
    ms->Append({NewEnt(offset + i, offset + i)});
  }
  auto l = new RaftLog(ms);
  for (i = num / 2; i < num; i++) {
    l->Append({NewEnt(offset + i, offset + i)});
  }

  struct TestArgs {
    uint64_t from;
    uint64_t to;
    uint64_t limit;

    std::vector<EntryPtr> w;
    bool wpanic;
  } tests[] = {
      // test no limit
      {
          offset - 1,
          offset + 1,
          kNoLimit,
          {},
          false,
      },
      {
          offset,
          offset + 1,
          kNoLimit,
          {},
          false,
      },
      {
          half - 1,
          half + 1,
          kNoLimit,
          {NewEnt(half - 1, half - 1), NewEnt(half, half)},
          false,
      },
      {
          half,
          half + 1,
          kNoLimit,
          {NewEnt(half, half)},
          false,
      },
      {last - 1, last, kNoLimit, {NewEnt(last - 1, last - 1)}, false},
      // { last, last + 1, kNoLimit, {}, true },

      // test limit
      {half - 1, half + 1, 0, {NewEnt(half - 1, half - 1)}, false},
      {half - 1,
       half + 1,
       halfe->ByteSizeLong() + 1,
       {NewEnt(half - 1, half - 1)},
       false},
      {half - 2,
       half + 1,
       halfe->ByteSizeLong() + 1,
       {NewEnt(half - 2, half - 2)},
       false},
      {half - 1,
       half + 1,
       halfe->ByteSizeLong() * 2,
       {
           NewEnt(half - 1, half - 1),
           NewEnt(half, half),
       },
       false},
      {half - 1,
       half + 2,
       halfe->ByteSizeLong() * 3,
       {
           NewEnt(half - 1, half - 1),
           NewEnt(half, half),
           NewEnt(half + 1, half + 1),
       },
       false},
      {half, half + 2, halfe->ByteSizeLong(), {NewEnt(half, half)}, false},
      {half,
       half + 2,
       halfe->ByteSizeLong() * 2,
       {NewEnt(half, half), NewEnt(half + 1, half + 1)},
       false}};

  int j = 0;
  for (const auto& tt : tests) {
    std::vector<EntryPtr> ents;
    ErrNum err = l->Slice(tt.from, tt.to, tt.limit, &ents);
    EXPECT_FALSE(tt.from <= offset && err != kErrCompacted) << "j: " << j;
    EXPECT_FALSE(tt.from > offset && err != kOk) << "j: " << j;
    EXPECT_TRUE(EntryDeepEqual(ents, tt.w)) << "j: " << j;

    j++;
  }

  delete l;
}

}  // namespace jraft
