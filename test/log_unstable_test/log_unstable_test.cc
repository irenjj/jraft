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
#include "log_unstable.h"

namespace jraft {

TEST(LogUnstableTest, MaybeFirstIndex) {
  struct TestArgs {
    SnapshotPtr snap;
    std::vector<EntryPtr> ents;
    uint64_t offset;

    bool wok;
    uint64_t windex;
  } tests[] = {
      // no snapshot
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .wok = false,
          .windex = 0,
      },
      {
          .snap = nullptr,
          .ents = {},
          .offset = 0,
          .wok = false,
          .windex = 0,
      },
      // has snapshot
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .wok = true,
          .windex = 5,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {},
          .offset = 5,
          .wok = true,
          .windex = 5,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    LogUnstable log_unstable(tt.snap, tt.ents, tt.offset);
    uint64_t index = 0;
    bool ok = log_unstable.MaybeFirstIndex(&index);
    EXPECT_EQ(ok, tt.wok) << "i: " << i;
    EXPECT_EQ(index, tt.windex) << "i: " << i;

    i++;
  }
}

TEST(LogUnstableTest, MaybeLastIndex) {
  struct TestArgs {
    SnapshotPtr snap;
    std::vector<EntryPtr> ents;
    uint64_t offset;

    bool wok;
    uint64_t windex;
  } tests[] = {
      // last in entries
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .wok = true,
          .windex = 5,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .wok = true,
          .windex = 5,
      },
      // last in snapshot
      {
          .snap = NewSnap(4, 1),
          .ents = {},
          .offset = 5,
          .wok = true,
          .windex = 4,
      },
      // empty unstable
      {
          .snap = nullptr,
          .ents = {},
          .offset = 0,
          .wok = false,
          .windex = 0,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    LogUnstable log_unstable(tt.snap, tt.ents, tt.offset);
    uint64_t index = 0;
    bool ok = log_unstable.MaybeLastIndex(&index);
    EXPECT_EQ(ok, tt.wok) << "i: " << i;
    EXPECT_EQ(index, tt.windex) << "i: " << i;

    i++;
  }
}

TEST(LogUnstableTest, MaybeTerm) {
  struct TestArgs {
    SnapshotPtr snap;
    std::vector<EntryPtr> ents;
    uint64_t offset;
    uint64_t index;

    bool wok;
    uint64_t wterm;
  } tests[] = {
      // term from entries
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 5,
          .wok = true,
          .wterm = 1,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 6,
          .wok = false,
          .wterm = 0,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 4,
          .wok = false,
          .wterm = 0,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 5,
          .wok = true,
          .wterm = 1,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt()},
          .offset = 5,
          .index = 6,
          .wok = false,
          .wterm = 0,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 4,
          .wok = true,
          .wterm = 1,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 3,
          .wok = false,
          .wterm = 0,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {},
          .offset = 5,
          .index = 5,
          .wok = false,
          .wterm = 0,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {},
          .offset = 5,
          .index = 4,
          .wok = true,
          .wterm = 1,
      },
      {
          .snap = nullptr,
          .ents = {},
          .offset = 0,
          .index = 5,
          .wok = false,
          .wterm = 0,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    LogUnstable log_unstable(tt.snap, tt.ents, tt.offset);
    uint64_t term = 0;
    bool ok = log_unstable.MaybeTerm(tt.index, &term);
    EXPECT_EQ(ok, tt.wok) << "i: " << i;
    EXPECT_EQ(term, tt.wterm) << "i: " << i;

    i++;
  }
}

TEST(LogUnstableTest, Restore) {
  auto log_snap = NewSnap(4, 1);
  LogUnstable log_unstable(log_snap, {NewEnt(5, 1)}, 5);
  auto snap = NewSnap(6, 2);
  log_unstable.Restore(snap);

  EXPECT_EQ(log_unstable.offset(), snap->metadata().index() + 1);
  EXPECT_EQ(log_unstable.entries().size(), 0);
}

TEST(LogUnstableTest, StableTo) {
  struct TestArgs {
    SnapshotPtr snap;
    std::vector<EntryPtr> ents;
    uint64_t offset;
    uint64_t index;
    uint64_t term;

    uint64_t woffset;
    int wlen;
  } tests[] = {
      {
          .snap = nullptr,
          .ents = {},
          .offset = 0,
          .index = 5,
          .term = 1,
          .woffset = 0,
          .wlen = 0,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 5,
          .term = 1,
          .woffset = 6,
          .wlen = 0,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1), NewEnt(6, 1)},
          .offset = 5,
          .index = 5,
          .term = 1,
          .woffset = 6,
          .wlen = 1,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(6, 2)},
          .offset = 6,
          .index = 6,
          .term = 1,
          .woffset = 6,
          .wlen = 1,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 4,
          .term = 1,
          .woffset = 5,
          .wlen = 1,
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 4,
          .term = 2,
          .woffset = 5,
          .wlen = 1,
      },
      // with snapshot
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 5,
          .term = 1,
          .woffset = 6,
          .wlen = 0,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1), NewEnt(6, 1)},
          .offset = 5,
          .index = 5,
          .term = 1,
          .woffset = 6,
          .wlen = 1,
      },
      {
          .snap = NewSnap(5, 1),
          .ents = {NewEnt(6, 2)},
          .offset = 6,
          .index = 6,
          .term = 1,
          .woffset = 6,
          .wlen = 1,
      },
      {
          .snap = NewSnap(4, 1),
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .index = 4,
          .term = 1,
          .woffset = 5,
          .wlen = 1,
      },
      {
          .snap = NewSnap(4, 2),
          .ents = {NewEnt(5, 2)},
          .offset = 5,
          .index = 4,
          .term = 1,
          .woffset = 5,
          .wlen = 1,
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    LogUnstable log_unstable(tt.snap, tt.ents, tt.offset);
    log_unstable.StableTo(tt.index, tt.term);
    EXPECT_EQ(log_unstable.offset(), tt.woffset) << "i: " << i;
    EXPECT_EQ(log_unstable.entries().size(), tt.wlen) << "i: " << i;

    i++;
  }
}

TEST(LogUnstableTest, TruncateAndAppend) {
  struct TestArgs {
    SnapshotPtr snap;
    std::vector<EntryPtr> ents;
    uint64_t offset;
    std::vector<EntryPtr> to_append;

    uint64_t woffset;
    std::vector<EntryPtr> wentries;
  } tests[] = {
      // append to the end
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .to_append = {NewEnt(6, 1), NewEnt(7, 1)},
          .woffset = 5,
          .wentries = {NewEnt(5, 1), NewEnt(6, 1), NewEnt(7, 1)},
      },
      // replace the unstable entries
      {
          .snap = nullptr,
          .ents =
              {
                  NewEnt(5, 1),
              },
          .offset = 5,
          .to_append = {NewEnt(5, 2), NewEnt(6, 2)},
          .woffset = 5,
          .wentries = {NewEnt(5, 2), NewEnt(6, 2)},
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1)},
          .offset = 5,
          .to_append = {NewEnt(4, 2), NewEnt(5, 2), NewEnt(6, 2)},
          .woffset = 4,
          .wentries = {NewEnt(4, 2), NewEnt(5, 2), NewEnt(6, 2)},
      },
      // truncate the existing entries and append
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1), NewEnt(6, 1), NewEnt(7, 1)},
          .offset = 5,
          .to_append = {NewEnt(6, 2)},
          .woffset = 5,
          .wentries = {NewEnt(5, 1), NewEnt(6, 2)},
      },
      {
          .snap = nullptr,
          .ents = {NewEnt(5, 1), NewEnt(6, 1), NewEnt(7, 1)},
          .offset = 5,
          .to_append = {NewEnt(7, 2), NewEnt(8, 2)},
          .woffset = 5,
          .wentries = {NewEnt(5, 1), NewEnt(6, 1), NewEnt(7, 2), NewEnt(8, 2)},
      },
  };

  int i = 0;
  for (const auto& tt : tests) {
    LogUnstable log_unstable(tt.snap, tt.ents, tt.offset);
    log_unstable.TruncateAndAppend(tt.to_append);
    EXPECT_EQ(log_unstable.offset(), tt.woffset) << "i: " << i;
    EXPECT_TRUE(EntryDeepEqual(log_unstable.entries(), tt.wentries))
        << "i: " << i;

    i++;
  }
}

}  // namespace jraft
