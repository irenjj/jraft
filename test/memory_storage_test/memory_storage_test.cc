// Copyright 2022 rejj - All Rights Reserved

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

namespace jraft {

TEST(MemoryStorageTest, TestStorageTerm) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};
  struct TestArgs {
    uint64_t i;

    ErrNum werr;
    uint64_t wterm;
    bool wpanic;
  } tests[] = {
      {2, kErrCompacted, 0, false},
      {3, kOk, 3, false},
      {4, kOk, 4, false},
      {5, kOk, 5, false},
      {6, kErrUnavailable, 0, false},
  };

  int i = 0;
  for (auto& tt : tests) {
    MemoryStorage s;
    s.set_entries(entries);

    uint64_t term = 0;
    ErrNum err = s.Term(tt.i, &term);
    EXPECT_EQ(err, tt.werr) << "i: " << i;
    EXPECT_EQ(term, tt.wterm) << "i: " << i;
    i++;
  }
}

TEST(MemoryStorageTest, TestStorageEntries) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5),
                                NewEnt(6, 6)};
  struct TestArgs {
    uint64_t lo;
    uint64_t hi;
    uint64_t max_size;

    ErrNum werr;
    std::vector<EntryPtr> wentries;
  } tests[] = {
      {2, 6, UINT64_MAX, kErrCompacted, {}},
      {3, 4, UINT64_MAX, kErrCompacted, {}},
      {4, 5, UINT64_MAX, kOk, {NewEnt(4, 4)}},
      {4, 6, UINT64_MAX, kOk, {NewEnt(4, 4), NewEnt(5, 5)}},
      {4, 7, UINT64_MAX, kOk, {NewEnt(4, 4), NewEnt(5, 5), NewEnt(6, 6)}},
      // even if max_size is zero, the first entry should be returned
      {4, 7, 0, kOk, {NewEnt(4, 4)}},
      // limit to 2
      {4,
       7,
       entries[1]->ByteSizeLong() + entries[2]->ByteSizeLong(),
       kOk,
       {NewEnt(4, 4), NewEnt(5, 5)}},
      // limit to 2
      {4,
       7,
       entries[1]->ByteSizeLong() + entries[2]->ByteSizeLong() +
           entries[3]->ByteSizeLong() / 2,
       kOk,
       {NewEnt(4, 4), NewEnt(5, 5)}},
      {4,
       7,
       entries[1]->ByteSizeLong() + entries[2]->ByteSizeLong() +
           entries[3]->ByteSizeLong() - 1,
       kOk,
       {NewEnt(4, 4), NewEnt(5, 5)}},
      // all
      {4,
       7,
       entries[1]->ByteSizeLong() + entries[2]->ByteSizeLong() +
           entries[3]->ByteSizeLong(),
       kOk,
       {NewEnt(4, 4), NewEnt(5, 5), NewEnt(6, 6)}},
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStorage s;
    s.set_entries(entries);

    std::vector<EntryPtr> get_entries;
    ErrNum err = s.Entries(tt.lo, tt.hi, tt.max_size, &get_entries);
    EXPECT_EQ(err, tt.werr) << "i: " << i;
    EXPECT_TRUE(EntryDeepEqual(get_entries, tt.wentries)) << "i: " << i;
    i++;
  }
}

TEST(MemoryStorageTest, TestStorageLastIndex) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};
  MemoryStorage s;
  s.set_entries(entries);
  uint64_t last;
  ErrNum err = s.LastIndex(&last);
  EXPECT_EQ(err, kOk) << "get last index fail";
  EXPECT_EQ(last, 5) << "last index wrong";

  s.Append({NewEnt(6, 5)});
  err = s.LastIndex(&last);
  EXPECT_EQ(err, kOk) << "get last index fail";
  EXPECT_EQ(last, 6) << "last index wrong";
}

TEST(MemoryStorageTest, TestStorageFirstIndex) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};
  MemoryStorage s;
  s.set_entries(entries);

  uint64_t first;
  ErrNum err = s.FirstIndex(&first);
  EXPECT_EQ(err, kOk) << "get first index fail";
  EXPECT_EQ(first, 4) << "first index wrong";

  s.Compact(4);
  err = s.FirstIndex(&first);
  EXPECT_EQ(err, kOk) << "get first index fail";
  EXPECT_EQ(first, 5) << "first index wrong";
}

TEST(MemoryStorageTest, TestStorageCompact) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};

  struct TestArgs {
    uint64_t i;

    ErrNum werr;
    uint64_t windex;
    uint64_t wterm;
    int wlen;
  } tests[] = {
      {2, kErrCompacted, 3, 3, 3},
      {3, kErrCompacted, 3, 3, 3},
      {4, kOk, 4, 4, 2},
      {5, kOk, 5, 5, 1},
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStorage s;
    s.set_entries(entries);
    ErrNum err = s.Compact(tt.i);
    EXPECT_EQ(err, tt.werr) << "i: " << i;
    EXPECT_EQ(s.entries(0).index(), tt.windex) << "i: " << i;
    EXPECT_EQ(s.entries(0).term(), tt.wterm) << "i: " << i;
    EXPECT_EQ(s.entries().size(), tt.wlen) << "i: " << i;
    i++;
  }
}

TEST(MemoryStorageTest, TestStorageCreateSnapshot) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};
  ConfState cs;
  cs.add_voters(1), cs.add_voters(2), cs.add_voters(3);
  std::string data("data");

  struct TestArgs {
    uint64_t i;

    ErrNum werr;
    SnapshotPtr wsnap;
  } tests[] = {
      {4, kOk, NewSnap(4, 4, data, &cs)},
      {5, kOk, NewSnap(5, 5, data, &cs)},
  };

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStorage s;
    s.set_entries(entries);
    SnapshotPtr snap = std::make_shared<Snapshot>();
    ErrNum err = s.CreateSnapshot(tt.i, &cs, data, &snap);
    EXPECT_EQ(err, tt.werr) << "i: " << i;
    EXPECT_TRUE(SnapshotDeepEqual(snap, tt.wsnap)) << "i: " << i;

    i++;
  }
}

TEST(MemoryStorageTest, TestStorageAppend) {
  std::vector<EntryPtr> entries{NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)};

  struct TestArgs {
    std::vector<EntryPtr> entries;

    ErrNum werr;
    std::vector<EntryPtr> wentries;
  } tests[] = {{
                   {NewEnt(1, 1), NewEnt(2, 2)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)},
               },
               {
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5)},
               },
               {
                   {NewEnt(3, 3), NewEnt(4, 6), NewEnt(5, 6)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 6), NewEnt(5, 6)},
               },
               {
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5), NewEnt(6, 5)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5), NewEnt(6, 5)},
               },
               {
                   {NewEnt(2, 3), NewEnt(3, 3), NewEnt(4, 5)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 5)},
               },
               {
                   {NewEnt(4, 5)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 5)},
               },
               {
                   {NewEnt(6, 5)},
                   kOk,
                   {NewEnt(3, 3), NewEnt(4, 4), NewEnt(5, 5), NewEnt(6, 5)},
               }};

  int i = 0;
  for (const auto& tt : tests) {
    MemoryStorage s;
    s.set_entries(entries);
    ErrNum err = s.Append(tt.entries);
    EXPECT_EQ(err, tt.werr) << "i: " << i;
    EXPECT_TRUE(EntryDeepEqual(s.entries(), tt.wentries)) << "i: " << i;
    i++;
  }
}

TEST(MemoryStorageTest, TestStorageApplySnapshot) {
  ConfState cs;
  cs.add_voters(1), cs.add_voters(2), cs.add_voters(3);
  std::string data("data");

  SnapshotPtr tests[] = {
      NewSnap(4, 4, data, &cs),
      NewSnap(3, 3, data, &cs),
  };
  MemoryStorage s;

  {
    auto snap = tests[0];
    EXPECT_EQ(kOk, s.ApplySnapshot(*snap));
  }

  {
    auto snap = tests[1];
    EXPECT_EQ(kErrSnapOutOfData, s.ApplySnapshot(*snap));
  }
}

}  // namespace jraft
