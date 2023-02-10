// Copyright (c) rejj - All Rights Reserved

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
#include <gtest/gtest.h>

#include "common/util.h"
#include "test_util/test_util.h"
#include "tracker/inflights.h"

namespace jraft {

std::vector<Inflight> InflightBuffer(std::vector<uint64_t> indices,
                                     std::vector<uint64_t> sizes) {
  if (indices.size() != sizes.size()) {
    return {};
  }

  std::vector<Inflight> buffer;
  for (size_t i = 0; i < indices.size(); i++) {
    buffer.push_back({indices[i], sizes[i]});
  }
  return buffer;
}

TEST(InflightsTest, TestInflightsAdd) {
  // no rotating case
  Inflights ins = ConsIn(0, 0, 0, 10, 0,
                         InflightBuffer({0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
  for (uint64_t i = 0; i < 5; i++) {
    ins.Add(i, 100 + i);
  }
  Inflights want_ins =
      ConsIn(0, 5, 510, 10, 0,
             InflightBuffer({0, 1, 2, 3, 4, 0, 0, 0, 0, 0},
                            {100, 101, 102, 103, 104, 0, 0, 0, 0, 0}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins));

  for (uint64_t i = 5; i < 10; i++) {
    ins.Add(i, 100 + i);
  }
  Inflights want_ins2 = ConsIn(
      0, 10, 1045, 10, 0,
      InflightBuffer({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                     {100, 101, 102, 103, 104, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins2));

  // rotating case
  Inflights ins2 = ConsIn(5, 0, 0, 10, 0,
                          InflightBuffer({0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                         {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
  for (uint64_t i = 0; i < 5; i++) {
    ins2.Add(i, 100 + i);
  }
  Inflights want_ins21 =
      ConsIn(5, 5, 510, 10, 0,
             InflightBuffer({0, 0, 0, 0, 0, 0, 1, 2, 3, 4},
                            {0, 0, 0, 0, 0, 100, 101, 102, 103, 104}));
  EXPECT_TRUE(InflightsDeepEqual(ins2, want_ins21));

  for (uint64_t i = 5; i < 10; i++) {
    ins2.Add(i, 100 + i);
  }
  Inflights want_ins22 = ConsIn(
      5, 10, 1045, 10, 0,
      InflightBuffer({5, 6, 7, 8, 9, 0, 1, 2, 3, 4},
                     {105, 106, 107, 108, 109, 100, 101, 102, 103, 104}));
  EXPECT_TRUE(InflightsDeepEqual(ins2, want_ins22));
}

TEST(InflightsTest, TestInflightsFreeTo) {
  // no rotating case
  Inflights ins = ConsIn(0, 0, 0, 10, 0, InflightBuffer({}, {}));
  for (uint64_t i = 0; i < 10; i++) {
    ins.Add(i, 100 + i);
  }

  ins.FreeLE(0);

  Inflights want_ins0 = ConsIn(
      1, 9, 945, 10, 0,
      InflightBuffer({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                     {100, 101, 102, 103, 104, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins0));

  ins.FreeLE(4);

  Inflights want_ins1 = ConsIn(
      5, 5, 535, 10, 0,
      InflightBuffer({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                     {100, 101, 102, 103, 104, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins1));

  ins.FreeLE(8);

  Inflights want_ins2 = ConsIn(
      9, 1, 109, 10, 0,
      InflightBuffer({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                     {100, 101, 102, 103, 104, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins2));

  // rotating case
  for (uint64_t i = 10; i < 15; i++) {
    ins.Add(i, 100 + i);
  }

  ins.FreeLE(12);

  Inflights want_ins3 = ConsIn(
      3, 2, 227, 10, 0,
      InflightBuffer({10, 11, 12, 13, 14, 5, 6, 7, 8, 9},
                     {110, 111, 112, 113, 114, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins3));

  ins.FreeLE((14));

  Inflights want_ins4 = ConsIn(
      0, 0, 0, 10, 0,
      InflightBuffer({10, 11, 12, 13, 14, 5, 6, 7, 8, 9},
                     {110, 111, 112, 113, 114, 105, 106, 107, 108, 109}));
  EXPECT_TRUE(InflightsDeepEqual(ins, want_ins4));
}

void AddUtilFull(Inflights& ins, uint64_t begin, uint64_t end) {
  while (begin < end) {
    EXPECT_FALSE(ins.Full());
    ins.Add(begin, 100 + begin);
    begin++;
  }
  EXPECT_TRUE(ins.Full());
}

TEST(InflightsTest, TestInflightsFull) {
  struct TestArgs {
    std::string name;
    size_t max_size;
    uint64_t max_bytes;
    uint64_t full_at;
    uint64_t free_le;
    uint64_t again_at;
  } tests[] = {
      {.name = "always-full", .max_size = 0, .full_at = 0},
      {.name = "single-entry",
       .max_size = 1,
       .full_at = 1,
       .free_le = 1,
       .again_at = 2},
      {.name = "single-entry-overflow",
       .max_size = 1,
       .max_bytes = 10,
       .full_at = 1,
       .free_le = 1,
       .again_at = 2},
      {.name = "multi-entry",
       .max_size = 15,
       .full_at = 15,
       .free_le = 6,
       .again_at = 22},
      {.name = "slight-overflow",
       .max_size = 8,
       .max_bytes = 400,
       .full_at = 4,
       .free_le = 2,
       .again_at = 7},
      {.name = "exact-max-bytes",
       .max_size = 8,
       .max_bytes = 406,
       .full_at = 4,
       .free_le = 3,
       .again_at = 8},
      {.name = "larger-overflow",
       .max_size = 15,
       .max_bytes = 408,
       .full_at = 5,
       .free_le = 1,
       .again_at = 6},
  };

  for (const auto& tt : tests) {
    Inflights ins =
        ConsIn(0, 0, 0, tt.max_size, tt.max_bytes, InflightBuffer({}, {}));
    AddUtilFull(ins, 0, tt.full_at);
    ins.FreeLE(tt.free_le);
    AddUtilFull(ins, tt.full_at, tt.again_at);

    // ins.Add(100, 1024);
  }
}

}  // namespace jraft
