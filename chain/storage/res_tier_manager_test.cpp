/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "chain/storage/memory_db.h"
#include "chain/storage/res_tier_manager.h"
#include "chain/storage/tiered_storage.h"

namespace resdb {

class ResTierManagerTest : public ::testing::Test {};

TEST_F(ResTierManagerTest, ComputeOffloadCandidatesBatches) {
  ResTierManagerConfig config;
  config.hot_block_threshold = 5;
  config.offload_batch_size = 3;

  ResTierManager manager([]() { return 0; }, config);

  EXPECT_TRUE(manager.ComputeOffloadCandidates(4).empty());

  std::vector<uint64_t> first = manager.ComputeOffloadCandidates(10);
  EXPECT_EQ(first, (std::vector<uint64_t>{1, 2, 3}));

  std::vector<uint64_t> second = manager.ComputeOffloadCandidates(10);
  EXPECT_EQ(second, (std::vector<uint64_t>{4, 5}));

  EXPECT_TRUE(manager.ComputeOffloadCandidates(10).empty());
}

TEST_F(ResTierManagerTest, TickOnceQueuesPendingWhenNoCallback) {
  ResTierManagerConfig config;
  config.hot_block_threshold = 2;
  config.offload_batch_size = 2;

  ResTierManager manager([]() { return 5; }, config);

  std::vector<uint64_t> candidates = manager.TickOnce();
  EXPECT_EQ(candidates, (std::vector<uint64_t>{1, 2}));

  std::vector<uint64_t> pending = manager.GetPendingOffloadHeights();
  EXPECT_EQ(pending, (std::vector<uint64_t>{1, 2}));

  EXPECT_TRUE(manager.GetPendingOffloadHeights().empty());
}

TEST_F(ResTierManagerTest, TickOnceInvokesCallback) {
  ResTierManagerConfig config;
  config.hot_block_threshold = 1;
  config.offload_batch_size = 2;

  std::vector<uint64_t> observed;
  ResTierManager manager([]() { return 4; }, config,
                         [&observed](const std::vector<uint64_t>& heights) {
                           observed = heights;
                         });

  std::vector<uint64_t> candidates = manager.TickOnce();
  EXPECT_EQ(candidates, (std::vector<uint64_t>{1, 2}));
  EXPECT_EQ(observed, (std::vector<uint64_t>{1, 2}));
}

}  // namespace resdb

namespace resdb {
namespace storage {

class TieredStorageTest : public ::testing::Test {};

TEST_F(TieredStorageTest, BatchOffloadCopiesEligibleSeqValues) {
  ResTierManagerConfig config;
  config.hot_block_threshold = 2;
  config.offload_batch_size = 2;

  auto hot = NewMemoryDB();
  auto cold = NewMemoryDB();
  TieredStorage storage(std::move(hot), std::move(cold), config,
                        []() { return 0; });

  EXPECT_EQ(storage.SetValueWithSeq("key", "value1", 1), 0);
  EXPECT_EQ(storage.SetValueWithSeq("key", "value2", 2), 0);
  EXPECT_EQ(storage.SetValueWithSeq("key", "value3", 3), 0);
  EXPECT_EQ(storage.SetValueWithSeq("key", "value4", 4), 0);

  storage.HandleOffloadHeights({1, 2});

  auto cold_value1 = storage.cold_->GetValueWithSeq("key", 1);
  auto cold_value2 = storage.cold_->GetValueWithSeq("key", 2);
  auto cold_value3 = storage.cold_->GetValueWithSeq("key", 3);

  EXPECT_EQ(cold_value1, std::make_pair(std::string("value1"), 1ULL));
  EXPECT_EQ(cold_value2, std::make_pair(std::string("value2"), 2ULL));
  EXPECT_EQ(cold_value3, std::make_pair(std::string(""), 0ULL));
}

}  // namespace storage
}  // namespace resdb
