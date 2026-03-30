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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "chain/storage/res_tier_manager.h"
#include "chain/storage/storage.h"

namespace resdb {
namespace storage {

// Storage wrapper that keeps a hot and cold backend and offloads by height.
class TieredStorage : public Storage {
 public:
  // Builds a tiered storage instance with hot/cold backends and a policy.
  TieredStorage(std::unique_ptr<Storage> hot,
                std::unique_ptr<Storage> cold,
                ResTierManagerConfig config,
                ResTierManager::HeightProvider height_provider = nullptr);
  // Stops the background tier manager before releasing storage.
  ~TieredStorage() override;

  // Starts the background offload manager.
  void StartTiering();
  // Stops the background offload manager.
  void StopTiering();

  // Writes always go to the hot tier to keep recent data fast.
  int SetValue(const std::string& key, const std::string& value) override;
  int SetValueWithSeq(const std::string& key, const std::string& value,
                      uint64_t seq) override;
  int SetValueWithVersion(const std::string& key, const std::string& value,
                          int version) override;

  // Reads check hot first and fall back to cold when missing.
  std::string GetValue(const std::string& key) override;
  std::pair<std::string, uint64_t> GetValueWithSeq(
      const std::string& key, uint64_t seq) override;
  std::pair<std::string, int> GetValueWithVersion(
      const std::string& key, int version) override;

  // Range and list operations merge hot and cold results.
  std::string GetRange(const std::string& min_key,
                       const std::string& max_key) override;
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>>
  GetAllItemsWithSeq() override;
  std::map<std::string, std::pair<std::string, int>> GetAllItems() override;
  std::map<std::string, std::pair<std::string, int>> GetKeyRange(
      const std::string& min_key, const std::string& max_key) override;

  // History queries combine hot and cold versions.
  std::vector<std::pair<std::string, int>> GetHistory(
      const std::string& key, int min_version, int max_version) override;
  std::vector<std::pair<std::string, int>> GetTopHistory(
      const std::string& key, int number) override;

  // Flushes both tiers to ensure durability.
  bool Flush() override;
  // Returns the hot tier checkpoint as the active height source.
  uint64_t GetLastCheckpoint() override;

 private:
    friend class TieredStorageTest;
  // Returns true if a seq-based lookup found a real value.
  bool HasSeqValue(const std::pair<std::string, uint64_t>& value) const;
  // Returns true if a version-based lookup found a real value.
  bool HasVersionValue(const std::pair<std::string, int>& value) const;
  // Copies eligible heights from hot to cold.
  void HandleOffloadHeights(const std::vector<uint64_t>& heights);

 private:
  std::unique_ptr<Storage> hot_;
  std::unique_ptr<Storage> cold_;
  ResTierManagerConfig config_;
  std::unique_ptr<ResTierManager> tier_manager_;

  // Guards hot/cold operations against concurrent offload threads.
  std::mutex mutex_;
};

// Factory that constructs and starts tiered storage in one call.
std::unique_ptr<Storage> NewTieredStorage(
    std::unique_ptr<Storage> hot,
    std::unique_ptr<Storage> cold,
    ResTierManagerConfig config,
    ResTierManager::HeightProvider height_provider = nullptr);

}  // namespace storage
}  // namespace resdb
