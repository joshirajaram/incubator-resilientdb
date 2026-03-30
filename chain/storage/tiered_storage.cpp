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

#include "chain/storage/tiered_storage.h"

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <utility>

namespace resdb {
namespace storage {

TieredStorage::TieredStorage(std::unique_ptr<Storage> hot,
                             std::unique_ptr<Storage> cold,
                             ResTierManagerConfig config,
                             ResTierManager::HeightProvider height_provider)
    : hot_(std::move(hot)),
      cold_(std::move(cold)),
      config_(std::move(config)) {
  // Provide a default height source when one is not supplied.
  if (!height_provider) {
    height_provider = [this]() {
      return hot_ ? hot_->GetLastCheckpoint() : 0;
    };
  }
  // Build the tier manager that decides which heights to offload.
  tier_manager_ = std::make_unique<ResTierManager>(
      std::move(height_provider), config_,
      [this](const std::vector<uint64_t>& heights) {
        HandleOffloadHeights(heights);
      });
}

TieredStorage::~TieredStorage() {
  // Ensure the background thread is stopped before destroying storage.
  StopTiering();
}

void TieredStorage::StartTiering() {
  // Start the tiering loop only when a manager exists.
  if (tier_manager_) {
    tier_manager_->Start();
  }
}

void TieredStorage::StopTiering() {
  // Stop the tiering loop so no background work touches storage.
  if (tier_manager_) {
    tier_manager_->Stop();
  }
}

int TieredStorage::SetValue(const std::string& key,
                            const std::string& value) {
  // Writes target the hot tier to keep recent data fast.
  std::lock_guard<std::mutex> lock(mutex_);
  return hot_ ? hot_->SetValue(key, value) : -1;
}

int TieredStorage::SetValueWithSeq(const std::string& key,
                                   const std::string& value, uint64_t seq) {
  // Sequence-based writes also stay in the hot tier for low latency.
  std::lock_guard<std::mutex> lock(mutex_);
  return hot_ ? hot_->SetValueWithSeq(key, value, seq) : -1;
}

int TieredStorage::SetValueWithVersion(const std::string& key,
                                       const std::string& value, int version) {
  // Versioned writes go to the hot tier for active history.
  std::lock_guard<std::mutex> lock(mutex_);
  return hot_ ? hot_->SetValueWithVersion(key, value, version) : -1;
}

std::string TieredStorage::GetValue(const std::string& key) {
  // Reads prefer the hot tier, then fall back to cold.
  std::lock_guard<std::mutex> lock(mutex_);
  if (hot_) {
    std::string value = hot_->GetValue(key);
    if (!value.empty()) {
      return value;
    }
  }
  return cold_ ? cold_->GetValue(key) : "";
}

std::pair<std::string, uint64_t> TieredStorage::GetValueWithSeq(
    const std::string& key, uint64_t seq) {
  // Look up in hot tier first and fall back to cold when missing.
  std::lock_guard<std::mutex> lock(mutex_);
  if (hot_) {
    auto value = hot_->GetValueWithSeq(key, seq);
    if (HasSeqValue(value)) {
      return value;
    }
  }
  return cold_ ? cold_->GetValueWithSeq(key, seq)
               : std::make_pair("", 0);
}

std::pair<std::string, int> TieredStorage::GetValueWithVersion(
    const std::string& key, int version) {
  // Look up in hot tier first and fall back to cold when missing.
  std::lock_guard<std::mutex> lock(mutex_);
  if (hot_) {
    auto value = hot_->GetValueWithVersion(key, version);
    if (HasVersionValue(value)) {
      return value;
    }
  }
  return cold_ ? cold_->GetValueWithVersion(key, version)
               : std::make_pair("", 0);
}

std::string TieredStorage::GetRange(const std::string& min_key,
                                    const std::string& max_key) {
  // Favor hot results, using cold only when hot returns empty.
  std::lock_guard<std::mutex> lock(mutex_);
  if (hot_) {
    std::string value = hot_->GetRange(min_key, max_key);
    if (!value.empty()) {
      return value;
    }
  }
  return cold_ ? cold_->GetRange(min_key, max_key) : "";
}

std::map<std::string, std::vector<std::pair<std::string, uint64_t>>>
TieredStorage::GetAllItemsWithSeq() {
  // Merge cold data first, then overlay hot data for freshness.
  std::lock_guard<std::mutex> lock(mutex_);
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> merged;
  if (cold_) {
    merged = cold_->GetAllItemsWithSeq();
  }
  if (hot_) {
    auto hot_items = hot_->GetAllItemsWithSeq();
    for (auto& entry : hot_items) {
      auto& values = merged[entry.first];
      values.insert(values.end(), entry.second.begin(), entry.second.end());
      std::sort(values.begin(), values.end(),
                [](const auto& left, const auto& right) {
                  return left.second < right.second;
                });
      values.erase(std::unique(values.begin(), values.end(),
                               [](const auto& left, const auto& right) {
                                 return left.second == right.second;
                               }),
                   values.end());
    }
  }
  return merged;
}

std::map<std::string, std::pair<std::string, int>>
TieredStorage::GetAllItems() {
  // Cold provides the base, hot overrides with most recent values.
  std::lock_guard<std::mutex> lock(mutex_);
  std::map<std::string, std::pair<std::string, int>> merged;
  if (cold_) {
    merged = cold_->GetAllItems();
  }
  if (hot_) {
    auto hot_items = hot_->GetAllItems();
    merged.insert(hot_items.begin(), hot_items.end());
  }
  return merged;
}

std::map<std::string, std::pair<std::string, int>> TieredStorage::GetKeyRange(
    const std::string& min_key, const std::string& max_key) {
  // Merge cold data with hot overrides for ranged queries.
  std::lock_guard<std::mutex> lock(mutex_);
  std::map<std::string, std::pair<std::string, int>> merged;
  if (cold_) {
    merged = cold_->GetKeyRange(min_key, max_key);
  }
  if (hot_) {
    auto hot_items = hot_->GetKeyRange(min_key, max_key);
    merged.insert(hot_items.begin(), hot_items.end());
  }
  return merged;
}

std::vector<std::pair<std::string, int>> TieredStorage::GetHistory(
    const std::string& key, int min_version, int max_version) {
  // Combine histories from both tiers into a single ordered list.
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::pair<std::string, int>> merged;
  if (cold_) {
    merged = cold_->GetHistory(key, min_version, max_version);
  }
  if (hot_) {
    auto hot_history = hot_->GetHistory(key, min_version, max_version);
    merged.insert(merged.end(), hot_history.begin(), hot_history.end());
    std::sort(merged.begin(), merged.end(),
              [](const auto& left, const auto& right) {
                return left.second > right.second;
              });
  }
  return merged;
}

std::vector<std::pair<std::string, int>> TieredStorage::GetTopHistory(
    const std::string& key, int number) {
  // Reuse merged history and trim to requested size.
  auto history = GetHistory(key, 0, std::numeric_limits<int>::max());
  if (history.size() > static_cast<size_t>(number)) {
    history.resize(static_cast<size_t>(number));
  }
  return history;
}

bool TieredStorage::Flush() {
  // Flush both tiers to ensure durable writes.
  std::lock_guard<std::mutex> lock(mutex_);
  bool hot_ok = hot_ ? hot_->Flush() : true;
  bool cold_ok = cold_ ? cold_->Flush() : true;
  return hot_ok && cold_ok;
}

uint64_t TieredStorage::GetLastCheckpoint() {
  // The hot tier represents the active write checkpoint.
  std::lock_guard<std::mutex> lock(mutex_);
  return hot_ ? hot_->GetLastCheckpoint() : 0;
}

bool TieredStorage::HasSeqValue(
    const std::pair<std::string, uint64_t>& value) const {
  // Treat empty values with zero seq as missing.
  return !(value.first.empty() && value.second == 0);
}

bool TieredStorage::HasVersionValue(
    const std::pair<std::string, int>& value) const {
  // Treat empty values with zero version as missing.
  return !(value.first.empty() && value.second == 0);
}

void TieredStorage::HandleOffloadHeights(
    const std::vector<uint64_t>& heights) {
  // Copy cold candidates without deleting hot values (safe first phase).
  if (heights.empty() || !hot_ || !cold_) {
    return;
  }

  std::vector<uint64_t> sorted_heights = heights;
  std::sort(sorted_heights.begin(), sorted_heights.end());
  std::unordered_set<uint64_t> target_heights(sorted_heights.begin(),
                                              sorted_heights.end());

  std::lock_guard<std::mutex> lock(mutex_);
  auto hot_items = hot_->GetAllItemsWithSeq();
  for (const auto& entry : hot_items) {
    const std::string& key = entry.first;
    for (const auto& value_pair : entry.second) {
      if (target_heights.find(value_pair.second) != target_heights.end()) {
        cold_->SetValueWithSeq(key, value_pair.first, value_pair.second);
      }
    }
  }
}

std::unique_ptr<Storage> NewTieredStorage(
    std::unique_ptr<Storage> hot,
    std::unique_ptr<Storage> cold,
    ResTierManagerConfig config,
    ResTierManager::HeightProvider height_provider) {
  // Build the tiered wrapper and start its background manager.
  auto storage = std::make_unique<TieredStorage>(
      std::move(hot), std::move(cold), std::move(config),
      std::move(height_provider));
  storage->StartTiering();
  return storage;
}

}  // namespace storage
}  // namespace resdb
