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

#include "chain/storage/ipfs_storage.h"

#include <algorithm>
#include <limits>

#include <glog/logging.h>

namespace resdb {
namespace storage {

IPFSStorage::IPFSStorage(std::string base_url)
    : client_(std::move(base_url)) {}

int IPFSStorage::SetValue(const std::string& key,
                          const std::string& value) {
  // Store the value in IPFS and remember its CID for the key.
  std::string cid = PushValue(value);
  if (cid.empty()) {
    return -1;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  kv_map_[key] = cid;
  return 0;
}

int IPFSStorage::SetValueWithSeq(const std::string& key,
                                 const std::string& value, uint64_t seq) {
  // Persist the value to IPFS and record its CID with the seq.
  std::string cid = PushValue(value);
  if (cid.empty()) {
    return -1;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto& history = kv_map_with_seq_[key];
  if (!history.empty() && history.back().second > seq) {
    LOG(ERROR) << "value seq not match. key:" << key
               << " last seq:" << history.back().second
               << " new seq:" << seq;
    return -2;
  }
  history.emplace_back(cid, seq);
  while (history.size() > max_history_) {
    history.erase(history.begin());
  }
  if (seq > last_seq_) {
    last_seq_ = seq;
  }
  return 0;
}

int IPFSStorage::SetValueWithVersion(const std::string& key,
                                     const std::string& value, int version) {
  // Keep version semantics consistent with MemoryDB (version + 1 stored).
  std::string cid = PushValue(value);
  if (cid.empty()) {
    return -1;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto& history = kv_map_with_version_[key];
  if ((history.empty() && version != 0) ||
      (!history.empty() && history.back().second != version)) {
    LOG(ERROR) << "value version not match. key:" << key
               << " last version:" << (history.empty() ? 0 : history.back().second)
               << " new version:" << version;
    return -2;
  }
  history.emplace_back(cid, version + 1);
  while (history.size() > max_history_) {
    history.erase(history.begin());
  }
  return 0;
}

std::string IPFSStorage::GetValue(const std::string& key) {
  // Resolve the key to a CID and fetch the content from IPFS.
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = kv_map_.find(key);
  if (it == kv_map_.end()) {
    return "";
  }
  return FetchValue(it->second);
}

std::pair<std::string, uint64_t> IPFSStorage::GetValueWithSeq(
    const std::string& key, uint64_t seq) {
  // Read the requested seq (or latest) and fetch the IPFS payload.
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = kv_map_with_seq_.find(key);
  if (it == kv_map_with_seq_.end() || it->second.empty()) {
    return std::make_pair("", 0);
  }
  const auto& history = it->second;
  if (seq == 0) {
    const auto& entry = history.back();
    return std::make_pair(FetchValue(entry.first), entry.second);
  }
  for (auto rit = history.rbegin(); rit != history.rend(); ++rit) {
    if (rit->second == seq) {
      return std::make_pair(FetchValue(rit->first), rit->second);
    }
    if (rit->second < seq) {
      break;
    }
  }
  return std::make_pair("", 0);
}

std::pair<std::string, int> IPFSStorage::GetValueWithVersion(
    const std::string& key, int version) {
  // Read the requested version and fetch the IPFS payload.
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = kv_map_with_version_.find(key);
  if (it == kv_map_with_version_.end() || it->second.empty()) {
    return std::make_pair("", 0);
  }
  const auto& history = it->second;
  for (auto rit = history.rbegin(); rit != history.rend(); ++rit) {
    if (rit->second == version) {
      return std::make_pair(FetchValue(rit->first), rit->second);
    }
    if (rit->second < version) {
      break;
    }
  }
  const auto& latest = history.back();
  return std::make_pair(FetchValue(latest.first), latest.second);
}

std::string IPFSStorage::GetRange(const std::string& min_key,
                                  const std::string& max_key) {
  // Produce a simple JSON array similar to MemoryDB/LevelDB behavior.
  std::string values = "[";
  bool first = true;
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& entry : kv_map_) {
    if (entry.first >= min_key && entry.first <= max_key) {
      if (!first) {
        values.append(",");
      }
      first = false;
      values.append(FetchValue(entry.second));
    }
  }
  values.append("]");
  return values;
}

std::map<std::string, std::vector<std::pair<std::string, uint64_t>>>
IPFSStorage::GetAllItemsWithSeq() {
  // Materialize all seq histories by fetching values from IPFS.
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> result;
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& entry : kv_map_with_seq_) {
    auto& out = result[entry.first];
    for (const auto& value_pair : entry.second) {
      out.emplace_back(FetchValue(value_pair.first), value_pair.second);
    }
  }
  return result;
}

std::map<std::string, std::pair<std::string, int>> IPFSStorage::GetAllItems() {
  // Materialize the latest version for each key.
  std::map<std::string, std::pair<std::string, int>> result;
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& entry : kv_map_with_version_) {
    if (!entry.second.empty()) {
      const auto& latest = entry.second.back();
      result.emplace(entry.first,
                     std::make_pair(FetchValue(latest.first), latest.second));
    }
  }
  return result;
}

std::map<std::string, std::pair<std::string, int>> IPFSStorage::GetKeyRange(
    const std::string& min_key, const std::string& max_key) {
  // Materialize latest versions only for keys within the range.
  std::map<std::string, std::pair<std::string, int>> result;
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& entry : kv_map_with_version_) {
    if (entry.first < min_key || entry.first > max_key || entry.second.empty()) {
      continue;
    }
    const auto& latest = entry.second.back();
    result.emplace(entry.first,
                   std::make_pair(FetchValue(latest.first), latest.second));
  }
  return result;
}

std::vector<std::pair<std::string, int>> IPFSStorage::GetHistory(
    const std::string& key, int min_version, int max_version) {
  // Materialize version history within bounds.
  std::vector<std::pair<std::string, int>> result;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = kv_map_with_version_.find(key);
  if (it == kv_map_with_version_.end()) {
    return result;
  }
  const auto& history = it->second;
  for (auto rit = history.rbegin(); rit != history.rend(); ++rit) {
    if (rit->second < min_version) {
      break;
    }
    if (rit->second <= max_version) {
      result.emplace_back(FetchValue(rit->first), rit->second);
    }
  }
  return result;
}

std::vector<std::pair<std::string, int>> IPFSStorage::GetTopHistory(
    const std::string& key, int number) {
  // Return the newest N versions by reading from history.
  auto history = GetHistory(key, 0, std::numeric_limits<int>::max());
  if (history.size() > static_cast<size_t>(number)) {
    history.resize(static_cast<size_t>(number));
  }
  return history;
}

uint64_t IPFSStorage::GetLastCheckpoint() {
  // Use the highest seq observed as the checkpoint proxy.
  std::lock_guard<std::mutex> lock(mutex_);
  return last_seq_;
}

std::string IPFSStorage::PushValue(const std::string& value) {
  // Delegate persistence to the IPFS sidecar client.
  return client_.PushBlock(value);
}

std::string IPFSStorage::FetchValue(const std::string& cid) const {
  // Delegate reads to the IPFS sidecar client.
  return client_.FetchBlock(cid);
}

std::unique_ptr<Storage> NewIPFSStorage(std::string base_url) {
  // Construct the storage wrapper for IPFS cold tier.
  return std::make_unique<IPFSStorage>(std::move(base_url));
}

}  // namespace storage
}  // namespace resdb
