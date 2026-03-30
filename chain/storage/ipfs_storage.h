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

#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "chain/storage/ipfs_sidecar_client.h"
#include "chain/storage/storage.h"

namespace resdb {
namespace storage {

// Cold storage implementation backed by an IPFS sidecar.
class IPFSStorage : public Storage {
 public:
  // Creates a storage instance that talks to the IPFS sidecar.
  explicit IPFSStorage(std::string base_url = "http://127.0.0.1:5001");
  // Defaulted destructor; resources are owned by standard containers.
  ~IPFSStorage() override = default;

  // Stores a value in IPFS and keeps the CID mapped to the key.
  int SetValue(const std::string& key, const std::string& value) override;
  int SetValueWithSeq(const std::string& key, const std::string& value,
                      uint64_t seq) override;
  int SetValueWithVersion(const std::string& key, const std::string& value,
                          int version) override;

  // Retrieves values by resolving CIDs from the in-memory index.
  std::string GetValue(const std::string& key) override;
  std::pair<std::string, uint64_t> GetValueWithSeq(
      const std::string& key, uint64_t seq) override;
  std::pair<std::string, int> GetValueWithVersion(
      const std::string& key, int version) override;

  // Range and list operations are best-effort using the local index.
  std::string GetRange(const std::string& min_key,
                       const std::string& max_key) override;
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>>
  GetAllItemsWithSeq() override;
  std::map<std::string, std::pair<std::string, int>> GetAllItems() override;
  std::map<std::string, std::pair<std::string, int>> GetKeyRange(
      const std::string& min_key, const std::string& max_key) override;

  // History queries use the version index to fetch stored values.
  std::vector<std::pair<std::string, int>> GetHistory(
      const std::string& key, int min_version, int max_version) override;
  std::vector<std::pair<std::string, int>> GetTopHistory(
      const std::string& key, int number) override;

  // Returns the highest seq observed to help tiering decisions.
  uint64_t GetLastCheckpoint() override;

 private:
  // Pushes data to IPFS and returns the CID (or "" on failure).
  std::string PushValue(const std::string& value);
  // Fetches data from IPFS by CID (or "" on failure).
  std::string FetchValue(const std::string& cid) const;

 private:
  IPFSSidecarClient client_;
  std::mutex mutex_;

  std::map<std::string, std::string> kv_map_;
  std::map<std::string, std::vector<std::pair<std::string, uint64_t>>>
      kv_map_with_seq_;
  std::map<std::string, std::vector<std::pair<std::string, int>>>
      kv_map_with_version_;

  uint64_t last_seq_ = 0;
};

// Factory for constructing IPFS-backed cold storage.
std::unique_ptr<Storage> NewIPFSStorage(
    std::string base_url = "http://127.0.0.1:5001");

}  // namespace storage
}  // namespace resdb
