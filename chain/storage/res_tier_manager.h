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

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace resdb {

struct ResTierManagerConfig {
  uint64_t hot_block_threshold = 100000;
  uint64_t offload_batch_size = 1000;
  std::chrono::milliseconds poll_interval{1000};
};

class ResTierManager {
 public:
  using HeightProvider = std::function<uint64_t()>;
  using OffloadCallback =
      std::function<void(const std::vector<uint64_t>& heights)>;

  // Constructs the manager with a height source and policy to decide offloads.
  ResTierManager(HeightProvider height_provider,
                 ResTierManagerConfig config,
                 OffloadCallback callback = nullptr);
  // Ensures the background worker is stopped on destruction.
  ~ResTierManager();

  // Starts a background thread to periodically compute offload candidates.
  void Start();
  // Stops the background thread and waits for it to exit safely.
  void Stop();

  // Returns and clears pending offload heights if no callback is supplied.
  std::vector<uint64_t> GetPendingOffloadHeights();
  // Executes a single polling cycle for deterministic tests or manual driving.
  std::vector<uint64_t> TickOnce();

 private:
  friend class ResTierManagerTest;
  // Worker loop that periodically calls TickOnce while running.
  void Run();
  // Computes which block heights are cold enough to offload.
  std::vector<uint64_t> ComputeOffloadCandidates(uint64_t current_height);
  // Dispatches candidates via callback or queues them for later retrieval.
  void HandleCandidates(const std::vector<uint64_t>& candidates);

 private:
  HeightProvider height_provider_;
  ResTierManagerConfig config_;
  OffloadCallback callback_;

  std::atomic<bool> running_{false};
  std::thread worker_;

  std::mutex mutex_;
  uint64_t last_offloaded_height_ = 0;
  std::vector<uint64_t> pending_offloads_;
};

}  // namespace resdb
