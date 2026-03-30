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

#include "chain/storage/res_tier_manager.h"

#include <algorithm>
#include <utility>

namespace resdb {

ResTierManager::ResTierManager(HeightProvider height_provider,
                               ResTierManagerConfig config,
                               OffloadCallback callback)
    // Store the sources and callbacks that drive tiering decisions.
    : height_provider_(std::move(height_provider)),
      config_(std::move(config)),
      callback_(std::move(callback)) {}

ResTierManager::~ResTierManager() {
  // Ensure background work is stopped to avoid dangling threads.
  Stop();
}

void ResTierManager::Start() {
  // Avoid starting multiple worker threads.
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;
  }
  // Spin up the polling loop in a dedicated thread.
  worker_ = std::thread(&ResTierManager::Run, this);
}

void ResTierManager::Stop() {
  // Only stop when the worker is running.
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }
  // Join so callers know all work is fully stopped.
  if (worker_.joinable()) {
    worker_.join();
  }
}

std::vector<uint64_t> ResTierManager::GetPendingOffloadHeights() {
  // Expose queued candidates to callers when no callback is used.
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<uint64_t> result;
  result.swap(pending_offloads_);
  return result;
}

std::vector<uint64_t> ResTierManager::TickOnce() {
  // Query the latest height and produce offload candidates.
  uint64_t current_height = 0;
  if (height_provider_) {
    current_height = height_provider_();
  }
  auto candidates = ComputeOffloadCandidates(current_height);
  HandleCandidates(candidates);
  return candidates;
}

void ResTierManager::Run() {
  // Poll on a fixed interval while the manager is running.
  while (running_.load()) {
    TickOnce();
    std::this_thread::sleep_for(config_.poll_interval);
  }
}

std::vector<uint64_t> ResTierManager::ComputeOffloadCandidates(
    uint64_t current_height) {
  // Only offload when we have more than the configured hot range.
  if (current_height <= config_.hot_block_threshold) {
    return {};
  }

  // Compute the maximum height eligible for offloading.
  uint64_t max_offload_height =
      current_height - config_.hot_block_threshold;

  std::lock_guard<std::mutex> lock(mutex_);
  // Continue from the last height we offloaded to avoid duplicates.
  uint64_t start_height = last_offloaded_height_ + 1;
  if (start_height > max_offload_height) {
    return {};
  }

  // Limit work per tick to the configured batch size.
  uint64_t end_height =
      std::min(max_offload_height,
               start_height + config_.offload_batch_size - 1);

  std::vector<uint64_t> candidates;
  candidates.reserve(static_cast<size_t>(end_height - start_height + 1));
  // Enumerate the heights to offload this cycle.
  for (uint64_t height = start_height; height <= end_height; ++height) {
    candidates.push_back(height);
  }
  // Advance the cursor so the next tick continues after this batch.
  last_offloaded_height_ = end_height;
  return candidates;
}

void ResTierManager::HandleCandidates(
    const std::vector<uint64_t>& candidates) {
  // Skip work if there is nothing to offload.
  if (candidates.empty()) {
    return;
  }
  // Prefer the callback path when provided (push model).
  if (callback_) {
    callback_(candidates);
    return;
  }
  // Otherwise queue candidates for pull-based consumption.
  std::lock_guard<std::mutex> lock(mutex_);
  pending_offloads_.insert(pending_offloads_.end(), candidates.begin(),
                           candidates.end());
}

}  // namespace resdb
