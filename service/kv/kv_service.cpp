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

#include <glog/logging.h>

#include <chrono>
#include <cstdlib>

#include "chain/storage/ipfs_storage.h"
#include "chain/storage/memory_db.h"
#include "chain/storage/tiered_storage.h"
#include "executor/kv/kv_executor.h"
#include "platform/config/resdb_config_utils.h"
#include "platform/statistic/stats.h"
#include "service/utils/server_factory.h"
#ifdef ENABLE_LEVELDB
#include "chain/storage/leveldb.h"
#endif

using namespace resdb;
using namespace resdb::storage;

void SignalHandler(int sig_num) {
  LOG(ERROR) << " signal:" << sig_num << " call"
             << " ======================";
}

void ShowUsage() {
  printf("<config> <private_key> <cert_file> [logging_dir]\n");
}

bool IsTieredStorageEnabled() {
  const char* value = std::getenv("RESDB_TIERED_STORAGE");
  if (value == nullptr) {
    return false;
  }
  std::string flag(value);
  return flag == "1" || flag == "true" || flag == "TRUE" || flag == "yes" ||
         flag == "YES";
}

std::string GetIpfsBaseUrl() {
  const char* value = std::getenv("RESDB_IPFS_URL");
  if (value == nullptr) {
    return "http://127.0.0.1:5001";
  }
  return std::string(value);
}

ResTierManagerConfig GetTierManagerConfig() {
  ResTierManagerConfig config;
  const char* threshold_value = std::getenv("RESDB_TIERED_HOT_THRESHOLD");
  if (threshold_value != nullptr) {
    char* endptr = nullptr;
    uint64_t parsed = std::strtoull(threshold_value, &endptr, 10);
    if (endptr != threshold_value) {
      config.hot_block_threshold = parsed;
    }
  }

  const char* batch_value = std::getenv("RESDB_TIERED_OFFLOAD_BATCH");
  if (batch_value != nullptr) {
    char* endptr = nullptr;
    uint64_t parsed = std::strtoull(batch_value, &endptr, 10);
    if (endptr != batch_value) {
      config.offload_batch_size = parsed;
    }
  }

  const char* poll_value = std::getenv("RESDB_TIERED_POLL_MS");
  if (poll_value != nullptr) {
    char* endptr = nullptr;
    uint64_t parsed = std::strtoull(poll_value, &endptr, 10);
    if (endptr != poll_value) {
      config.poll_interval = std::chrono::milliseconds(parsed);
    }
  }

  return config;
}

std::unique_ptr<Storage> NewStorage(const std::string& db_path,
                                    const ResConfigData& config_data) {
#ifdef ENABLE_LEVELDB
  if (IsTieredStorageEnabled()) {
    LOG(INFO) << "use tiered leveldb storage.";
    auto hot = NewResLevelDB(db_path, config_data.leveldb_info());
    auto cold = NewIPFSStorage(GetIpfsBaseUrl());
    return NewTieredStorage(std::move(hot), std::move(cold),
                            GetTierManagerConfig());
  }
  LOG(INFO) << "use leveldb storage.";
  return NewResLevelDB(db_path, config_data.leveldb_info());
#endif
  if (IsTieredStorageEnabled()) {
    LOG(INFO) << "use tiered memory storage.";
    return NewTieredStorage(NewMemoryDB(), NewIPFSStorage(GetIpfsBaseUrl()),
                            GetTierManagerConfig());
  }
  LOG(INFO) << "use memory storage.";
  return NewMemoryDB();
}

int main(int argc, char** argv) {
  if (argc < 4) {
    ShowUsage();
    exit(0);
  }
  google::InitGoogleLogging(argv[0]);
  FLAGS_minloglevel = 0;
  signal(SIGINT, SignalHandler);
  signal(SIGKILL, SignalHandler);

  char* config_file = argv[1];
  char* private_key_file = argv[2];
  char* cert_file = argv[3];

  if (argc == 5) {
    std::string grafana_port = argv[4];
    std::string grafana_address = "0.0.0.0:" + grafana_port;

    auto monitor_port = Stats::GetGlobalStats(5);
    monitor_port->SetPrometheus(grafana_address);
    LOG(ERROR) << "monitoring port:" << grafana_address;
  }

  std::unique_ptr<ResDBConfig> config =
      GenerateResDBConfig(config_file, private_key_file, cert_file);
  ResConfigData config_data = config->GetConfigData();

  std::string db_path = std::to_string(config->GetSelfInfo().port()) + "_db/";
  LOG(ERROR) << "db path:" << db_path;

  auto server = GenerateResDBServer(
      config_file, private_key_file, cert_file,
      std::make_unique<KVExecutor>(NewStorage(db_path, config_data)), nullptr);
  server->Run();
}
