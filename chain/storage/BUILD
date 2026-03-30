# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "storage",
    hdrs = ["storage.h"],
    deps = [
    ],
)

cc_library(
    name = "mock_storage",
    hdrs = ["mock_storage.h"],
    deps = [
        ":storage",
    ],
)

cc_library(
    name = "memory_db",
    srcs = ["memory_db.cpp"],
    hdrs = ["memory_db.h"],
    deps = [
        ":storage",
        "//common:comm",
    ],
)

cc_library(
    name = "res_tier_manager",
    srcs = ["res_tier_manager.cpp"],
    hdrs = ["res_tier_manager.h"],
    deps = [
    ],
)

cc_library(
    name = "tiered_storage",
    srcs = ["tiered_storage.cpp"],
    hdrs = ["tiered_storage.h"],
    deps = [
        ":res_tier_manager",
        ":storage",
    ],
)

cc_library(
    name = "ipfs_sidecar_client",
    srcs = ["ipfs_sidecar_client.cpp"],
    hdrs = ["ipfs_sidecar_client.h"],
    linkopts = ["-lcurl"],
    deps = [
    ],
)

cc_library(
    name = "ipfs_storage",
    srcs = ["ipfs_storage.cpp"],
    hdrs = ["ipfs_storage.h"],
    deps = [
        ":ipfs_sidecar_client",
        ":storage",
        "//common:comm",
    ],
)

cc_library(
    name = "leveldb",
    srcs = ["leveldb.cpp"],
    hdrs = ["leveldb.h"],
    deps = [
        ":storage",
        "//chain/storage/proto:kv_cc_proto",
        "//chain/storage/proto:leveldb_config_cc_proto",
        "//common:comm",
        "//common/lru:lru_cache",
        "//platform/statistic:stats",
        "//third_party:leveldb",
    ],
)

cc_test(
    name = "kv_storage_test",
    size = "small",  # Set the size to "small"
    timeout = "short",  # Set the timeout to "short"
    srcs = ["kv_storage_test.cpp"],
    deps = [
        ":leveldb",
        ":memory_db",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "leveldb_test",
    size = "small",  # Set the size to "small"
    timeout = "short",  # Set the timeout to "short"
    srcs = ["leveldb_test.cpp"],
    deps = [
        ":leveldb",
        "//common/test:test_main",
        "//platform/statistic:stats",
    ],
)

cc_test(
    name = "res_tier_manager_test",
    size = "small",
    timeout = "short",
    srcs = ["res_tier_manager_test.cpp"],
    deps = [
        ":memory_db",
        ":res_tier_manager",
        ":tiered_storage",
        "//common/test:test_main",
    ],
)
