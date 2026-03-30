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

#include <string>

namespace resdb {

class IPFSSidecarClient {
 public:
  // Initializes the client with the sidecar base URL for all API calls.
  explicit IPFSSidecarClient(
      std::string base_url = "http://127.0.0.1:5001");

  // Pushes a serialized block to IPFS and returns its CID (or "" on failure).
  std::string PushBlock(std::string serialized_block);
  // Fetches a block by CID from IPFS (or "" on failure).
  std::string FetchBlock(std::string cid);

 private:
  // Builds a full URL from a path/query to keep request formatting consistent.
  std::string BuildUrl(const std::string& path_query) const;
  // libcurl callback that writes response bytes into a std::string buffer.
  static size_t WriteCallback(char* ptr, size_t size, size_t nmemb,
                              void* userdata);
  // Extracts a string field from a simple JSON response body.
  static std::string ExtractJsonStringField(const std::string& body,
                                            const std::string& field);

 private:
  std::string base_url_;
  long timeout_seconds_ = 30;
};

}  // namespace resdb
