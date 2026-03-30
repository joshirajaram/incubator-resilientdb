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

#include "chain/storage/ipfs_sidecar_client.h"

#include <curl/curl.h>

#include <cctype>
#include <string>

namespace resdb {
namespace {

class CurlGlobalInit {
 public:
  // Initializes libcurl globally once for the process lifetime.
  CurlGlobalInit() { curl_global_init(CURL_GLOBAL_DEFAULT); }
  // Cleans up libcurl global state on shutdown.
  ~CurlGlobalInit() { curl_global_cleanup(); }
};

CurlGlobalInit curl_global_init_guard;

// Normalizes whitespace checks for JSON parsing.
bool IsWhitespace(char value) {
  return std::isspace(static_cast<unsigned char>(value)) != 0;
}

}  // namespace

IPFSSidecarClient::IPFSSidecarClient(std::string base_url)
    // Store the base URL so all requests share a consistent prefix.
    : base_url_(std::move(base_url)) {}

std::string IPFSSidecarClient::BuildUrl(
    const std::string& path_query) const {
  // Compose a full URL while avoiding duplicate or missing slashes.
  if (base_url_.empty()) {
    return path_query;
  }
  if (base_url_.back() == '/' && !path_query.empty() && path_query.front() == '/') {
    return base_url_ + path_query.substr(1);
  }
  if (base_url_.back() != '/' && !path_query.empty() && path_query.front() != '/') {
    return base_url_ + "/" + path_query;
  }
  return base_url_ + path_query;
}

size_t IPFSSidecarClient::WriteCallback(char* ptr, size_t size, size_t nmemb,
                                        void* userdata) {
  // libcurl callback writes response bytes into the caller's buffer.
  if (userdata == nullptr || ptr == nullptr) {
    return 0;
  }
  auto* buffer = static_cast<std::string*>(userdata);
  buffer->append(ptr, size * nmemb);
  return size * nmemb;
}

std::string IPFSSidecarClient::ExtractJsonStringField(
    const std::string& body, const std::string& field) {
  // Lightweight string extraction for simple JSON responses (no full parser).
  std::string needle = "\"" + field + "\"";
  std::size_t pos = body.find(needle);
  if (pos == std::string::npos) {
    return "";
  }
  pos = body.find(':', pos + needle.size());
  if (pos == std::string::npos) {
    return "";
  }
  ++pos;
  while (pos < body.size() && IsWhitespace(body[pos])) {
    ++pos;
  }
  if (pos >= body.size() || body[pos] != '"') {
    return "";
  }
  ++pos;
  std::string value;
  while (pos < body.size()) {
    char ch = body[pos];
    if (ch == '\\') {
      if (pos + 1 < body.size()) {
        value.push_back(body[pos + 1]);
        pos += 2;
        continue;
      }
      break;
    }
    if (ch == '"') {
      break;
    }
    value.push_back(ch);
    ++pos;
  }
  return value;
}

std::string IPFSSidecarClient::PushBlock(std::string serialized_block) {
  // Push a block via IPFS HTTP API and return the resulting CID.
  CURL* curl = curl_easy_init();
  if (curl == nullptr) {
    return "";
  }

  std::string response;
  std::string url = BuildUrl("/api/v0/add?pin=true");

  curl_mime* mime = curl_mime_init(curl);
  curl_mimepart* part = curl_mime_addpart(mime);
  curl_mime_name(part, "file");
  curl_mime_data(part, serialized_block.data(), serialized_block.size());

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_MIMEPOST, mime);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_seconds_);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &IPFSSidecarClient::WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

  CURLcode res = curl_easy_perform(curl);

  curl_mime_free(mime);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    return "";
  }

  return ExtractJsonStringField(response, "Hash");
}

std::string IPFSSidecarClient::FetchBlock(std::string cid) {
  // Fetch a block's raw bytes from IPFS by CID.
  CURL* curl = curl_easy_init();
  if (curl == nullptr) {
    return "";
  }

  std::string response;
  char* escaped_cid = curl_easy_escape(curl, cid.c_str(), cid.size());
  std::string url = BuildUrl("/api/v0/cat?arg=");
  if (escaped_cid != nullptr) {
    url += escaped_cid;
    curl_free(escaped_cid);
  } else {
    url += cid;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_seconds_);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &IPFSSidecarClient::WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

  CURLcode res = curl_easy_perform(curl);

  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    return "";
  }

  return response;
}

}  // namespace resdb
