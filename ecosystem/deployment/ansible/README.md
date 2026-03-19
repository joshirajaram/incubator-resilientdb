#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# resilientdb-ansible

Docker image to provision and run ResilientDB along with supporting services (GraphQL, Crow HTTP server, Nginx) using systemd and Ansible.

---

## Quick Start

Build the Docker image (usually takes ~10 min the first time):

```bash
cd ecosystem/deployment/ansible
docker build -t resilientdb-ansible .
```

If you cannot build the project successfully, try to reduce the bazel jobs [here](
https://github.com/apache/incubator-resilientdb/tree/master/ecosystem/deployment/ansible/dockerfile#41).

Run the container:

```bash
docker run --privileged -v /sys/fs/cgroup:/sys/fs/cgroup:ro -p 80:80 -p 18000:18000 -p 8000:8000 resilientdb-ansible
```
