#!/usr/bin/env bash
set -euo pipefail

# Spins up 4 ResilientDB containers and 4 IPFS sidecars on a docker network.
# Assumes you run this from within the repo, or at least from a path inside it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

RESDB_IMAGE="${RESDB_IMAGE:-resilientdb:local}"
IPFS_IMAGE="${IPFS_IMAGE:-ipfs/kubo:latest}"
DOCKER_NETWORK="${DOCKER_NETWORK:-resdb-net}"
CONFIG_DIR="${CONFIG_DIR:-${ROOT_DIR}/scripts/deploy/config_out_docker}"

# Tiering configuration (overridable via environment).
RESDB_TIERED_STORAGE="${RESDB_TIERED_STORAGE:-1}"
RESDB_TIERED_HOT_THRESHOLD="${RESDB_TIERED_HOT_THRESHOLD:-100000}"
RESDB_TIERED_OFFLOAD_BATCH="${RESDB_TIERED_OFFLOAD_BATCH:-1000}"
RESDB_TIERED_POLL_MS="${RESDB_TIERED_POLL_MS:-1000}"

# Use host ports 15001-15004 for IPFS APIs so you can inspect them locally.
IPFS_HOST_PORT_BASE="${IPFS_HOST_PORT_BASE:-15000}"

function ensure_network() {
  if ! docker network inspect "${DOCKER_NETWORK}" >/dev/null 2>&1; then
    docker network create "${DOCKER_NETWORK}" >/dev/null
  fi
}

function build_image() {
  # Build ResilientDB image if it does not exist locally.
  if ! docker image inspect "${RESDB_IMAGE}" >/dev/null 2>&1; then
    docker build -t "${RESDB_IMAGE}" -f "${ROOT_DIR}/Docker/Dockerfile" "${ROOT_DIR}"
  fi
}

function prepare_config() {
  mkdir -p "${CONFIG_DIR}"
  # Generate a docker-friendly server.config with container hostnames.
  cat > "${CONFIG_DIR}/server.config" <<EOF
{
  region : {
    replica_info : { id:1, ip:"resdb1", port: 10001, },
    replica_info : { id:2, ip:"resdb2", port: 10002, },
    replica_info : { id:3, ip:"resdb3", port: 10003, },
    replica_info : { id:4, ip:"resdb4", port: 10004, },
    region_id: 1,
  },
  self_region_id:1,
  leveldb_info : {
    write_buffer_size_mb:128,
    write_batch_size:1,
    enable_block_cache: true,
    block_cache_capacity: 100
  },
  require_txn_validation:true,
  enable_viewchange:false,
  enable_resview:true,
  enable_faulty_switch:false
}
EOF

  # Copy existing certs/keys so each container can mount a single config folder.
  cp -f "${ROOT_DIR}/service/tools/data/cert/node"*.key.pri "${CONFIG_DIR}/"
  cp -f "${ROOT_DIR}/service/tools/data/cert/cert_"*.cert "${CONFIG_DIR}/"
}

function cleanup_existing() {
  for i in 1 2 3 4; do
    docker rm -f "ipfs${i}" "resdb${i}" >/dev/null 2>&1 || true
  done
}

function start_ipfs() {
  for i in 1 2 3 4; do
    local host_port=$((IPFS_HOST_PORT_BASE + i))
    docker run -d --name "ipfs${i}" \
      --network "${DOCKER_NETWORK}" \
      -p "${host_port}:5001" \
      "${IPFS_IMAGE}" >/dev/null
  done
}

function start_resilientdb() {
  for i in 1 2 3 4; do
    docker run -d --name "resdb${i}" \
      --network "${DOCKER_NETWORK}" \
      -e RESDB_TIERED_STORAGE="${RESDB_TIERED_STORAGE}" \
      -e RESDB_TIERED_HOT_THRESHOLD="${RESDB_TIERED_HOT_THRESHOLD}" \
      -e RESDB_TIERED_OFFLOAD_BATCH="${RESDB_TIERED_OFFLOAD_BATCH}" \
      -e RESDB_TIERED_POLL_MS="${RESDB_TIERED_POLL_MS}" \
      -e RESDB_IPFS_URL="http://ipfs${i}:5001" \
      -v "${CONFIG_DIR}:/config:ro" \
      "${RESDB_IMAGE}" \
      bash -lc "bazel build //service/kv:kv_service && \
        ./bazel-bin/service/kv/kv_service /config/server.config \
        /config/node${i}.key.pri /config/cert_${i}.cert" >/dev/null
  done
}

function main() {
  ensure_network
  build_image
  prepare_config
  cleanup_existing
  start_ipfs
  start_resilientdb
  echo "Started 4 ResilientDB replicas with 4 IPFS sidecars."
  echo "IPFS API ports: 15001-15004 on the host by default."
}

main "$@"
