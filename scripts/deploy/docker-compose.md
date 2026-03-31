# ResilientDB + IPFS Docker Compose

This compose file runs 4 ResilientDB replicas and 4 IPFS sidecars on a shared network.

## Run

From `scripts/deploy`:

```bash
docker compose -f docker-compose.yml up -d
```

## Stop

```bash
docker compose -f docker-compose.yml down
```

## Environment overrides

```bash
export RESDB_IMAGE=resilientdb:local
export IPFS_IMAGE=ipfs/kubo:latest
export RESDB_TIERED_STORAGE=1
export RESDB_TIERED_HOT_THRESHOLD=100000
export RESDB_TIERED_OFFLOAD_BATCH=1000
export RESDB_TIERED_POLL_MS=1000
export IPFS_HOST_PORT_BASE=15000
```

## Notes

- IPFS API ports are exposed as `15001-15004` by default.
- The config file is `scripts/deploy/docker/server.config`.
- Certs are mounted from `service/tools/data/cert`.
