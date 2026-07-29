# Gridiron shared object storage (MinIO) — estate integration

This repo is **upstream `github.com/minio/minio`, unmodified** (same rule as our
other forks — the engine is not ours to rewrite). It is the estate's shared,
self-hosted **S3-compatible object store, replacing AWS S3** for every module.

Everything Gridiron-specific lives in two added directories beside the engine:

| Path | What |
|---|---|
| `mcp/` | **Contract A MCP sidecar** (Go). Lets the LangGraph brain + any module use storage as MCP tools — same shape as the openproject/pos/gdc servers. |
| `deploy/` | `docker-compose.yml` (MinIO + sidecar + bootstrap on `erp_shared_network`), the `mc` **bootstrap** (buckets, service accounts, tenant-scoped policies), and `.env.example`. |

## MCP surface (`MCP_STANDARD.md` "Contract A")
- `GET /tools?server=minio` → tool catalog (`input_schema` + `annotations.destructiveHint`); unknown `?server=` → `{"tools":[]}` (200, not 404)
- `POST /invoke` `{server,tool,arguments}` → `{tool,result[,replayed:true]}`; 400/401/404/422 per spec
- `HEAD /` → 200 (unauthenticated health so gateways probe tokenlessly)

**Tools** (reads `destructiveHint:false`, writes `true`):
`list_buckets`, `list_objects`, `stat_object`, `presign_get` · `ensure_bucket`,
`presign_put`, `put_object`, `delete_object`.

## Security (matches the other modules)
- **Caller gate**: `Authorization: Bearer` verified constant-time against
  `MCP_AUTH_TOKEN`. An **unset token fails closed** — `/tools` and `/invoke`
  answer `503` naming the server's misconfiguration, rather than serving object
  storage to anything that can reach the port. Reopening it for local dev needs
  an explicit `MCP_ALLOW_INSECURE=true` and produces a loud boot warning.
  `HEAD /` and `GET /healthz` stay exempt so probes still work.
- **Tenant isolation is structural**: tools take a `module`, never a raw bucket;
  the sidecar derives `bucket = t-<tenant>-<module>` from the authoritative
  `X-Tenant-Id` header. Both segments must **already be canonical** (lowercase
  alphanumeric, interior dashes only) — a malformed segment is refused `422`,
  never repaired. `list_buckets` only returns `t-<tenant>-*`.
- **Idempotency**: `Idempotency-Key` header → per-tenant replay of mutating
  tools, in a **bounded** store (LRU + TTL, `MINIO_MCP_REPLAY_MAX` /
  `MINIO_MCP_REPLAY_TTL`). Set `MINIO_MCP_REPLAY_REDIS_URL` to make replay
  **durable across restarts**; `/healthz` reports `replay_durable` so an
  in-memory fallback is visible rather than assumed.
- **Self-heal rail**: `ERROR`-severity records ship to the module's OpenObserve
  stream over OTLP (`OTEL_*`, same contract as the estate Python drop-in), which
  is what fires the `level=error` alert into the langgraph diagnose → fix→PR
  loop. Only 5xx faults emit; 4xx caller errors deliberately do not page.
- **Single login**: MinIO's native OIDC federation points at the `erp` realm
  (console + STS delegate to Keycloak) — config, not code (`MINIO_OIDC_*`).
- **Encryption at rest**: SSE-KMS via a KES endpoint (`MINIO_KMS_KES_*`).
- **Per-module service accounts** with policies scoped to their buckets
  (`deploy/bootstrap.sh`, `deploy/policies/`). The image tag is **pinned**.

### What is proven vs configured

Being explicit about this, because "it is in the compose file" and "it is
enforced" are different claims and only one of them is testable here.

| Control | Status | How it is proven |
|---|---|---|
| Fail-closed caller auth | **Proven** | Unit tests (unset token → 503 for any bearer; mutation-verified) + `deploy/smoke.sh` asserts the live surface never returns 200 without a valid token |
| Tenant isolation / no segment collapse | **Proven** | Handler tests drive the real S3 client and assert the bucket **on the wire**; hostile `X-Tenant-Id` values are refused |
| Replay bounded (LRU + TTL) | **Proven** | Unit tests for capacity eviction, LRU order and TTL expiry |
| Replay durable across restart | **Proven (Redis path)** | A store rebuilt over the same backing data still replays; `/healthz` reports which store is live |
| Error → OpenObserve → self-heal | **Proven (emit side)** | A real upstream failure emits an `ERROR`-severity record carrying tool + tenant; `severity_text` is asserted because OpenObserve reads `level` from it |
| Buckets are not public | **Proven (live)** | `smoke.sh` anonymous GET must be refused |
| SSE-KMS (KES) actually encrypting | **Configured only** | Needs a live KES; no automated proof |
| OIDC federation to the `erp` realm | **Configured only** | Needs a live Keycloak; no automated proof |
| Per-module service-account scoping | **Configured only** | Needs real per-module credentials to attempt a cross-module access and be denied |

The three "configured only" rows are the honest remaining gap. They are not
asserted by any gate, so a regression in them would not be caught.

## Boot it (on the shared network)
```bash
cd deploy && cp .env.example .env    # set MINIO_ROOT_PASSWORD, MCP_MINIO_SECRET_KEY, MINIO_MCP_TOKEN
docker network create erp_shared_network 2>/dev/null || true
docker compose up -d                 # minio + bootstrap (buckets/policies) + minio-mcp
docker compose exec minio mc ls gr   # buckets exist
curl -s -H "Authorization: Bearer $MINIO_MCP_TOKEN" http://localhost:8090/tools?server=minio | head
```

## How a module uses it
- **Object I/O**: talk S3 directly to `minio:9000` with the module's scoped
  service-account keys (drop-in for the AWS SDK — set endpoint + path-style).
- **Agent/brain**: via the MCP gateway → `minio-mcp:8090` (`presign_get` to hand
  a user a download link, `put_object`/`list_objects`, etc.).

## Registration (consumer side, separate repos)
- **langgraph-agents** `docker-compose.real.yml` / `ADAPTER_ROUTES`:
  `{ "kind": "contract", "url": "http://minio-mcp:8090", "server": "minio" }`
  (+ a `GATEWAY_TOKENS[minio-mcp]` entry matching `MINIO_MCP_TOKEN`). Registered
  as gateway §26 (`storage-gateway:4025`) — DONE.
- **Erp** composition root: `compose/storage.yml` overlay `include:`s
  `deploy/docker-compose.yml` (storage underpins every module) — DONE.

## Migrating a consumer off AWS S3

Almost every consumer already speaks S3 through the AWS SDK / a storage library
that exposes an **endpoint override + path-style flag** — so the migration is a
config change, not a code change. Point the module at the internal store:

| Setting | Value |
|---|---|
| Endpoint | `http://minio:9000` (container-name on `erp_shared_network`) |
| Force path-style | `true` (MinIO needs path-style, not virtual-host buckets) |
| Region | `us-east-1` (MinIO's default; any value works if the client insists) |
| TLS | off in-cluster (`http://`); terminate at the ingress for external clients |
| Access / secret key | the module's scoped pair — `S3_KEY_<MODULE>` / `S3_SECRET_<MODULE>` from `deploy/.env` (bootstrap mints a svcacct scoped to `t-*-<module>`) |
| Bucket | `t-<tenant>-<module>` (single-tenant demo: `t-default-<module>`) |

Each module gets a service account that can touch **only its own buckets**
across all tenants (`t-*-<module>`); it cannot read another module's blobs.
Modules that need broader reach can fall back to the `gridiron-storage` account
(all `t-*`), but prefer the scoped one.

The bucket already exists if the module is in `BOOTSTRAP_MODULES`; otherwise add
it there (and a `S3_KEY_/S3_SECRET_` pair) and re-run bootstrap — it's
idempotent.

If a consumer's S3 client is **hardcoded to AWS with no endpoint override**, add
the override (SDK v3: `endpoint` + `forcePathStyle`; boto3: `endpoint_url` +
`s3={'addressing_style':'path'}`; Rails ActiveStorage: `endpoint:` +
`force_path_style: true` on the `:amazon`/`:s3` service) — a few lines, no logic
change.
