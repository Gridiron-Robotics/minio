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
  `MCP_AUTH_TOKEN` (presence-only when unset — never open). `HEAD /` exempt.
- **Tenant isolation is structural**: tools take a `module`, never a raw bucket;
  the sidecar derives `bucket = t-<tenant>-<module>` from the authoritative
  `X-Tenant-Id` header and sanitizes both segments, so a caller can **never**
  name another tenant's bucket. `list_buckets` only returns `t-<tenant>-*`.
- **Idempotency**: `Idempotency-Key` header → per-tenant replay of mutating tools.
- **Single login**: MinIO's native OIDC federation points at the `erp` realm
  (console + STS delegate to Keycloak) — config, not code (`MINIO_OIDC_*`).
- **Encryption at rest**: SSE-KMS via a KES endpoint (`MINIO_KMS_KES_*`).
- **Per-module service accounts** with policies scoped to their buckets
  (`deploy/bootstrap.sh`, `deploy/policies/`). The image tag is **pinned**.

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
  (+ a `GATEWAY_TOKENS[minio-mcp]` entry matching `MINIO_MCP_TOKEN`).
- **Erp** composition root: add `deploy/docker-compose.yml` as a core-spine
  overlay (storage underpins every module).
