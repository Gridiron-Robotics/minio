# repo-gates — the offline judge for the Gridiron minio overlay

This repo is **upstream `minio/minio`, unmodified**. Only two directories are
ours — `mcp/` (the Contract A MCP sidecar) and `deploy/` (compose + bootstrap) —
plus this gate. These checks never touch the upstream engine tree.

`STRICT=1 ./repo-gates/verify.sh` is the verdict for the overlay. It runs
**offline** — no live MinIO, no Docker network, no GPU:

| Gate | What it proves |
|---|---|
| **go** | `gofmt -l mcp` is clean, and `go vet ./...` + `go test ./...` pass **inside `mcp/`** (it is a nested module — a root-level `go build ./...` silently excludes it, which is why upstream CI never caught anything here). |
| **pins** | No floating tags on the overlay's images — every `image:` in `deploy/docker-compose.yml` and every `FROM` in `mcp/Dockerfile` carries an explicit tag or digest (mechanical enforcement of the compose header's own supply-chain rule). |
| **compose** | `docker compose config -q` parses and interpolates `deploy/docker-compose.yml` with dummy values exported for the required (`:?`) vars. |
| **shell** | `shellcheck -S error` (or `bash -n` fallback) on `deploy/*.sh` and `repo-gates/*.sh`. |

```bash
./repo-gates/verify.sh            # normal: a missing tool => SKIP (yellow)
STRICT=1 ./repo-gates/verify.sh   # CI / real verdict: SKIP counts as FAIL
```

Exit codes: `0` all passed · `1` a real failure · `2` skipped-only (→ `1` under
`STRICT=1`).

## This is separate from `deploy/smoke.sh`

`deploy/smoke.sh` is the **live-stack probe**: it boots nothing and probes the
already-running store by container name on `erp_shared_network`. It needs Docker,
a live MinIO and the shared network. `repo-gates/verify.sh` is the **offline
gate** that runs with none of that. Both must be green.

## The one hard rule

**No gate file may be edited to make a check pass.** The check is right; the code
is wrong — fix the code. Weakening a gate to go green defeats its only purpose.
