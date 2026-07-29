#!/usr/bin/env bash
#
# smoke.sh — prove the shared object store is live and integrated.
#
# Boots nothing; it probes the ALREADY-RUNNING storage stack (deploy/
# docker-compose.yml) BY CONTAINER NAME on `erp_shared_network` — the exact path
# the brain's storage-gateway and every module's S3 client use. Probing on the
# shared network (not published host ports) is deliberate: it sidesteps
# single-host port collisions and tests the integration path that matters.
#
# What it checks, in order:
#   1. erp_shared_network exists.
#   2. Engine   — minio liveness (`/minio/health/live`) is 200.
#   3. Sidecar  — minio-mcp health (`HEAD /`) is 200 (unauthenticated probe).
#   4. Contract — minio-mcp `/tools?server=minio` answers. Without a bearer it is
#                 401 (auth-gated = UP); with MINIO_MCP_TOKEN set it is 200 and
#                 must advertise the `presign_get` tool.
#
# A surface answering 401/403 still counts as UP (booted, auth-gated). Only a
# connection failure or 5xx is a FAIL.
#
# Usage:
#   deploy/smoke.sh                                   # probe the running stack
#   RETRIES=30 RETRY_SLEEP=5 deploy/smoke.sh          # slower boots
#   MINIO_MCP_TOKEN=... deploy/smoke.sh               # also assert the catalog
#
# Exit code: 0 iff every required probe passed.

set -uo pipefail

NETWORK="${NETWORK:-erp_shared_network}"
RETRIES="${RETRIES:-20}"
RETRY_SLEEP="${RETRY_SLEEP:-5}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.10.1}"

# Estate hostnames (must match deploy/docker-compose.yml + langgraph-agents
# INTEGRATIONS.md §26).
ENGINE_URL="http://minio:9000/minio/health/live"
MCP_HEALTH_URL="http://minio-mcp:8090/"
MCP_TOOLS_URL="http://minio-mcp:8090/tools?server=minio"

pass=0
fail=0
declare -a RESULTS=()

log()  { printf '%s\n' "$*" >&2; }
ok()   { RESULTS+=("PASS  $1"); pass=$((pass + 1)); }
bad()  { RESULTS+=("FAIL  $1"); fail=$((fail + 1)); }
skip() { RESULTS+=("SKIP  $1"); }

# curl_in_net <curl-args...> -> prints the curl output (empty on failure).
curl_in_net() {
  docker run --rm --network "$NETWORK" "$CURL_IMAGE" --max-time 8 "$@" 2>/dev/null
}

# status_in_net <url> [extra curl args] -> prints HTTP status (000 on failure).
status_in_net() {
  local url="$1"; shift
  curl_in_net -s -o /dev/null -w '%{http_code}' "$@" "$url"
}

# probe <label> <url> [required] — UP = any HTTP response < 500.
probe() {
  local label="$1" url="$2" required="${3:-yes}" code=""
  for _ in $(seq 1 "$RETRIES"); do
    code="$(status_in_net "$url")"
    if [[ -n "$code" && "$code" != "000" && "$code" -lt 500 ]]; then
      ok "$label ($code) $url"
      return 0
    fi
    sleep "$RETRY_SLEEP"
  done
  if [[ "$required" == "yes" ]]; then bad "$label (last=$code) $url"; else skip "$label (last=$code, optional) $url"; fi
  return 1
}

# ── 1 · shared network ──────────────────────────────────────────────────────
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  log "FATAL: docker network '$NETWORK' not found. Bring up the store first:"
  log "  docker compose -f deploy/docker-compose.yml up -d"
  exit 2
fi
ok "network $NETWORK exists"

# ── 2-3 · engine + sidecar health ───────────────────────────────────────────
log "==> probing the object store on $NETWORK (retries=$RETRIES, sleep=${RETRY_SLEEP}s)"
probe "engine/minio"      "$ENGINE_URL"
probe "sidecar/minio-mcp" "$MCP_HEALTH_URL"    # HEAD-less GET; root returns 200 on HEAD, 404 on GET — both <500 = UP
probe "contract/minio-mcp /tools" "$MCP_TOOLS_URL"

# ── 4 · authenticated catalog assertion (optional) ──────────────────────────
if [[ -n "${MINIO_MCP_TOKEN:-}" ]]; then
  body="$(curl_in_net -s -H "Authorization: Bearer ${MINIO_MCP_TOKEN}" "$MCP_TOOLS_URL")"
  if printf '%s' "$body" | grep -q '"presign_get"'; then
    ok "contract/minio-mcp catalog advertises presign_get"
  else
    bad "contract/minio-mcp catalog missing presign_get (got: ${body:0:120})"
  fi
else
  skip "contract/minio-mcp catalog — set MINIO_MCP_TOKEN to assert the tool list"
fi

# ── 5 · security posture, asserted rather than assumed ──────────────────────
# Liveness alone cannot tell an authenticated surface from an open one: a
# sidecar deployed with MCP_AUTH_TOKEN unset used to answer 200 to ANY bearer,
# and every probe above would still have reported it healthy. These checks look
# for the specific failure, not for signs of life.

# 5a. The tool surface must never serve an unauthenticated caller. 401 (no
#     credential) and 503 (server has no token configured, fail-closed) are both
#     correct refusals. 200 is the bug.
code="$(status_in_net "$MCP_TOOLS_URL")"
case "$code" in
  401|503) ok "security/minio-mcp refuses unauthenticated /tools ($code)" ;;
  200)     bad "security/minio-mcp SERVED /tools WITH NO CREDENTIAL (200) — the surface is open to anything on $NETWORK" ;;
  *)       bad "security/minio-mcp unauthenticated /tools returned unexpected $code (want 401 or 503)" ;;
esac

# 5b. An arbitrary bearer must not be accepted either. This is the exact
#     fail-open that shipped: presence-only auth treated any non-empty token as
#     valid, which on a flat network is every container in the cluster.
code="$(status_in_net "$MCP_TOOLS_URL" -H "Authorization: Bearer definitely-not-the-real-token")"
case "$code" in
  401|503) ok "security/minio-mcp rejects an arbitrary bearer ($code)" ;;
  200)     bad "security/minio-mcp ACCEPTED AN ARBITRARY BEARER (200) — presence-only auth is back" ;;
  *)       bad "security/minio-mcp arbitrary bearer returned unexpected $code (want 401 or 503)" ;;
esac

# 5c. Posture, self-reported. /healthz is unauthenticated on purpose so an
#     orchestrator can distinguish "misconfigured" from "down", and it states
#     whether idempotency replay actually survives a restart — a silent
#     downgrade to the in-memory store is otherwise indistinguishable from
#     durability.
health="$(curl_in_net -s "http://minio-mcp:8090/healthz")"
if [[ -z "$health" ]]; then
  bad "security/minio-mcp /healthz did not answer"
else
  case "$health" in
    *'"auth":"insecure-explicitly-allowed"'*)
      bad "security/minio-mcp is running with MCP_ALLOW_INSECURE — no caller authentication" ;;
    *'"auth":"fail-closed"'*)
      bad "security/minio-mcp has no MCP_AUTH_TOKEN configured; it is refusing all tool traffic" ;;
    *'"auth":"token-verify"'*)
      ok "security/minio-mcp is enforcing token-verify auth" ;;
    *)
      bad "security/minio-mcp /healthz auth mode unrecognised: ${health:0:160}" ;;
  esac
  case "$health" in
    *'"replay_durable":true'*)  ok "durability/minio-mcp idempotency replay survives a restart" ;;
    *'"replay_durable":false'*) skip "durability/minio-mcp replay is in-memory — a retry after a restart re-executes (set MINIO_MCP_REPLAY_REDIS_URL)" ;;
    *)                          bad "durability/minio-mcp /healthz did not report replay_durable" ;;
  esac
fi

# 5d. Tenant buckets must not be world-readable. bootstrap.sh creates them
#     private and SSE-KMS applies at rest, but neither is proven by a config
#     file — an anonymous GET is.
probe_bucket="${SMOKE_PROBE_BUCKET:-t-default-safedocs}"
code="$(status_in_net "http://minio:9000/${probe_bucket}/")"
case "$code" in
  403|401) ok "security/minio bucket ${probe_bucket} refuses anonymous access ($code)" ;;
  404)     skip "security/minio bucket ${probe_bucket} not present — set SMOKE_PROBE_BUCKET to one that is" ;;
  200)     bad "security/minio bucket ${probe_bucket} IS PUBLICLY LISTABLE (200)" ;;
  *)       bad "security/minio anonymous bucket probe returned unexpected $code" ;;
esac

# NOTE — deliberately NOT claimed as verified here: SSE-KMS (KES) key usage,
# OIDC federation to the `erp` realm, and per-module service-account scoping.
# They are configured in bootstrap.sh + docker-compose.yml, but proving them
# needs a live KES and Keycloak plus real per-module credentials, which this
# probe does not have. Asserting them from config alone would be a green light
# for something never tested. See README.md § "What is proven vs configured".

# ── report ──────────────────────────────────────────────────────────────────
log ""
log "──────── minio smoke ────────"
for r in "${RESULTS[@]}"; do log "  $r"; done
log "─────────────────────────────"
log "  $pass passed, $fail failed"

[[ "$fail" -eq 0 ]] && exit 0 || exit 1
