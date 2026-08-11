#!/usr/bin/env bash
#
# verify.sh - THE OFFLINE JUDGE for the Gridiron minio overlay (mcp/ + deploy/).
#
# This repo is upstream minio/minio, unmodified. Only two added directories are
# ours (mcp/ and deploy/), plus this gate. deploy/smoke.sh probes an ALREADY-
# RUNNING stack on the shared network; it needs Docker, a live MinIO and the
# erp_shared_network, so it cannot check anything offline. This gate fills that
# gap: it builds and tests the sidecar, checks version pinning, validates compose,
# and lints the shell — with NO live services.
#
# It deliberately scopes every check to mcp/, deploy/ and repo-gates/ — NEVER the
# upstream engine tree.
#
# Usage:
#   ./repo-gates/verify.sh            # normal: missing tools => SKIP (yellow)
#   STRICT=1 ./repo-gates/verify.sh   # CI / real verdict: SKIP counts as FAIL
#
# Exit: 0 all passed · 1 a real failure · 2 skipped-only (unless STRICT=1 -> 1).
#
set -uo pipefail   # NOTE: no -e; we WANT every gate to run even if one fails
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh"

ROOT="$(repo_root)"
cd "${ROOT}"

printf '%s\n' "${C_BOLD}Gridiron minio overlay - repo-gates/verify.sh${C_RESET}"
printf 'mode: %s\n' "$([[ "${STRICT}" == "1" ]] && echo "STRICT (skips count as failures)" || echo "normal (skips are warnings)")"

# ── gate: go (the sidecar is a NESTED module; run from inside mcp/) ───────────
# A root-level `go build ./...` silently EXCLUDES mcp/ because it is a separate
# module — which is exactly why upstream CI never caught anything here.
section "go — sidecar fmt/vet/test (mcp/)"
if have gofmt; then
  unformatted="$(gofmt -l mcp 2>/dev/null || true)"
  if [[ -n "${unformatted}" ]]; then
    fail "gofmt: mcp files not formatted"
    note "run: gofmt -w ${unformatted//$'\n'/ }"
  else
    pass "gofmt: mcp/ formatted"
  fi
else
  skip "gofmt not found"
fi

if have go; then
  if (cd "${ROOT}/mcp" && go vet ./... ) >.gate-vet.log 2>&1; then
    pass "go vet ./mcp"
  else
    fail "go vet ./mcp"
    note "$(tail -n 15 .gate-vet.log)"
  fi
  rm -f .gate-vet.log
  if (cd "${ROOT}/mcp" && go test ./... ) >.gate-test.log 2>&1; then
    pass "go test ./mcp"
  else
    fail "go test ./mcp"
    note "$(tail -n 20 .gate-test.log)"
  fi
  rm -f .gate-test.log
else
  skip "go toolchain not found"
fi

# ── gate: pins (no floating tags on the overlay's images) ────────────────────
# Mechanical enforcement of deploy/docker-compose.yml's own header rule. Scans
# ONLY the two overlay files; scoped to image:/FROM lines so prose comments that
# mention ':latest' are never flagged.
section "pins — no floating image tags (deploy/ + mcp/)"
pins_ok=1

COMPOSE="deploy/docker-compose.yml"
if [[ -f "${COMPOSE}" ]]; then
  while IFS= read -r img; do
    [[ -z "${img}" ]] && continue
    case "${img}" in *'${'*) continue;; esac        # env-substituted, pinned via .env
    if [[ "${img}" == *:latest ]]; then
      fail "${COMPOSE}: image ${img} uses :latest"; pins_ok=0
    elif [[ "${img}" != *:* && "${img}" != *@sha256:* ]]; then
      fail "${COMPOSE}: image ${img} has no tag"; pins_ok=0
    fi
  done < <(grep -E '^[[:space:]]*image:[[:space:]]*' "${COMPOSE}" 2>/dev/null \
            | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/["'\'']//g' | sort -u || true)
else
  fail "missing ${COMPOSE}"; pins_ok=0
fi

DFILE="mcp/Dockerfile"
if [[ -f "${DFILE}" ]]; then
  while IFS= read -r line; do
    img="$(awk '{print $2}' <<< "${line}")"
    [[ -z "${img}" || "${img}" == "scratch" ]] && continue
    if [[ "${img}" == *:latest ]]; then
      fail "${DFILE}: FROM ${img} uses :latest"; pins_ok=0
    elif [[ "${img}" != *:* && "${img}" != *@sha256:* ]]; then
      fail "${DFILE}: FROM ${img} has no explicit tag"; pins_ok=0
    fi
  done < <(grep -iE '^[[:space:]]*FROM[[:space:]]+' "${DFILE}" 2>/dev/null | grep -ivE '\sfrom=' || true)
else
  fail "missing ${DFILE}"; pins_ok=0
fi
(( pins_ok == 1 )) && pass "overlay images pinned (no floating tags)"

# ── gate: compose (structurally valid + interpolates) ────────────────────────
section "compose — docker compose config -q (deploy/)"
if have docker && docker compose version >/dev/null 2>&1; then
  # Export dummy values for the REQUIRED (:?) vars so config fails only on a real
  # structural error, not on an unset var.
  if MINIO_ROOT_USER=x MINIO_ROOT_PASSWORD=xxxxxxxx \
     MCP_MINIO_ACCESS_KEY=x MCP_MINIO_SECRET_KEY=x \
     MINIO_MCP_TOKEN=x MINIO_MCP_IMAGE_TAG=0.0.0-gate \
     docker compose -f "${COMPOSE}" config -q >.gate-compose.log 2>&1; then
    pass "docker compose config: ${COMPOSE}"
  else
    fail "docker compose config: ${COMPOSE}"
    note "$(tail -n 12 .gate-compose.log)"
  fi
  rm -f .gate-compose.log
else
  skip "docker compose not available"
fi

# ── gate: shell (syntax / shellcheck on overlay + gate scripts) ──────────────
section "shell — deploy/*.sh + repo-gates/*.sh"
sh_checked=0
for sh in deploy/*.sh repo-gates/*.sh; do
  [[ -f "${sh}" ]] || continue
  sh_checked=1
  if have shellcheck; then
    if shellcheck -S error "${sh}" >.gate-sh.log 2>&1; then
      pass "shellcheck: ${sh}"
    else
      fail "shellcheck (error-level): ${sh}"
      note "$(tail -n 10 .gate-sh.log)"
    fi
  else
    if bash -n "${sh}" >.gate-sh.log 2>&1; then
      pass "bash -n: ${sh}"
    else
      fail "bash -n syntax error: ${sh}"
      note "$(tail -n 10 .gate-sh.log)"
    fi
  fi
  rm -f .gate-sh.log
done
(( sh_checked == 0 )) && skip "no shell scripts found to check"
have shellcheck || note "install shellcheck for deeper analysis (bash -n only checks syntax)"

finish "minio overlay verify"
exit $?
