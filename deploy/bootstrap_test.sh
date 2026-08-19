#!/usr/bin/env bash
#
# bootstrap_test.sh — behavioural coverage for deploy/bootstrap.sh.
#
# WHAT THIS PROVES, AND WHAT IT DOES NOT
# --------------------------------------
# bootstrap.sh is a shell script whose every side effect is an `mc` invocation
# against a live MinIO. There is no docker daemon and no MinIO in the gate
# environment, so this harness puts a STUB `mc` first on PATH and runs the real,
# unmodified bootstrap.sh against it.
#
#   PROVES  — bootstrap.sh's OWN logic: that it validates tenant/module names
#             before it provisions anything, that it refuses an invalid tenant
#             with a non-zero exit, that a second run neither fails nor
#             double-creates, that it passes --ignore-existing to `mc mb`, and
#             that it tolerates the "already exists" failure `mc admin user
#             svcacct add` returns on a re-run (that command is genuinely not
#             idempotent — bootstrap.sh line 61 says so).
#
#   DOES NOT PROVE — anything about MinIO or about real `mc`. The stub models
#             mc's contract as bootstrap.sh assumes it (mb --ignore-existing is
#             idempotent; policy create overwrites; svcacct add fails on a
#             duplicate access key). If real mc's behaviour differs from that
#             model, this harness is green and production is still wrong. Only
#             deploy/smoke.sh against a running stack covers that, and it needs
#             Docker + a live MinIO.
#
# Usage:  deploy/bootstrap_test.sh          (exit 0 = all cases passed)

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP="${HERE}/bootstrap.sh"
[[ -f "${BOOTSTRAP}" ]] || { echo "cannot find ${BOOTSTRAP}" >&2; exit 1; }

PASSED=0
FAILED=0
ok()   { PASSED=$((PASSED + 1)); printf '    ok   %s\n' "$*"; }
bad()  { FAILED=$((FAILED + 1)); printf '    FAIL %s\n' "$*"; }
case_() { printf '\n== %s\n' "$*"; }

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# ---- stub mc ---------------------------------------------------------------
# Models exactly the three mc behaviours bootstrap.sh depends on, and records
# every invocation so the assertions can inspect what was actually called.
BIN="${WORK}/bin"
mkdir -p "${BIN}"
cat > "${BIN}/mc" <<'STUB'
#!/bin/sh
# Stub `mc`. State lives in $MC_STATE; every call is appended to $MC_LOG.
set -eu
printf 'mc %s\n' "$*" >> "$MC_LOG"
cmd="$1"; shift
case "$cmd" in
  alias)
    # `alias set gr <endpoint> <user> <pass>` — always succeeds.
    exit 0 ;;
  mb)
    ignore=0; bucket=""
    for a in "$@"; do
      case "$a" in
        --ignore-existing) ignore=1 ;;
        gr/*) bucket="${a#gr/}" ;;
      esac
    done
    if [ -z "$bucket" ]; then echo "mc: no bucket argument" >&2; exit 1; fi
    if [ -e "$MC_STATE/buckets/$bucket" ]; then
      # Real mc errors on an existing bucket unless --ignore-existing is passed.
      if [ "$ignore" -eq 1 ]; then exit 0; fi
      echo "mc: <ERROR> Unable to make bucket \`$bucket\`: you already own it." >&2
      exit 1
    fi
    : > "$MC_STATE/buckets/$bucket"
    exit 0 ;;
  admin)
    sub="$1"; shift
    case "$sub" in
      policy)
        # `policy create gr <name> <file>` — real mc overwrites, so idempotent.
        exit 0 ;;
      user)
        # `user svcacct add gr <root> --access-key K --secret-key S --policy P`
        akey=""; want=""
        for a in "$@"; do
          if [ "$want" = "akey" ]; then akey="$a"; want=""; continue; fi
          case "$a" in --access-key) want="akey" ;; esac
        done
        if [ -z "$akey" ]; then echo "mc: missing --access-key" >&2; exit 1; fi
        if [ -e "$MC_STATE/svcacct/$akey" ]; then
          # svcacct add is NOT idempotent in real mc — this is the failure
          # bootstrap.sh must swallow on a re-run.
          echo "mc: <ERROR> service account $akey already exists" >&2
          exit 1
        fi
        : > "$MC_STATE/svcacct/$akey"
        exit 0 ;;
    esac
    exit 0 ;;
esac
exit 0
STUB
chmod +x "${BIN}/mc"

# run_bootstrap <state-dir> <log-file> <out-file> — runs the REAL bootstrap.sh
# with the stub mc first on PATH. Echoes the exit code.
run_bootstrap() {
  local state="$1" log="$2" out="$3"
  mkdir -p "${state}/buckets" "${state}/svcacct"
  : > "${log}"
  env -i \
    PATH="${BIN}:/usr/bin:/bin" \
    HOME="${WORK}" \
    MC_STATE="${state}" \
    MC_LOG="${log}" \
    MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}" \
    MINIO_ROOT_USER="${MINIO_ROOT_USER:-root}" \
    MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-rootpassword}" \
    MCP_MINIO_ACCESS_KEY="${MCP_MINIO_ACCESS_KEY:-mcpkey}" \
    MCP_MINIO_SECRET_KEY="${MCP_MINIO_SECRET_KEY:-mcpsecret}" \
    BOOTSTRAP_TENANTS="${BOOTSTRAP_TENANTS}" \
    BOOTSTRAP_MODULES="${BOOTSTRAP_MODULES}" \
    S3_KEY_ORDERS="${S3_KEY_ORDERS:-}" \
    S3_SECRET_ORDERS="${S3_SECRET_ORDERS:-}" \
    sh "${BOOTSTRAP}" >"${out}" 2>&1
  echo $?
}

# state_fingerprint <state-dir> — the provisioned world, as a sorted list.
# A plain glob rather than `find -printf`: -printf is a GNU extension and the
# gate image may well be busybox.
state_fingerprint() {
  local f
  { for f in "$1"/buckets/*; do [[ -e "${f}" ]] && printf 'bucket %s\n' "${f##*/}"; done
    for f in "$1"/svcacct/*; do [[ -e "${f}" ]] && printf 'svcacct %s\n' "${f##*/}"; done
  } | LC_ALL=C sort
}

count_calls() { grep -c -- "$2" "$1" 2>/dev/null || true; }

# ============================================================================
# CASE 1 — run it twice. Idempotent: the second run must not fail and must not
# double-create. This is the claim in bootstrap.sh's own header ("Safe to
# re-run: every step tolerates 'already exists'"), previously untested.
# ============================================================================
case_ "CASE 1: running bootstrap.sh twice is idempotent"
export BOOTSTRAP_TENANTS="acme acme-hr"
export BOOTSTRAP_MODULES="orders payroll"
export S3_KEY_ORDERS="orderskey" S3_SECRET_ORDERS="orderssecret"

STATE="${WORK}/state1"
rc1="$(run_bootstrap "${STATE}" "${WORK}/log1" "${WORK}/out1")"
fp1="$(state_fingerprint "${STATE}")"

if [[ "${rc1}" == "0" ]]; then
  ok "first run exits 0"
else
  bad "first run exited ${rc1}, want 0 ($(tail -n 3 "${WORK}/out1"))"
fi

# The four t-<tenant>-<module> buckets, and NOT the ambiguous collision name.
want_buckets="bucket t-acme-hr-orders
bucket t-acme-hr-payroll
bucket t-acme-orders
bucket t-acme-payroll"
got_buckets="$(printf '%s\n' "${fp1}" | grep '^bucket ' | LC_ALL=C sort)"
if [[ "${got_buckets}" == "${want_buckets}" ]]; then
  ok "first run created exactly the 4 t-<tenant>-<module> buckets"
else
  bad "buckets after run 1 = [${got_buckets}], want [${want_buckets}]"
fi

# --ignore-existing is what makes bucket creation re-runnable at all.
mb_total="$(count_calls "${WORK}/log1" '^mc mb ')"
mb_ignore="$(count_calls "${WORK}/log1" '^mc mb --ignore-existing ')"
if [[ "${mb_total}" == "4" && "${mb_ignore}" == "4" ]]; then
  ok "all 4 mb calls passed --ignore-existing"
else
  bad "mb calls = ${mb_total}, of which --ignore-existing = ${mb_ignore}, want 4/4"
fi

if grep -q 'svcacct mcpkey scoped\|mcpkey' "${WORK}/log1"; then
  ok "first run created the MCP service account"
else
  bad "first run never created the MCP service account"
fi

# --- second run, same state -------------------------------------------------
rc2="$(run_bootstrap "${STATE}" "${WORK}/log2" "${WORK}/out2")"
fp2="$(state_fingerprint "${STATE}")"

if [[ "${rc2}" == "0" ]]; then
  ok "second run exits 0 (idempotent)"
else
  bad "second run exited ${rc2}, want 0 ($(tail -n 5 "${WORK}/out2"))"
fi

if [[ "${fp1}" == "${fp2}" ]]; then
  ok "second run left the provisioned state byte-identical (no double-create)"
else
  bad "state changed on re-run:"$'\n'"$(diff <(echo "${fp1}") <(echo "${fp2}") || true)"
fi

# The re-run must actually have HIT the not-idempotent commands and swallowed
# their failure — otherwise "idempotent" would be vacuously true.
if grep -q 'mcp service account already exists' "${WORK}/out2"; then
  ok "second run hit the non-idempotent svcacct add and tolerated its failure"
else
  bad "second run never exercised the svcacct 'already exists' path (out2: $(tail -n 5 "${WORK}/out2"))"
fi
if grep -q 'orders: svcacct already exists' "${WORK}/out2"; then
  ok "second run tolerated the per-module svcacct already existing"
else
  bad "second run did not report the per-module svcacct as already existing"
fi

# ============================================================================
# CASE 2 — an invalid tenant must be REFUSED, not provisioned around.
# bootstrap.sh validates before it creates; the assertion is both the exit code
# and that nothing was provisioned.
# ============================================================================
case_ "CASE 2: an invalid tenant is refused before anything is provisioned"
export BOOTSTRAP_MODULES="orders"
export S3_KEY_ORDERS="" S3_SECRET_ORDERS=""
# NOTE on what cannot be tested here: BOOTSTRAP_TENANTS is a SPACE-SEPARATED
# list, so a tenant id containing a space is not expressible through this
# interface at all — "good acme hr" is three tenants, not two. That is the list
# format, not a hole in the validator; the sidecar rejects a spaced X-Tenant-Id
# on its own path (mcp/tools.go checkTenant, TestAmbiguousTenantIdsAreRejected-
# NotMerged). Asserting a refusal here would be asserting a wish.
for bad_tenant in "acme_hr" "-acme" "acme-" "ACME" "acme.hr" "acme/x" "acme@hr" "acme:hr"; do
  export BOOTSTRAP_TENANTS="good ${bad_tenant}"
  st="${WORK}/state-bad"
  rm -rf "${st}"
  rc="$(run_bootstrap "${st}" "${WORK}/log-bad" "${WORK}/out-bad")"
  provisioned="$(state_fingerprint "${st}")"
  mb_calls="$(count_calls "${WORK}/log-bad" '^mc mb ')"
  sa_calls="$(count_calls "${WORK}/log-bad" 'svcacct add')"
  if [[ "${rc}" != "2" ]]; then
    bad "tenant '${bad_tenant}' -> exit ${rc}, want 2 (refuse)"
  elif [[ -n "${provisioned}" ]]; then
    bad "tenant '${bad_tenant}' refused but still provisioned: ${provisioned}"
  elif [[ "${mb_calls}" != "0" || "${sa_calls}" != "0" ]]; then
    bad "tenant '${bad_tenant}' refused but issued ${mb_calls} mb / ${sa_calls} svcacct calls"
  else
    ok "tenant '${bad_tenant}' refused (exit 2, zero mb/svcacct calls)"
  fi
done

# ============================================================================
# CASE 3 — a DASHED module is the cross-tenant collision this repo already
# fixed in the sidecar (mcp/tools.go bucketFor): tenant "acme" + module
# "hr-payroll" and tenant "acme-hr" + module "payroll" name the same bucket.
# Provisioning must refuse it for the same reason the sidecar does, or it
# creates a bucket the sidecar can never safely address.
# ============================================================================
case_ "CASE 3: a dashed module is refused (t-<tenant>-<module> would be ambiguous)"
export BOOTSTRAP_TENANTS="acme"
export S3_KEY_ORDERS="" S3_SECRET_ORDERS=""
for bad_module in "hr-payroll" "Orders" "orders_v2" ""; do
  export BOOTSTRAP_MODULES="${bad_module}"
  st="${WORK}/state-mod"
  rm -rf "${st}"
  rc="$(run_bootstrap "${st}" "${WORK}/log-mod" "${WORK}/out-mod")"
  mb_calls="$(count_calls "${WORK}/log-mod" '^mc mb ')"
  if [[ -z "${bad_module}" ]]; then
    # An empty BOOTSTRAP_MODULES word-splits to zero iterations: nothing to
    # validate and nothing to create. Not a refusal — assert the real behaviour
    # rather than a wished-for one.
    if [[ "${rc}" == "0" && "${mb_calls}" == "0" ]]; then
      ok "empty BOOTSTRAP_MODULES provisions no buckets (exit 0, 0 mb calls)"
    else
      bad "empty BOOTSTRAP_MODULES -> exit ${rc}, ${mb_calls} mb calls; want 0/0"
    fi
    continue
  fi
  if [[ "${rc}" == "2" && "${mb_calls}" == "0" ]]; then
    ok "module '${bad_module}' refused (exit 2, zero mb calls)"
  else
    bad "module '${bad_module}' -> exit ${rc}, ${mb_calls} mb calls; want exit 2 and 0 calls"
  fi
done

# The legitimate dashed-TENANT case must still work — the refusal must not have
# been bought by banning real tenant slugs like gridiron-robotics.
export BOOTSTRAP_TENANTS="gridiron-robotics" BOOTSTRAP_MODULES="orders"
st="${WORK}/state-ok"
rm -rf "${st}"
rc="$(run_bootstrap "${st}" "${WORK}/log-ok" "${WORK}/out-ok")"
if [[ "${rc}" == "0" ]] && [[ -e "${st}/buckets/t-gridiron-robotics-orders" ]]; then
  ok "a legitimately dashed tenant still provisions t-gridiron-robotics-orders"
else
  bad "dashed tenant gridiron-robotics -> exit ${rc}, buckets: $(state_fingerprint "${st}")"
fi

printf '\nbootstrap.sh behavioural: %d passed, %d failed\n' "${PASSED}" "${FAILED}"
(( FAILED == 0 )) || exit 1
exit 0
