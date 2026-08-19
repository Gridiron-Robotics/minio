#!/bin/sh
# minio bootstrap — idempotent. Runs once after MinIO is healthy.
#  1. registers the root alias
#  2. pre-creates t-<tenant>-<module> buckets for the configured tenants/modules
#  3. creates the MCP sidecar service account with a policy scoped to t-* buckets
#  4. (extend) per-module service accounts with tighter policies
# Safe to re-run: every step tolerates "already exists".
set -eu

mc alias set gr "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Bucket names are t-<tenant>-<module>. That is only unambiguous while at most ONE
# of the two segments may contain the "-" separator: with a dashed module,
# tenant "acme" + module "hr-payroll" and tenant "acme-hr" + module "payroll" name
# the SAME bucket, and the per-module policy below (t-*-<module>) matches across
# tenants the same way. Tenant slugs legitimately carry dashes, so the module is
# the segment that may not. The MCP sidecar enforces the identical rule
# (mcp/tools.go bucketFor); refuse here rather than provision a bucket the sidecar
# can never safely address.
echo "==> validating tenant/module names"
for t in $BOOTSTRAP_TENANTS; do
  case "$t" in
    *[!a-z0-9-]* | -* | *- | "")
      echo "FATAL: tenant '$t' must be lowercase letters/digits/dashes with no leading or trailing dash" >&2
      exit 2 ;;
  esac
done
for m in $BOOTSTRAP_MODULES; do
  case "$m" in
    *[!a-z0-9]* | "")
      echo "FATAL: module '$m' must be lowercase letters and digits only — a dash in the module makes t-<tenant>-<module> ambiguous across tenants" >&2
      exit 2 ;;
  esac
done

echo "==> buckets"
for t in $BOOTSTRAP_TENANTS; do
  for m in $BOOTSTRAP_MODULES; do
    b="t-${t}-${m}"
    mc mb --ignore-existing "gr/${b}"
    # default: private; encryption-at-rest auto-applies when SSE-KMS is on.
    echo "    ${b}"
  done
done

echo "==> MCP service-account policy (scoped to all tenant buckets t-*)"
cat >/tmp/mcp-policy.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::t-*", "arn:aws:s3:::t-*/*"] }
  ]
}
JSON
mc admin policy create gr gridiron-storage /tmp/mcp-policy.json 2>/dev/null || \
  mc admin policy create gr gridiron-storage /tmp/mcp-policy.json || true

echo "==> MCP service account"
# svcacct add is not idempotent; ignore "already exists".
mc admin user svcacct add gr "$MINIO_ROOT_USER" \
  --access-key "$MCP_MINIO_ACCESS_KEY" \
  --secret-key "$MCP_MINIO_SECRET_KEY" \
  --policy /tmp/mcp-policy.json 2>/dev/null || \
  echo "    (mcp service account already exists — leaving as-is)"

# 4. Per-module service accounts — how a module talks S3 DIRECTLY to the store
#    (not via the MCP sidecar). Each gets keys scoped to ONLY its own buckets
#    across every tenant (t-*-<module>), so a compromised module can never read
#    another module's blobs. Keys are supplied per module via env
#    S3_KEY_<MODULE> / S3_SECRET_<MODULE> (module upper-cased, '-'→'_'), so they
#    live in Infisical, not the repo. Modules without a dedicated pair are simply
#    skipped (their consumer can fall back to the broad gridiron-storage account).
echo "==> per-module service accounts (scoped to t-*-<module>)"
for m in $BOOTSTRAP_MODULES; do
  mu="$(printf '%s' "$m" | tr '[:lower:]-' '[:upper:]_')"
  akey="$(printenv "S3_KEY_${mu}" 2>/dev/null || true)"
  skey="$(printenv "S3_SECRET_${mu}" 2>/dev/null || true)"
  if [ -z "$akey" ] || [ -z "$skey" ]; then
    echo "    ${m}: no S3_KEY_${mu}/S3_SECRET_${mu} — skipped (uses shared gridiron-storage)"
    continue
  fi
  pol="/tmp/mod-${m}-policy.json"
  cat >"$pol" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::t-*-${m}", "arn:aws:s3:::t-*-${m}/*"] }
  ]
}
JSON
  mc admin policy create gr "gridiron-storage-${m}" "$pol" 2>/dev/null || true
  if mc admin user svcacct add gr "$MINIO_ROOT_USER" \
       --access-key "$akey" --secret-key "$skey" --policy "$pol" 2>/dev/null; then
    echo "    ${m}: svcacct ${akey} scoped to t-*-${m}"
  else
    echo "    ${m}: svcacct already exists — leaving as-is"
  fi
done

echo "bootstrap complete."
