#!/bin/sh
# minio bootstrap — idempotent. Runs once after MinIO is healthy.
#  1. registers the root alias
#  2. pre-creates t-<tenant>-<module> buckets for the configured tenants/modules
#  3. creates the MCP sidecar service account with a policy scoped to t-* buckets
#  4. (extend) per-module service accounts with tighter policies
# Safe to re-run: every step tolerates "already exists".
set -eu

mc alias set gr "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

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

echo "bootstrap complete."
