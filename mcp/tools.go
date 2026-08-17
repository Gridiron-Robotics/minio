package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// handlerErr carries an HTTP status + message back to the Contract A envelope.
type handlerErr struct {
	status int
	msg    string
}

func errf(status int, format string, a ...any) *handlerErr {
	return &handlerErr{status: status, msg: fmt.Sprintf(format, a...)}
}

type toolHandler func(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr)

type toolDef struct {
	Name        string
	Description string
	Destructive bool
	InputSchema map[string]any
	Handler     toolHandler
}

// Bucket names are "t-<tenant>-<module>". That composition is only injective —
// i.e. only actually isolating — if at most ONE of the two segments may contain
// the "-" separator. Tenant ids in this estate are slugs that DO contain dashes
// (gridiron-robotics), so the MODULE segment is the one that must not:
//
//	tenant "acme"    + module "hr-payroll" -> t-acme-hr-payroll
//	tenant "acme-hr" + module "payroll"    -> t-acme-hr-payroll   <- same bucket
//
// With a dash-free module the last segment is unambiguously the module and the
// middle is unambiguously the tenant, so no (tenant, module) pair a caller can
// ask for lands on another tenant's bucket.
var (
	// tenantRe: lowercase alnum + dashes, no leading/trailing dash (S3 rules).
	tenantRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	// moduleRe: lowercase alnum only — deliberately NO dash (see above).
	moduleRe = regexp.MustCompile(`^[a-z0-9]+$`)
)

const (
	maxSegmentLen = 40 // per segment
	maxBucketLen  = 63 // S3/MinIO hard limit for a bucket name
)

// normalizeSegment lower-cases and trims a segment. It deliberately does NOT
// rewrite invalid characters: mangling maps distinct identities onto ONE bucket
// ("acme_hr" and "acme-hr" both became "acme-hr"), silently merging two tenants'
// namespaces. Anything that does not normalize to a valid segment is rejected by
// bucketFor instead.
func normalizeSegment(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// checkTenant validates the authoritative X-Tenant-Id value and returns its
// normalized form. Every bucket-touching path goes through it, so an unusable
// tenant id is a loud 422 rather than a quietly-rewritten namespace.
func checkTenant(tenant string) (string, *handlerErr) {
	t := normalizeSegment(tenant)
	if t == "" || !tenantRe.MatchString(t) {
		return "", errf(422, "invalid tenant %q: expected lowercase letters, digits and dashes (no leading/trailing dash)", tenant)
	}
	if len(t) > maxSegmentLen {
		// Truncating instead would map two long tenant ids sharing a prefix onto
		// one bucket — the same silent namespace merge as mangling.
		return "", errf(422, "tenant is longer than %d characters", maxSegmentLen)
	}
	return t, nil
}

// bucketFor is the ONLY way a bucket name is derived. Tenant isolation is
// structural: a caller supplies a `module`, never a raw bucket, so it can only
// ever touch `t-<tenant>-<module>` — and because the module may not contain the
// "-" separator, that name can belong to no other tenant.
func bucketFor(tenant, module string) (string, *handlerErr) {
	t, herr := checkTenant(tenant)
	if herr != nil {
		return "", herr
	}
	m := normalizeSegment(module)
	if m == "" || !moduleRe.MatchString(m) {
		return "", errf(422, "invalid module %q: expected lowercase letters and digits only (no dashes — the dash separates tenant from module in t-<tenant>-<module>)", module)
	}
	if len(m) > maxSegmentLen {
		return "", errf(422, "module is longer than %d characters", maxSegmentLen)
	}
	bucket := fmt.Sprintf("t-%s-%s", t, m)
	if len(bucket) > maxBucketLen {
		// Caught here as a 422 caller error; otherwise MinIO rejects the name and
		// the 502 pages the self-heal rail for what is a bad argument.
		return "", errf(422, "bucket name t-<tenant>-<module> would exceed %d characters", maxBucketLen)
	}
	return bucket, nil
}

// tenantPrefix is the bucket-name prefix that scopes list_buckets to one tenant.
func tenantPrefix(tenant string) string { return "t-" + normalizeSegment(tenant) + "-" }

// moduleOfBucket returns the module segment of a bucket that belongs to this
// tenant, and false when the bucket is NOT this tenant's. A plain prefix test is
// not enough: "t-acme-" also prefixes tenant "acme-hr"'s bucket
// "t-acme-hr-payroll", which would leak a sibling tenant's bucket names into
// list_buckets. A module never contains "-", so a remainder that does belongs to
// a longer tenant id.
func moduleOfBucket(tenant, bucket string) (string, bool) {
	prefix := tenantPrefix(tenant)
	if !strings.HasPrefix(bucket, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(bucket, prefix)
	if rest == "" || strings.Contains(rest, "-") {
		return "", false
	}
	return rest, true
}

// objectKey validates the caller-supplied object key. MinIO itself routes with
// SkipClean(true) (cmd/routers.go), so "../" in a key is a literal key there and
// not a bucket escape — but the sidecar also hands keys to *presigned URLs* that
// browsers and proxies normalize before sending. Rejecting traversal segments
// keeps the request that is signed and the request that is sent identical.
func objectKey(args map[string]any) (string, *handlerErr) {
	key := strArg(args, "key")
	if key == "" {
		return "", errf(422, "key must be a non-empty string")
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, `\`) {
		return "", errf(422, "key must be a relative object key (no leading '/' or backslash)")
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "." || seg == ".." || seg == "" {
			return "", errf(422, "key must not contain empty, '.' or '..' path segments")
		}
	}
	return key, nil
}

func buildTools() []toolDef {
	obj := func(props map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "properties": props, "required": required}
	}
	str := map[string]any{"type": "string"}
	return []toolDef{
		{
			Name:        "list_buckets",
			Description: "List this tenant's object-storage buckets (each is a t-<tenant>-<module> namespace). Read-only.",
			InputSchema: obj(map[string]any{}),
			Handler:     hListBuckets,
		},
		{
			Name:        "ensure_bucket",
			Description: "Idempotently create this tenant's bucket for the given module (t-<tenant>-<module>) if it does not exist. `module` is lowercase letters/digits only — no dashes. Returns the bucket name.",
			Destructive: true,
			InputSchema: obj(map[string]any{"module": str}, "module"),
			Handler:     hEnsureBucket,
		},
		{
			Name:        "list_objects",
			Description: "List objects under a key prefix in the tenant's module bucket. Read-only. Args: module, prefix (optional), recursive (optional bool), max (optional int, default 1000).",
			InputSchema: obj(map[string]any{
				"module": str, "prefix": str,
				"recursive": map[string]any{"type": "boolean"},
				"max":       map[string]any{"type": "integer"},
			}, "module"),
			Handler: hListObjects,
		},
		{
			Name:        "stat_object",
			Description: "Return metadata (size, etag, content-type, last-modified) for one object. Read-only. Args: module, key.",
			InputSchema: obj(map[string]any{"module": str, "key": str}, "module", "key"),
			Handler:     hStatObject,
		},
		{
			Name:        "presign_get",
			Description: "Mint a short-lived presigned GET URL to download one object without credentials. Read-only. Args: module, key, expiry_seconds (optional).",
			InputSchema: obj(map[string]any{"module": str, "key": str,
				"expiry_seconds": map[string]any{"type": "integer"}}, "module", "key"),
			Handler: hPresignGet,
		},
		{
			Name:        "presign_put",
			Description: "Mint a short-lived presigned PUT URL so a client can upload one object directly. Grants write capability — destructive. Args: module, key, expiry_seconds (optional).",
			Destructive: true,
			InputSchema: obj(map[string]any{"module": str, "key": str,
				"expiry_seconds": map[string]any{"type": "integer"}}, "module", "key"),
			Handler: hPresignPut,
		},
		{
			Name:        "put_object",
			Description: "Upload an object from inline base64 content. Destructive (overwrites). Args: module, key, content_base64, content_type (optional).",
			Destructive: true,
			InputSchema: obj(map[string]any{"module": str, "key": str,
				"content_base64": str, "content_type": str}, "module", "key", "content_base64"),
			Handler: hPutObject,
		},
		{
			Name:        "delete_object",
			Description: "Delete one object from the tenant's module bucket. Destructive. Args: module, key.",
			Destructive: true,
			InputSchema: obj(map[string]any{"module": str, "key": str}, "module", "key"),
			Handler:     hDeleteObject,
		},
	}
}

// ---- handlers --------------------------------------------------------------

func hListBuckets(ctx context.Context, s *server, tenant string, _ map[string]any) (any, *handlerErr) {
	if _, herr := checkTenant(tenant); herr != nil {
		return nil, herr
	}
	all, err := s.minio.ListBuckets(ctx)
	if err != nil {
		return nil, errf(502, "list buckets: %v", err)
	}
	out := []map[string]any{}
	for _, b := range all {
		module, mine := moduleOfBucket(tenant, b.Name)
		if !mine {
			continue
		}
		out = append(out, map[string]any{
			"bucket":  b.Name,
			"module":  module,
			"created": b.CreationDate.Format(time.RFC3339),
		})
	}
	return map[string]any{"buckets": out}, nil
}

func hEnsureBucket(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	exists, err := s.minio.BucketExists(ctx, bucket)
	if err != nil {
		return nil, errf(502, "bucket exists check: %v", err)
	}
	created := false
	if !exists {
		if err := s.minio.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, errf(502, "make bucket: %v", err)
		}
		created = true
	}
	return map[string]any{"bucket": bucket, "created": created}, nil
}

func hListObjects(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	max := intArg(args, "max", 1000)
	opts := minio.ListObjectsOptions{
		Prefix:    strArg(args, "prefix"),
		Recursive: boolArg(args, "recursive", false),
	}
	out := []map[string]any{}
	for info := range s.minio.ListObjects(ctx, bucket, opts) {
		if info.Err != nil {
			return nil, errf(502, "list objects: %v", info.Err)
		}
		out = append(out, map[string]any{
			"key":           info.Key,
			"size":          info.Size,
			"etag":          info.ETag,
			"last_modified": info.LastModified.Format(time.RFC3339),
		})
		if len(out) >= max {
			break
		}
	}
	return map[string]any{"bucket": bucket, "objects": out, "count": len(out)}, nil
}

func hStatObject(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	key, herr := objectKey(args)
	if herr != nil {
		return nil, herr
	}
	info, err := s.minio.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		// A blanket 404 hid every storage outage behind "not found": stat is the
		// most-called read path, and a 404 never reaches the >=500 self-heal rail,
		// so a dead store paged nobody. Only a real "no such key" is a 404.
		if resp := minio.ToErrorResponse(err); resp.StatusCode == 404 {
			return nil, errf(404, "stat object: %v", err)
		}
		return nil, errf(502, "stat object: %v", err)
	}
	return map[string]any{
		"bucket": bucket, "key": info.Key, "size": info.Size, "etag": info.ETag,
		"content_type": info.ContentType, "last_modified": info.LastModified.Format(time.RFC3339),
	}, nil
}

func (s *server) presignExpiry(args map[string]any) time.Duration {
	if secs := intArg(args, "expiry_seconds", 0); secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return s.cfg.presignExpiry
}

func hPresignGet(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	key, herr := objectKey(args)
	if herr != nil {
		return nil, herr
	}
	u, err := s.minio.PresignedGetObject(ctx, bucket, key, s.presignExpiry(args), url.Values{})
	if err != nil {
		return nil, errf(502, "presign get: %v", err)
	}
	return map[string]any{"url": u.String(), "method": "GET", "expires_in": int(s.presignExpiry(args).Seconds())}, nil
}

func hPresignPut(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	key, herr := objectKey(args)
	if herr != nil {
		return nil, herr
	}
	u, err := s.minio.PresignedPutObject(ctx, bucket, key, s.presignExpiry(args))
	if err != nil {
		return nil, errf(502, "presign put: %v", err)
	}
	return map[string]any{"url": u.String(), "method": "PUT", "expires_in": int(s.presignExpiry(args).Seconds())}, nil
}

func hPutObject(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	key, herr := objectKey(args)
	if herr != nil {
		return nil, herr
	}
	raw, err := base64.StdEncoding.DecodeString(strArg(args, "content_base64"))
	if err != nil {
		return nil, errf(422, "content_base64 is not valid base64: %v", err)
	}
	ct := strArg(args, "content_type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	info, err := s.minio.PutObject(ctx, bucket, key, bytes.NewReader(raw), int64(len(raw)),
		minio.PutObjectOptions{ContentType: ct})
	if err != nil {
		return nil, errf(502, "put object: %v", err)
	}
	return map[string]any{"bucket": bucket, "key": key, "size": info.Size, "etag": info.ETag}, nil
}

func hDeleteObject(ctx context.Context, s *server, tenant string, args map[string]any) (any, *handlerErr) {
	bucket, herr := bucketFor(tenant, strArg(args, "module"))
	if herr != nil {
		return nil, herr
	}
	key, herr := objectKey(args)
	if herr != nil {
		return nil, herr
	}
	if err := s.minio.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return nil, errf(502, "delete object: %v", err)
	}
	return map[string]any{"bucket": bucket, "key": key, "deleted": true}, nil
}

// ---- schema + arg helpers --------------------------------------------------

// missingRequired returns the required schema keys absent from args.
func missingRequired(schema, args map[string]any) []string {
	req, _ := schema["required"].([]string)
	var out []string
	for _, k := range req {
		if _, ok := args[k]; !ok {
			out = append(out, "missing required argument: "+k)
		}
	}
	return out
}

func strArg(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	return ""
}

func intArg(args map[string]any, key string, def int) int {
	switch v := args[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return def
}

func boolArg(args map[string]any, key string, def bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return def
}
