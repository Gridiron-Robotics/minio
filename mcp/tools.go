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

// segRe validates a tenant/module segment used to build a bucket name.
var segRe = regexp.MustCompile(`[^a-z0-9-]`)

// sanitizeSegment lower-cases and strips anything not valid in an S3 bucket
// segment, so a hostile tenant/module value can't inject a path or a foreign
// bucket name.
func sanitizeSegment(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = segRe.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

// bucketFor is the ONLY way a bucket name is derived. Tenant isolation is
// structural: a caller supplies a `module`, never a raw bucket, so it can only
// ever touch `t-<tenant>-<module>`.
func bucketFor(tenant, module string) (string, *handlerErr) {
	t := sanitizeSegment(tenant)
	m := sanitizeSegment(module)
	if t == "" || m == "" {
		return "", errf(422, "tenant and module must be non-empty alphanumeric")
	}
	return fmt.Sprintf("t-%s-%s", t, m), nil
}

// tenantPrefix is the bucket-name prefix that scopes list_buckets to one tenant.
func tenantPrefix(tenant string) string { return "t-" + sanitizeSegment(tenant) + "-" }

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
			Description: "Idempotently create this tenant's bucket for the given module (t-<tenant>-<module>) if it does not exist. Returns the bucket name.",
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
	all, err := s.minio.ListBuckets(ctx)
	if err != nil {
		return nil, errf(502, "list buckets: %v", err)
	}
	prefix := tenantPrefix(tenant)
	out := []map[string]any{}
	for _, b := range all {
		if strings.HasPrefix(b.Name, prefix) {
			out = append(out, map[string]any{
				"bucket":  b.Name,
				"module":  strings.TrimPrefix(b.Name, prefix),
				"created": b.CreationDate.Format(time.RFC3339),
			})
		}
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
	key := strArg(args, "key")
	info, err := s.minio.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, errf(404, "stat object: %v", err)
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
	u, err := s.minio.PresignedGetObject(ctx, bucket, strArg(args, "key"), s.presignExpiry(args), url.Values{})
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
	u, err := s.minio.PresignedPutObject(ctx, bucket, strArg(args, "key"), s.presignExpiry(args))
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
	raw, err := base64.StdEncoding.DecodeString(strArg(args, "content_base64"))
	if err != nil {
		return nil, errf(422, "content_base64 is not valid base64: %v", err)
	}
	ct := strArg(args, "content_type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	key := strArg(args, "key")
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
	key := strArg(args, "key")
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
