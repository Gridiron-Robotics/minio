package main

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// s3Stub builds a real *server whose minio.Client points at an httptest server,
// so the object handlers in tools.go run for real against a canned S3 backend —
// no production code changes and no live MinIO. The handler under test proves the
// on-the-wire bucket/key (the structural tenant-isolation claim) that bucketFor's
// unit test alone cannot.
func s3Stub(t *testing.T, h http.HandlerFunc) *server {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	cfg := config{
		s3Endpoint:  strings.TrimPrefix(ts.URL, "http://"), // host:port, no scheme
		s3AccessKey: "k", s3SecretKey: "s", s3Secure: false,
		authSet: true, authDigest: sha256.Sum256([]byte("secret")),
		presignExpiry: 15 * time.Minute,
	}
	s, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	return s
}

// answerLocation replies to minio-go's bucket-region lookup (GET /<bucket>?location=).
// minio-go resolves a bucket's region before signing any request, so every stub
// must serve this first or the operation under test fails with an opaque signing
// error instead of the behaviour we want to assert. Returns true if it handled r.
func answerLocation(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		return true
	}
	return false
}

// invokeResult runs a POST /invoke against the stub-backed server and returns the
// decoded {"tool","result"} envelope's result map (nil on non-2xx).
func invokeResult(t *testing.T, s *server, tenant, body string) (int, map[string]any) {
	t.Helper()
	rec := doH(s, http.MethodPost, "/invoke", "secret",
		map[string]string{"X-Tenant-Id": tenant}, body)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	var out struct {
		Tool   string         `json:"tool"`
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode result: %v (body=%s)", err, rec.Body.String())
	}
	return rec.Code, out.Result
}

// ---- put_object ------------------------------------------------------------

func TestPutObjectWritesToTenantBucket(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotCT string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		gotMethod, gotPath = r.Method, r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("ETag", `"deadbeef"`)
		w.WriteHeader(http.StatusOK)
	})

	code, res := invokeResult(t, s, "acme",
		`{"tool":"put_object","arguments":{"module":"orders","key":"a/b.txt","content_base64":"aGk="}}`)
	if code != http.StatusOK {
		t.Fatalf("put_object -> %d, want 200", code)
	}
	if gotMethod != http.MethodPut || gotPath != "/t-acme-orders/a/b.txt" {
		t.Fatalf("wire = %s %s, want PUT /t-acme-orders/a/b.txt", gotMethod, gotPath)
	}
	// minio-go streams the payload with aws-chunked signature framing over http,
	// so the raw body wraps "hi" between chunk-signature lines rather than being
	// the bare bytes. Its presence in the framing is the on-the-wire proof.
	if !strings.Contains(gotBody, "\r\nhi\r\n") {
		t.Fatalf("wire body = %q, want the \"hi\" payload chunk", gotBody)
	}
	if gotCT != "application/octet-stream" {
		t.Fatalf("default content-type = %q, want application/octet-stream", gotCT)
	}
	if res["bucket"] != "t-acme-orders" || res["key"] != "a/b.txt" || res["etag"] != "deadbeef" {
		t.Fatalf("result = %v, want bucket/key/etag set", res)
	}
}

func TestPutObjectRejectsBadBase64(t *testing.T) {
	hits := 0
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		hits++
		w.WriteHeader(http.StatusOK)
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"put_object","arguments":{"module":"orders","key":"a/b.txt","content_base64":"!!!"}}`)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("bad base64 -> %d, want 422", code)
	}
	if hits != 0 {
		t.Fatalf("bad base64 reached S3 %d times, want 0", hits)
	}
}

func TestPutObjectHonorsContentType(t *testing.T) {
	var gotCT string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		gotCT = r.Header.Get("Content-Type")
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"put_object","arguments":{"module":"orders","key":"k.json","content_base64":"e30=","content_type":"application/json"}}`)
	if code != http.StatusOK {
		t.Fatalf("put_object -> %d, want 200", code)
	}
	if gotCT != "application/json" {
		t.Fatalf("content-type = %q, want application/json", gotCT)
	}
}

// ---- delete_object ---------------------------------------------------------

func TestDeleteObjectHitsTenantBucket(t *testing.T) {
	var gotMethod, gotPath string
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"delete_object","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusOK {
		t.Fatalf("delete_object -> %d, want 200", code)
	}
	if gotMethod != http.MethodDelete || gotPath != "/t-acme-orders/a/b.txt" {
		t.Fatalf("wire = %s %s, want DELETE /t-acme-orders/a/b.txt", gotMethod, gotPath)
	}
	if res["deleted"] != true || res["bucket"] != "t-acme-orders" || res["key"] != "a/b.txt" {
		t.Fatalf("result = %v, want {bucket,key,deleted:true}", res)
	}
}

func TestDeleteObjectMapsFailureTo502(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		// 403 (not a retryable 5xx) exercises the same err -> 502 mapping without
		// minio-go's 5xx retry/backoff slowing the test down.
		w.WriteHeader(http.StatusForbidden)
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"delete_object","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusBadGateway {
		t.Fatalf("delete failure -> %d, want 502", code)
	}
}

// ---- presign ---------------------------------------------------------------

func TestPresignPutURL(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		// Presigning is signature-only; the stub only needs the region lookup.
		answerLocation(w, r)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"presign_put","arguments":{"module":"orders","key":"a/b.txt","expiry_seconds":120}}`)
	if code != http.StatusOK {
		t.Fatalf("presign_put -> %d, want 200", code)
	}
	u, _ := res["url"].(string)
	if !strings.Contains(u, "/t-acme-orders/a/b.txt") || !strings.Contains(u, "X-Amz-Signature") {
		t.Fatalf("url = %q, want /t-acme-orders/a/b.txt with X-Amz-Signature", u)
	}
	if res["method"] != "PUT" {
		t.Fatalf("method = %v, want PUT", res["method"])
	}
	if got, _ := res["expires_in"].(float64); int(got) != 120 {
		t.Fatalf("expires_in = %v, want 120 (from expiry_seconds)", res["expires_in"])
	}
}

func TestPresignGetURL(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		answerLocation(w, r)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"presign_get","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusOK {
		t.Fatalf("presign_get -> %d, want 200", code)
	}
	u, _ := res["url"].(string)
	if !strings.Contains(u, "/t-acme-orders/a/b.txt") || !strings.Contains(u, "X-Amz-Signature") {
		t.Fatalf("url = %q, want /t-acme-orders/a/b.txt with X-Amz-Signature", u)
	}
	// No expiry_seconds -> falls back to cfg.presignExpiry (15m = 900s).
	if got, _ := res["expires_in"].(float64); int(got) != 900 {
		t.Fatalf("expires_in = %v, want 900 (cfg default)", res["expires_in"])
	}
}

// ---- stat / list -----------------------------------------------------------

func TestStatObjectMaps404(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	code, _ := invokeResult(t, s, "acme",
		`{"tool":"stat_object","arguments":{"module":"orders","key":"missing.txt"}}`)
	if code != http.StatusNotFound {
		t.Fatalf("stat missing -> %d, want 404", code)
	}
}

func TestStatObjectReturnsMetadata(t *testing.T) {
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		w.Header().Set("Content-Length", "2")
		w.Header().Set("ETag", `"deadbeef"`)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Last-Modified", "Wed, 01 Jan 2024 00:00:00 GMT")
		w.WriteHeader(http.StatusOK)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"stat_object","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusOK {
		t.Fatalf("stat -> %d, want 200", code)
	}
	if res["bucket"] != "t-acme-orders" || res["etag"] != "deadbeef" {
		t.Fatalf("result = %v, want bucket/etag set", res)
	}
	if got, _ := res["size"].(float64); int(got) != 2 {
		t.Fatalf("size = %v, want 2", res["size"])
	}
}

func TestListObjectsReturnsCanned(t *testing.T) {
	const listXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>t-acme-orders</Name><Prefix></Prefix><KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>a/b.txt</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified>
  <ETag>"deadbeef"</ETag><Size>2</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, listXML)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"list_objects","arguments":{"module":"orders"}}`)
	if code != http.StatusOK {
		t.Fatalf("list_objects -> %d, want 200", code)
	}
	if res["bucket"] != "t-acme-orders" {
		t.Fatalf("bucket = %v, want t-acme-orders", res["bucket"])
	}
	if got, _ := res["count"].(float64); int(got) != 1 {
		t.Fatalf("count = %v, want 1", res["count"])
	}
}

func TestListBucketsFiltersByTenant(t *testing.T) {
	const bucketsXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>minio</ID><DisplayName>minio</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>t-acme-orders</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
    <Bucket><Name>t-globex-orders</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, bucketsXML)
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"list_buckets","arguments":{}}`)
	if code != http.StatusOK {
		t.Fatalf("list_buckets -> %d, want 200", code)
	}
	bs, _ := res["buckets"].([]any)
	if len(bs) != 1 {
		t.Fatalf("buckets = %v, want exactly the tenant's one (foreign tenant hidden)", res["buckets"])
	}
	first, _ := bs[0].(map[string]any)
	if first["bucket"] != "t-acme-orders" {
		t.Fatalf("bucket = %v, want t-acme-orders", first["bucket"])
	}
}

func TestEnsureBucketCreatesWhenAbsent(t *testing.T) {
	var madeBucket bool
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		switch r.Method {
		case http.MethodHead: // BucketExists -> 404 = absent
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut: // MakeBucket
			madeBucket = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	code, res := invokeResult(t, s, "acme",
		`{"tool":"ensure_bucket","arguments":{"module":"orders"}}`)
	if code != http.StatusOK {
		t.Fatalf("ensure_bucket -> %d, want 200", code)
	}
	if !madeBucket {
		t.Fatal("ensure_bucket did not issue MakeBucket for an absent bucket")
	}
	if res["bucket"] != "t-acme-orders" || res["created"] != true {
		t.Fatalf("result = %v, want {bucket:t-acme-orders, created:true}", res)
	}
}
