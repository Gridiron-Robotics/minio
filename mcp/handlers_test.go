package main

// Handler coverage against a fake S3 endpoint.
//
// Before this file, not one of put/get/stat/delete/presign/list was ever
// executed by a test — the suite proved the catalog's shape and the auth gate
// and stopped there. These tests drive the REAL minio-go client against an
// httptest server speaking enough S3 to exercise each handler end to end, so
// the tenant-isolation guarantee is checked where it actually matters: in the
// bucket each request is sent to, on the wire.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 records every request it receives and answers with the minimum valid
// S3 responses minio-go needs.
type fakeS3 struct {
	mu       sync.Mutex
	requests []string          // "METHOD /path" in order
	objects  map[string]string // "bucket/key" -> body
	buckets  map[string]bool
	failNext bool // make object ops return 500 until cleared
}

// failObjectOps makes every object operation return 500 until stopFailing is
// called. It has to be sticky rather than one-shot: minio-go retries 5xx, so a
// single-shot failure is silently absorbed by the client's own retry and the
// handler never sees an error at all.
func (f *fakeS3) failObjectOps() {
	f.mu.Lock()
	f.failNext = true
	f.mu.Unlock()
}

func (f *fakeS3) stopFailing() {
	f.mu.Lock()
	f.failNext = false
	f.mu.Unlock()
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects: map[string]string{},
		buckets: map[string]bool{},
	}
}

func (f *fakeS3) record(r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
}

// sawRequestTo reports whether any recorded request targeted the given path.
func (f *fakeS3) sawRequestTo(method, path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, req := range f.requests {
		if req == method+" "+path {
			return true
		}
	}
	return false
}

func (f *fakeS3) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.record(r)
	q := r.URL.Query()
	trimmed := strings.Trim(r.URL.Path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	// Bucket location probe — minio-go issues this before the first op.
	if _, ok := q["location"]; ok {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		return
	}

	// ListBuckets
	if bucket == "" && r.Method == http.MethodGet {
		f.mu.Lock()
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>x</ID><DisplayName>x</DisplayName></Owner><Buckets>`)
		for b := range f.buckets {
			fmt.Fprintf(&sb, `<Bucket><Name>%s</Name><CreationDate>2026-01-01T00:00:00.000Z</CreationDate></Bucket>`, b)
		}
		sb.WriteString(`</Buckets></ListAllMyBucketsResult>`)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, sb.String())
		return
	}

	f.mu.Lock()
	fail := f.failNext
	f.mu.Unlock()
	if fail && key != "" {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>boom</Message></Error>`)
		return
	}

	switch {
	// ListObjects (v2)
	case key == "" && r.Method == http.MethodGet && q.Get("list-type") == "2":
		prefix := q.Get("prefix")
		f.mu.Lock()
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		fmt.Fprintf(&sb, `<Name>%s</Name><Prefix>%s</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, bucket, prefix)
		for ok, body := range f.objects {
			bk := strings.SplitN(ok, "/", 2)
			if bk[0] != bucket || !strings.HasPrefix(bk[1], prefix) {
				continue
			}
			fmt.Fprintf(&sb, `<Contents><Key>%s</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag-%s&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`,
				bk[1], bk[1], len(body))
		}
		sb.WriteString(`</ListBucketResult>`)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, sb.String())

	// BucketExists
	case key == "" && r.Method == http.MethodHead:
		f.mu.Lock()
		exists := f.buckets[bucket]
		f.mu.Unlock()
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)

	// MakeBucket
	case key == "" && r.Method == http.MethodPut:
		f.mu.Lock()
		f.buckets[bucket] = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	// StatObject
	case key != "" && r.Method == http.MethodHead:
		f.mu.Lock()
		body, ok := f.objects[bucket+"/"+key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Header().Set("ETag", `"etag-`+key+`"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", time.Unix(1_700_000_000, 0).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)

	// PutObject
	case key != "" && r.Method == http.MethodPut:
		buf := new(strings.Builder)
		_, _ = fmt.Fprint(buf) // keep imports honest
		b := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			_, _ = r.Body.Read(b)
		}
		f.mu.Lock()
		f.objects[bucket+"/"+key] = string(b)
		f.mu.Unlock()
		w.Header().Set("ETag", `"etag-`+key+`"`)
		w.WriteHeader(http.StatusOK)

	// RemoveObject
	case key != "" && r.Method == http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, bucket+"/"+key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// s3TestServer wires a server whose minio client points at the fake.
func s3TestServer(t *testing.T) (*server, *fakeS3) {
	t.Helper()
	fake := newFakeS3()
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	cfg := config{
		s3Endpoint:    strings.TrimPrefix(ts.URL, "http://"),
		s3AccessKey:   "key",
		s3SecretKey:   "secret",
		authToken:     "secret-token",
		authSet:       true,
		presignExpiry: 15 * time.Minute,
	}
	cfg.authDigest = sha256Sum("secret-token")
	s := testServerCfg(t, cfg)
	return s, fake
}

// invoke runs a tool through the full /invoke path (auth, schema, tenant,
// replay) and returns the decoded envelope.
func invoke(t *testing.T, s *server, tenant, idemKey, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	if tenant != "" {
		req.Header.Set("X-Tenant-Id", tenant)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	s.handleInvoke(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func resultOf(t *testing.T, env map[string]any) map[string]any {
	t.Helper()
	res, ok := env["result"].(map[string]any)
	if !ok {
		t.Fatalf("envelope has no result object: %v", env)
	}
	return res
}

// ---- per-handler coverage --------------------------------------------------

func TestPutObjectHandler(t *testing.T) {
	s, fake := s3TestServer(t)
	code, env := invoke(t, s, "acme", "", `{"tool":"put_object","arguments":{"module":"orders","key":"a/b.txt","content_base64":"aGVsbG8=","content_type":"text/plain"}}`)
	if code != http.StatusOK {
		t.Fatalf("put_object -> %d: %v", code, env)
	}
	res := resultOf(t, env)
	if res["bucket"] != "t-acme-orders" || res["key"] != "a/b.txt" {
		t.Fatalf("unexpected result: %v", res)
	}
	// The write must have gone to the tenant's bucket, on the wire.
	if !fake.sawRequestTo(http.MethodPut, "/t-acme-orders/a/b.txt") {
		t.Fatalf("no PUT to the tenant bucket; requests were %v", fake.paths())
	}
}

func TestPutObjectRejectsBadBase64(t *testing.T) {
	s, _ := s3TestServer(t)
	code, env := invoke(t, s, "acme", "", `{"tool":"put_object","arguments":{"module":"orders","key":"k","content_base64":"!!!not base64!!!"}}`)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("bad base64 -> %d, want 422 (%v)", code, env)
	}
}

func TestStatObjectHandler(t *testing.T) {
	s, fake := s3TestServer(t)
	fake.objects["t-acme-orders/report.pdf"] = "0123456789"

	code, env := invoke(t, s, "acme", "", `{"tool":"stat_object","arguments":{"module":"orders","key":"report.pdf"}}`)
	if code != http.StatusOK {
		t.Fatalf("stat_object -> %d: %v", code, env)
	}
	res := resultOf(t, env)
	if res["bucket"] != "t-acme-orders" {
		t.Fatalf("bucket = %v, want t-acme-orders", res["bucket"])
	}
	if size, _ := res["size"].(float64); int(size) != 10 {
		t.Fatalf("size = %v, want 10", res["size"])
	}
}

func TestStatObjectMissingIs404(t *testing.T) {
	s, _ := s3TestServer(t)
	code, _ := invoke(t, s, "acme", "", `{"tool":"stat_object","arguments":{"module":"orders","key":"nope"}}`)
	if code != http.StatusNotFound {
		t.Fatalf("missing object -> %d, want 404", code)
	}
}

func TestDeleteObjectHandler(t *testing.T) {
	s, fake := s3TestServer(t)
	fake.objects["t-acme-orders/gone.txt"] = "x"

	code, env := invoke(t, s, "acme", "", `{"tool":"delete_object","arguments":{"module":"orders","key":"gone.txt"}}`)
	if code != http.StatusOK {
		t.Fatalf("delete_object -> %d: %v", code, env)
	}
	if resultOf(t, env)["deleted"] != true {
		t.Fatalf("want deleted=true, got %v", env)
	}
	if _, still := fake.objects["t-acme-orders/gone.txt"]; still {
		t.Fatal("object should be gone from the store")
	}
}

func TestListObjectsHandler(t *testing.T) {
	s, fake := s3TestServer(t)
	fake.objects["t-acme-orders/inv/1.txt"] = "aa"
	fake.objects["t-acme-orders/inv/2.txt"] = "bbb"
	fake.objects["t-acme-orders/other/3.txt"] = "c"

	code, env := invoke(t, s, "acme", "", `{"tool":"list_objects","arguments":{"module":"orders","prefix":"inv/","recursive":true}}`)
	if code != http.StatusOK {
		t.Fatalf("list_objects -> %d: %v", code, env)
	}
	res := resultOf(t, env)
	objs, _ := res["objects"].([]any)
	if len(objs) != 2 {
		t.Fatalf("prefix inv/ returned %d objects, want 2: %v", len(objs), res)
	}
}

// `max` must actually cap the result, or a huge bucket becomes a memory
// problem for the caller and the sidecar alike.
func TestListObjectsRespectsMax(t *testing.T) {
	s, fake := s3TestServer(t)
	for i := 0; i < 10; i++ {
		fake.objects[fmt.Sprintf("t-acme-orders/f%d.txt", i)] = "x"
	}
	code, env := invoke(t, s, "acme", "", `{"tool":"list_objects","arguments":{"module":"orders","max":3}}`)
	if code != http.StatusOK {
		t.Fatalf("-> %d: %v", code, env)
	}
	if objs, _ := resultOf(t, env)["objects"].([]any); len(objs) != 3 {
		t.Fatalf("max=3 returned %d objects", len(objs))
	}
}

func TestListBucketsIsTenantScoped(t *testing.T) {
	s, fake := s3TestServer(t)
	fake.buckets["t-acme-orders"] = true
	fake.buckets["t-acme-catalog"] = true
	fake.buckets["t-globex-orders"] = true // another tenant's bucket

	code, env := invoke(t, s, "acme", "", `{"tool":"list_buckets","arguments":{}}`)
	if code != http.StatusOK {
		t.Fatalf("list_buckets -> %d: %v", code, env)
	}
	buckets, _ := resultOf(t, env)["buckets"].([]any)
	if len(buckets) != 2 {
		t.Fatalf("acme should see exactly its own 2 buckets, got %d: %v", len(buckets), buckets)
	}
	for _, b := range buckets {
		name := b.(map[string]any)["bucket"].(string)
		if strings.Contains(name, "globex") {
			t.Fatalf("cross-tenant leak: acme saw %q", name)
		}
	}
}

func TestEnsureBucketCreatesOnceThenReports(t *testing.T) {
	s, fake := s3TestServer(t)

	code, env := invoke(t, s, "acme", "", `{"tool":"ensure_bucket","arguments":{"module":"orders"}}`)
	if code != http.StatusOK {
		t.Fatalf("-> %d: %v", code, env)
	}
	if res := resultOf(t, env); res["created"] != true || res["bucket"] != "t-acme-orders" {
		t.Fatalf("first call should create: %v", res)
	}
	if !fake.buckets["t-acme-orders"] {
		t.Fatal("bucket was not actually created")
	}
	// Second call is a no-op that reports created=false.
	_, env2 := invoke(t, s, "acme", "", `{"tool":"ensure_bucket","arguments":{"module":"orders"}}`)
	if resultOf(t, env2)["created"] != false {
		t.Fatalf("second call should report created=false: %v", env2)
	}
}

func TestPresignGetAndPut(t *testing.T) {
	s, _ := s3TestServer(t)
	for _, tool := range []string{"presign_get", "presign_put"} {
		code, env := invoke(t, s, "acme", "",
			fmt.Sprintf(`{"tool":%q,"arguments":{"module":"orders","key":"f.bin","expiry_seconds":60}}`, tool))
		if code != http.StatusOK {
			t.Fatalf("%s -> %d: %v", tool, code, env)
		}
		res := resultOf(t, env)
		u, _ := res["url"].(string)
		if !strings.Contains(u, "t-acme-orders") || !strings.Contains(u, "f.bin") {
			t.Fatalf("%s url does not address the tenant object: %s", tool, u)
		}
		// A presigned URL that is not actually signed would hand out
		// unauthenticated access to the bucket.
		if !strings.Contains(u, "X-Amz-Signature") {
			t.Fatalf("%s url is not signed: %s", tool, u)
		}
		if exp, _ := res["expires_in"].(float64); int(exp) != 60 {
			t.Fatalf("%s expires_in = %v, want 60", tool, res["expires_in"])
		}
	}
}

// An upstream S3 failure must surface as 502, not a 200 with a broken body.
func TestUpstreamFailureIsBadGateway(t *testing.T) {
	s, fake := s3TestServer(t)
	fake.failObjectOps()
	code, _ := invoke(t, s, "acme", "", `{"tool":"put_object","arguments":{"module":"orders","key":"k","content_base64":"aGk="}}`)
	if code != http.StatusBadGateway {
		t.Fatalf("upstream 500 -> %d, want 502", code)
	}
}

// ---- tenant isolation on the wire ------------------------------------------

// The security property that matters: two tenants using an identical module
// and key must never touch the same bucket.
func TestTenantsCannotCollide(t *testing.T) {
	s, fake := s3TestServer(t)
	body := `{"tool":"put_object","arguments":{"module":"orders","key":"same.txt","content_base64":"aGk="}}`
	if code, env := invoke(t, s, "acme", "", body); code != http.StatusOK {
		t.Fatalf("acme put -> %d: %v", code, env)
	}
	if code, env := invoke(t, s, "globex", "", body); code != http.StatusOK {
		t.Fatalf("globex put -> %d: %v", code, env)
	}
	if !fake.sawRequestTo(http.MethodPut, "/t-acme-orders/same.txt") ||
		!fake.sawRequestTo(http.MethodPut, "/t-globex-orders/same.txt") {
		t.Fatalf("tenants did not land in separate buckets: %v", fake.paths())
	}
}

// A hostile tenant header must not be able to steer the request at another
// tenant's bucket.
//
// This is a regression test for a real escape. The old sanitiser replaced every
// invalid character with a dash and then trimmed leading/trailing dashes, so
// "../globex", "..globex", "/globex/" and "___globex" ALL collapsed to exactly
// "globex" — a different, legitimate tenant. Anything able to influence
// X-Tenant-Id could read, overwrite and delete that tenant's objects while the
// request looked perfectly well-formed. Malformed segments are now refused
// rather than repaired.
func TestHostileTenantHeaderCannotEscape(t *testing.T) {
	victim := "globex"
	for _, hostile := range []string{"../globex", "..globex", "/globex/", "___globex", "GLOBEX/../globex", "globex/"} {
		t.Run(hostile, func(t *testing.T) {
			s, fake := s3TestServer(t)
			code, env := invoke(t, s, hostile, "",
				`{"tool":"put_object","arguments":{"module":"orders","key":"k.txt","content_base64":"aGk="}}`)
			if code != http.StatusUnprocessableEntity {
				t.Fatalf("hostile tenant %q -> %d, want 422 (refused, not normalised): %v", hostile, code, env)
			}
			// Nothing may have reached the victim's bucket on the wire.
			if fake.sawRequestTo(http.MethodPut, "/t-"+victim+"-orders/k.txt") {
				t.Fatalf("tenant %q escaped into the victim bucket: %v", hostile, fake.paths())
			}
		})
	}
}

// The mirror of the above: a legitimate tenant must still work, and case is
// normalised rather than rejected.
func TestValidTenantsStillWork(t *testing.T) {
	for _, tenant := range []string{"globex", "acme-corp", "t2", "ACME"} {
		t.Run(tenant, func(t *testing.T) {
			s, _ := s3TestServer(t)
			code, env := invoke(t, s, tenant, "",
				`{"tool":"put_object","arguments":{"module":"orders","key":"k.txt","content_base64":"aGk="}}`)
			if code != http.StatusOK {
				t.Fatalf("valid tenant %q -> %d: %v", tenant, code, env)
			}
			want := "t-" + strings.ToLower(tenant) + "-orders"
			if got := resultOf(t, env)["bucket"]; got != want {
				t.Fatalf("tenant %q -> bucket %v, want %s", tenant, got, want)
			}
		})
	}
}

// ---- idempotency through the real handler path -----------------------------

// The replay promise, end to end: the second call with the same key must NOT
// reach S3 again, and must return the first call's result.
func TestIdempotentReplaySkipsSecondExecution(t *testing.T) {
	s, fake := s3TestServer(t)
	body := `{"tool":"put_object","arguments":{"module":"orders","key":"once.txt","content_base64":"aGk="}}`

	code1, env1 := invoke(t, s, "acme", "key-1", body)
	if code1 != http.StatusOK {
		t.Fatalf("first -> %d: %v", code1, env1)
	}
	if env1["replayed"] == true {
		t.Fatal("first call must not be marked replayed")
	}
	putsAfterFirst := countPuts(fake, "/t-acme-orders/once.txt")

	code2, env2 := invoke(t, s, "acme", "key-1", body)
	if code2 != http.StatusOK {
		t.Fatalf("second -> %d: %v", code2, env2)
	}
	if env2["replayed"] != true {
		t.Fatalf("second call must be marked replayed: %v", env2)
	}
	if got := countPuts(fake, "/t-acme-orders/once.txt"); got != putsAfterFirst {
		t.Fatalf("replay re-executed against S3: %d PUTs before, %d after", putsAfterFirst, got)
	}
	// And it must be the SAME result, not merely a well-formed one.
	if fmt.Sprint(resultOf(t, env1)) != fmt.Sprint(resultOf(t, env2)) {
		t.Fatalf("replayed result differs:\n first=%v\nsecond=%v", env1["result"], env2["result"])
	}
}

// An Idempotency-Key is scoped per tenant: one tenant reusing another's key
// must execute its own call, never read back the other's stored result.
func TestIdempotencyKeyIsTenantScoped(t *testing.T) {
	s, fake := s3TestServer(t)
	body := `{"tool":"put_object","arguments":{"module":"orders","key":"x.txt","content_base64":"aGk="}}`

	invoke(t, s, "acme", "shared-key", body)
	code, env := invoke(t, s, "globex", "shared-key", body)
	if code != http.StatusOK {
		t.Fatalf("-> %d: %v", code, env)
	}
	if env["replayed"] == true {
		t.Fatal("a different tenant must not replay another tenant's key")
	}
	if !fake.sawRequestTo(http.MethodPut, "/t-globex-orders/x.txt") {
		t.Fatalf("globex call did not execute: %v", fake.paths())
	}
}

// Without a key there is no replay — every call executes.
func TestNoIdempotencyKeyAlwaysExecutes(t *testing.T) {
	s, fake := s3TestServer(t)
	body := `{"tool":"put_object","arguments":{"module":"orders","key":"n.txt","content_base64":"aGk="}}`
	if code, env := invoke(t, s, "acme", "", body); code != http.StatusOK {
		t.Fatalf("first keyless call -> %d: %v", code, env)
	}
	first := countPuts(fake, "/t-acme-orders/n.txt")
	_, env := invoke(t, s, "acme", "", body)
	if env["replayed"] == true {
		t.Fatal("no key -> must not be treated as a replay")
	}
	if got := countPuts(fake, "/t-acme-orders/n.txt"); got <= first {
		t.Fatalf("second keyless call did not execute (%d -> %d)", first, got)
	}
}

// A failed call must NOT be cached: retrying after a transient upstream error
// has to actually retry, not replay the failure or a phantom success.
func TestFailedCallIsNotReplayed(t *testing.T) {
	s, fake := s3TestServer(t)
	body := `{"tool":"put_object","arguments":{"module":"orders","key":"r.txt","content_base64":"aGk="}}`

	fake.failObjectOps()
	if code, _ := invoke(t, s, "acme", "retry-key", body); code != http.StatusBadGateway {
		t.Fatalf("first call should fail 502, got %d", code)
	}
	fake.stopFailing()
	code, env := invoke(t, s, "acme", "retry-key", body)
	if code != http.StatusOK {
		t.Fatalf("retry after failure -> %d, want 200: %v", code, env)
	}
	if env["replayed"] == true {
		t.Fatal("a failed call must not have been stored as a replayable result")
	}
}

func countPuts(f *fakeS3, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r == "PUT "+path {
			n++
		}
	}
	return n
}

// Guard the encoding contract the put handler relies on.
func TestBase64RoundTrip(t *testing.T) {
	if got := base64.StdEncoding.EncodeToString([]byte("hi")); got != "aGk=" {
		t.Fatalf("fixture drift: %q", got)
	}
}
