package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// resetReplay clears the idempotency cache so replay tests are order-independent.
// Defined in a _test.go file (same package main) so it never ships in the binary.
func (s *server) resetReplay() {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	s.replay = map[string]map[string]any{}
	s.replayOrder = nil
}

// ensureBucketStub counts how many times ensure_bucket actually reaches S3
// (one HEAD BucketExists per real execution) so a test can prove a replayed call
// did NOT re-run the handler. The stub reports the bucket as already existing, so
// no MakeBucket PUT follows.
func ensureBucketStub(t *testing.T, execs *int) *server {
	return s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		if r.Method == http.MethodHead {
			*execs++
		}
		w.WriteHeader(http.StatusOK) // HEAD 200 => bucket exists => created:false
	})
}

func invokeEnvelope(t *testing.T, s *server, tenant, idem, body string) (int, map[string]any) {
	t.Helper()
	headers := map[string]string{"X-Tenant-Id": tenant}
	if idem != "" {
		headers["Idempotency-Key"] = idem
	}
	rec := doH(s, http.MethodPost, "/invoke", "secret", headers, body)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	var env map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v (body=%s)", err, rec.Body.String())
	}
	return rec.Code, env
}

const ensureBucketBody = `{"tool":"ensure_bucket","arguments":{"module":"orders"}}`

// replayKeyFor builds the cache key handleInvoke would build for ensureBucketBody
// under (tenant, idem), including the request fingerprint.
func replayKeyFor(tenant, idem string) string {
	return tenant + "\x00" + idem + "\x00" +
		requestFingerprint("ensure_bucket", map[string]any{"module": "orders"})
}

func TestIdempotentReplay(t *testing.T) {
	execs := 0
	s := ensureBucketStub(t, &execs)

	code, env := invokeEnvelope(t, s, "acme", "key-1", ensureBucketBody)
	if code != http.StatusOK {
		t.Fatalf("first call -> %d, want 200", code)
	}
	if _, replayed := env["replayed"]; replayed {
		t.Fatal("first call should not be marked replayed")
	}

	code, env = invokeEnvelope(t, s, "acme", "key-1", ensureBucketBody)
	if code != http.StatusOK {
		t.Fatalf("second call -> %d, want 200", code)
	}
	if env["replayed"] != true {
		t.Fatalf("second call env = %v, want replayed:true", env)
	}
	if execs != 1 {
		t.Fatalf("handler ran %d times, want exactly 1 (replay must not re-execute)", execs)
	}
}

func TestReplayIsPerTenant(t *testing.T) {
	execs := 0
	s := ensureBucketStub(t, &execs)

	if code, _ := invokeEnvelope(t, s, "acme", "key-1", ensureBucketBody); code != http.StatusOK {
		t.Fatalf("acme call -> %d, want 200", code)
	}
	// Same Idempotency-Key, different tenant: must NOT replay acme's result.
	code, env := invokeEnvelope(t, s, "globex", "key-1", ensureBucketBody)
	if code != http.StatusOK {
		t.Fatalf("globex call -> %d, want 200", code)
	}
	if _, replayed := env["replayed"]; replayed {
		t.Fatal("different tenant, same key: must not be a replay")
	}
	if execs != 2 {
		t.Fatalf("handler ran %d times, want 2 (replay is per-tenant)", execs)
	}
}

func TestReplayEvictsOldest(t *testing.T) {
	execs := 0
	s := ensureBucketStub(t, &execs)
	s.resetReplay()

	// Fill exactly to the bound, then push one more to force one eviction. Keys are
	// built exactly as handleInvoke builds them (tenant + Idempotency-Key + request
	// fingerprint) so the end-to-end half below hits the same entries.
	oldest := replayKeyFor("acme", "k"+fmtInt(0))
	for i := 0; i < maxReplayEntries; i++ {
		s.rememberReplay(replayKeyFor("acme", "k"+fmtInt(i)), map[string]any{"tool": "ensure_bucket", "result": map[string]any{}})
	}
	newest := replayKeyFor("acme", "k"+fmtInt(maxReplayEntries))
	s.rememberReplay(newest, map[string]any{"tool": "ensure_bucket", "result": map[string]any{}})

	if got := len(s.replay); got != maxReplayEntries {
		t.Fatalf("replay size = %d, want %d (bounded)", got, maxReplayEntries)
	}
	if _, ok := s.replay[oldest]; ok {
		t.Fatal("oldest key should have been evicted")
	}
	if _, ok := s.replay[newest]; !ok {
		t.Fatal("newest key should still be cached")
	}

	// End-to-end: the newest key replays (handler not run); the evicted oldest
	// key re-executes (handler runs once).
	code, env := invokeEnvelope(t, s, "acme", "k"+fmtInt(maxReplayEntries), ensureBucketBody)
	if code != http.StatusOK || env["replayed"] != true {
		t.Fatalf("newest key: code=%d env=%v, want 200 replayed:true", code, env)
	}
	if execs != 0 {
		t.Fatalf("newest key replayed but handler ran %d times, want 0", execs)
	}
	code, env = invokeEnvelope(t, s, "acme", "k0", ensureBucketBody)
	if code != http.StatusOK {
		t.Fatalf("evicted key -> %d, want 200", code)
	}
	if _, replayed := env["replayed"]; replayed {
		t.Fatal("evicted oldest key must re-execute, not replay")
	}
	if execs != 1 {
		t.Fatalf("evicted key: handler ran %d times, want 1", execs)
	}
}

func fmtInt(i int) string { return fmt.Sprintf("%05d", i) }

// The replay cache was keyed on (tenant, Idempotency-Key) alone, so ONE reused key
// across two different calls returned the first call's envelope and the second
// call never ran — a delete_object that reports success and never happened. The
// key now carries a fingerprint of (tool, arguments): identical requests replay,
// different ones execute.
func TestReplayIsBoundToTheRequest(t *testing.T) {
	// Answers every verb the three tools below need: HEAD=bucket exists,
	// DELETE=204 no content, PUT=stored.
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	})

	if code, _ := invokeEnvelope(t, s, "acme", "key-1", ensureBucketBody); code != http.StatusOK {
		t.Fatalf("first call -> %d, want 200", code)
	}

	// Same key, DIFFERENT tool: must not be answered from the ensure_bucket entry.
	code, env := invokeEnvelope(t, s, "acme", "key-1",
		`{"tool":"delete_object","arguments":{"module":"orders","key":"a.txt"}}`)
	if code != http.StatusOK {
		t.Fatalf("different tool under a reused key -> %d, want 200 (executed)", code)
	}
	if env["replayed"] == true {
		t.Fatalf("delete_object was answered from another tool's cache entry: %v", env)
	}
	if env["tool"] != "delete_object" {
		t.Fatalf("envelope tool = %v, want delete_object — the caller got a foreign result", env["tool"])
	}

	// Same key, same tool, DIFFERENT arguments: also a different request.
	code, env = invokeEnvelope(t, s, "acme", "key-1",
		`{"tool":"ensure_bucket","arguments":{"module":"invoices"}}`)
	if code != http.StatusOK {
		t.Fatalf("different args under a reused key -> %d, want 200", code)
	}
	if env["replayed"] == true {
		t.Fatalf("different arguments replayed a stale result: %v", env)
	}
	res, _ := env["result"].(map[string]any)
	if res["bucket"] != "t-acme-invoices" {
		t.Fatalf("result = %v, want the invoices bucket actually operated on", env["result"])
	}

	// And the identical request still replays (idempotency is not lost).
	code, env = invokeEnvelope(t, s, "acme", "key-1", ensureBucketBody)
	if code != http.StatusOK || env["replayed"] != true {
		t.Fatalf("identical repeat -> code=%d env=%v, want replayed:true", code, env)
	}
	// Argument ORDER in the JSON must not change the fingerprint.
	code, env = invokeEnvelope(t, s, "acme", "key-2",
		`{"tool":"put_object","arguments":{"key":"a.txt","module":"orders","content_base64":"aGk="}}`)
	if code != http.StatusOK {
		t.Fatalf("put -> %d, want 200", code)
	}
	code, env = invokeEnvelope(t, s, "acme", "key-2",
		`{"tool":"put_object","arguments":{"module":"orders","content_base64":"aGk=","key":"a.txt"}}`)
	if code != http.StatusOK || env["replayed"] != true {
		t.Fatalf("same args in another order -> code=%d env=%v, want replayed:true", code, env)
	}
}
