package main

import (
	"encoding/json"
	"fmt"
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

	// Fill exactly to the bound, then push one more to force one eviction.
	for i := 0; i < maxReplayEntries; i++ {
		s.rememberReplay("acme\x00k"+fmtInt(i), map[string]any{"tool": "ensure_bucket", "result": map[string]any{}})
	}
	newest := "acme\x00k" + fmtInt(maxReplayEntries)
	s.rememberReplay(newest, map[string]any{"tool": "ensure_bucket", "result": map[string]any{}})

	if got := len(s.replay); got != maxReplayEntries {
		t.Fatalf("replay size = %d, want %d (bounded)", got, maxReplayEntries)
	}
	if _, ok := s.replay["acme\x00k0"]; ok {
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
