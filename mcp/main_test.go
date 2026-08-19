package main

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testServer(t *testing.T, token string) *server {
	t.Helper()
	cfg := config{s3Endpoint: "minio:9000", s3AccessKey: "x", s3SecretKey: "y", authToken: token}
	if token != "" {
		cfg.authSet = true
		cfg.authDigest = sha256.Sum256([]byte(token))
	}
	s, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	return s
}

func do(s *server, method, path, token, body string) *httptest.ResponseRecorder {
	return doH(s, method, path, token, nil, body)
}

// doH is do with arbitrary request headers, so tests can exercise X-Tenant-Id
// and Idempotency-Key (neither of which the bare `do` sets).
func doH(s *server, method, path, token string, headers map[string]string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	switch {
	case path == "/tools" || strings.HasPrefix(path, "/tools?"):
		s.handleTools(rec, req)
	case path == "/invoke":
		s.handleInvoke(rec, req)
	default:
		s.handleRoot(rec, req)
	}
	return rec
}

func TestHealthHeadNoAuth(t *testing.T) {
	s := testServer(t, "secret")
	rec := do(s, http.MethodHead, "/", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD / = %d, want 200 (unauthenticated health)", rec.Code)
	}
}

func TestToolsRequiresBearer(t *testing.T) {
	s := testServer(t, "secret")
	if rec := do(s, http.MethodGet, "/tools", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no bearer -> %d, want 401", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/tools", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token -> %d, want 401", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/tools", "secret", ""); rec.Code != http.StatusOK {
		t.Fatalf("right token -> %d, want 200", rec.Code)
	}
}

func TestToolsCatalogShape(t *testing.T) {
	s := testServer(t, "secret")
	rec := do(s, http.MethodGet, "/tools?server=minio", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code %d", rec.Code)
	}
	var out struct {
		Tools []struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"input_schema"`
			Annotations struct {
				DestructiveHint bool `json:"destructiveHint"`
			} `json:"annotations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if len(out.Tools) == 0 {
		t.Fatal("empty catalog")
	}
	seen := map[string]bool{}
	for _, tl := range out.Tools {
		if tl.Name == "" || tl.Description == "" || tl.InputSchema == nil {
			t.Errorf("tool %q missing name/description/input_schema", tl.Name)
		}
		if tl.InputSchema["type"] != "object" {
			t.Errorf("tool %q input_schema.type != object", tl.Name)
		}
		seen[tl.Name] = tl.Annotations.DestructiveHint
	}
	for name, wantDestr := range map[string]bool{
		"list_buckets": false, "list_objects": false, "stat_object": false, "presign_get": false,
		"put_object": true, "delete_object": true, "ensure_bucket": true, "presign_put": true,
	} {
		got, ok := seen[name]
		if !ok {
			t.Errorf("missing tool %q", name)
			continue
		}
		if got != wantDestr {
			t.Errorf("tool %q destructiveHint=%v, want %v", name, got, wantDestr)
		}
	}
}

func TestUnknownServerIsEmptyList(t *testing.T) {
	s := testServer(t, "secret")
	rec := do(s, http.MethodGet, "/tools?server=nope", "secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("unknown server -> %d, want 200 empty list", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"tools":[]`) {
		t.Fatalf("want empty tools, got %s", rec.Body.String())
	}
}

func TestInvokeUnknownTool404(t *testing.T) {
	s := testServer(t, "secret")
	rec := do(s, http.MethodPost, "/invoke", "secret", `{"tool":"does_not_exist","arguments":{}}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown tool -> %d, want 404", rec.Code)
	}
}

func TestInvokeMissingRequired422(t *testing.T) {
	s := testServer(t, "secret")
	rec := do(s, http.MethodPost, "/invoke", "secret", `{"tool":"stat_object","arguments":{"module":"x"}}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing required -> %d, want 422", rec.Code)
	}
}

// With no MCP_AUTH_TOKEN configured the sidecar must fail CLOSED. It previously
// accepted any non-empty bearer, which handed put_object/delete_object/
// presign_put/ensure_bucket to any caller that could reach the port.
func TestNoTokenDeniesEveryCaller(t *testing.T) {
	s := testServer(t, "")
	if rec := do(s, http.MethodGet, "/tools", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token configured, no bearer -> %d, want 401", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/tools", "anything", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token configured, any bearer -> %d, want 401 (must fail closed)", rec.Code)
	}
	rec := do(s, http.MethodPost, "/invoke", "anything", `{"tool":"put_object","arguments":{}}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token configured, /invoke with any bearer -> %d, want 401", rec.Code)
	}
	// HEAD / stays unauthenticated so gateways and deploy/smoke.sh still probe.
	if rec := do(s, http.MethodHead, "/", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("HEAD / -> %d, want 200 even with no token configured", rec.Code)
	}
}

// realToken is a stand-in for a real Infisical-issued bearer: long enough and not
// a sample value, so bootRefusal must let it through.
const realToken = "Zk3Qm8vT1sB7xH2pR9wL4nC6"

func TestBootRefusesWithoutToken(t *testing.T) {
	if bootRefusal(config{}) == "" {
		t.Fatal("no token and no opt-out: want a boot refusal, got none")
	}
	if msg := bootRefusal(config{authSet: true, authToken: realToken}); msg != "" {
		t.Fatalf("real token set: want boot, got refusal %q", msg)
	}
	if msg := bootRefusal(config{allowInsecure: true}); msg != "" {
		t.Fatalf("%s=1: want boot (deny-all), got refusal %q", allowInsecureVar, msg)
	}
}

// compose only enforces that MINIO_MCP_TOKEN is NON-EMPTY (${VAR:?}), so a copied
// .env.example or a throwaway value boots a sidecar whose single bearer — the only
// thing in front of put_object/delete_object/presign_put on EVERY tenant bucket —
// is publicly known or trivially guessable.
func TestBootRefusesPlaceholderOrWeakToken(t *testing.T) {
	for _, tok := range []string{
		"change_me_min_8_chars", // literally .env.example's sample value
		"changeme", "CHANGEME", "secret", "token", "placeholder", "your-token-here",
		"short", "0123456789abcde", // 15 chars: below the floor
	} {
		if msg := bootRefusal(config{authSet: true, authToken: tok}); msg == "" {
			t.Errorf("token %q booted — want a refusal", tok)
		}
	}
	for _, tok := range []string{realToken, "0123456789abcdef", strings.Repeat("k", 64)} {
		if msg := bootRefusal(config{authSet: true, authToken: tok}); msg != "" {
			t.Errorf("token %q refused (%q) — a real secret must boot", tok, msg)
		}
	}
}

// Replaces the previous sanitize-and-proceed assertion: a tenant value that does
// not normalize to a valid segment is now REJECTED rather than rewritten, because
// rewriting mapped distinct tenants onto one bucket (see tenant_test.go).
func TestBucketForTenantIsolation(t *testing.T) {
	b, herr := bucketFor("acme", "orders")
	if herr != nil || b != "t-acme-orders" {
		t.Fatalf("bucketFor(acme,orders) = %q,%v want t-acme-orders", b, herr)
	}
	for _, bad := range []string{"acme/../globex", "acme hr", "-acme", "acme-", ""} {
		got, herr := bucketFor(bad, "orders")
		if herr == nil {
			t.Fatalf("tenant %q accepted as %q — want a 422 rejection", bad, got)
		}
		if herr.status != 422 {
			t.Fatalf("tenant %q -> status %d, want 422", bad, herr.status)
		}
	}
	for _, bad := range []string{"or ders", "or/ders", "", "orders-v2"} {
		got, herr := bucketFor("acme", bad)
		if herr == nil {
			t.Fatalf("module %q accepted as %q — want a 422 rejection", bad, got)
		}
	}
	// Whatever bucketFor DOES return can never carry a path or a space.
	if strings.ContainsAny(b, "/ .\\") || strings.Contains(b, "..") {
		t.Fatalf("bucket name %q is not path-safe", b)
	}
}

// ALLOW_INSECURE excuses running WITHOUT a token (deny-all dev mode). It must
// NOT excuse running WITH a guessable one: when a token is set the server runs
// in token-VERIFY mode and that value is a live credential.
//
// The first version of the placeholder guard returned early on allowInsecure
// above the placeholder/length checks, so
//
//	MINIO_MCP_ALLOW_INSECURE=1 MCP_AUTH_TOKEN=change_me
//
// booted in verify mode and ACCEPTED the publicly-known bearer — granting
// put_object/delete_object/presign_put on every tenant bucket to anyone who
// read the sample env file. Found by the 2026-08-17 adversarial review.
func TestAllowInsecureDoesNotExcuseAWeakToken(t *testing.T) {
	for _, tok := range []string{"change_me", "change_me_min_8_chars", "secret", "0123456789abcde"} {
		if msg := bootRefusal(config{allowInsecure: true, authSet: true, authToken: tok}); msg == "" {
			t.Errorf("ALLOW_INSECURE=1 with weak token %q booted in verify mode — "+
				"want a refusal; the escape hatch is for TOKENLESS deny-all only", tok)
		}
	}
	// The documented escape still works: no token at all boots deny-all.
	if msg := bootRefusal(config{allowInsecure: true}); msg != "" {
		t.Fatalf("%s=1 with no token: want boot (deny-all), got refusal %q", allowInsecureVar, msg)
	}
	// And a real token still boots with the escape set.
	if msg := bootRefusal(config{allowInsecure: true, authSet: true, authToken: realToken}); msg != "" {
		t.Fatalf("ALLOW_INSECURE=1 with a real token: want boot, got refusal %q", msg)
	}
}
