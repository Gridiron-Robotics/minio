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

func TestBootRefusesWithoutToken(t *testing.T) {
	if bootRefusal(config{}) == "" {
		t.Fatal("no token and no opt-out: want a boot refusal, got none")
	}
	if msg := bootRefusal(config{authSet: true}); msg != "" {
		t.Fatalf("token set: want boot, got refusal %q", msg)
	}
	if msg := bootRefusal(config{allowInsecure: true}); msg != "" {
		t.Fatalf("%s=1: want boot (deny-all), got refusal %q", allowInsecureVar, msg)
	}
}

func TestBucketForTenantIsolation(t *testing.T) {
	b, herr := bucketFor("acme", "orders")
	if herr != nil || b != "t-acme-orders" {
		t.Fatalf("bucketFor(acme,orders) = %q,%v want t-acme-orders", b, herr)
	}
	b2, herr := bucketFor("acme/../globex", "or ders")
	if herr != nil {
		t.Fatalf("unexpected err: %v", herr)
	}
	if strings.Contains(b2, "/") || strings.Contains(b2, "..") || strings.Contains(b2, " ") {
		t.Fatalf("sanitize failed: %q", b2)
	}
	if _, herr := bucketFor("", "orders"); herr == nil {
		t.Fatal("empty tenant should error")
	}
}
