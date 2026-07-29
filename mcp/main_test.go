package main

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
	return testServerCfg(t, cfg)
}

func testServerCfg(t *testing.T, cfg config) *server {
	t.Helper()
	s, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	return s
}

func do(s *server, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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

// TestUnsetTokenFailsClosed replaces an earlier test that asserted the OPPOSITE
// — that an unset MCP_AUTH_TOKEN accepted any non-empty bearer. That behaviour
// was the defect: on a flat network "any bearer" is every container in the
// cluster, standing in front of tools that list, read and delete every tenant's
// objects. The old test did not catch the bug because it encoded it, so the
// test had to change with the code.
func TestUnsetTokenFailsClosed(t *testing.T) {
	s := testServer(t, "")
	for _, tc := range []struct{ name, token string }{
		{"no bearer", ""},
		{"arbitrary bearer", "anything"},
		{"plausible-looking bearer", "Bearer-ish-token-value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if rec := do(s, http.MethodGet, "/tools", tc.token, ""); rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("/tools with %s -> %d, want 503 (fail-closed)", tc.name, rec.Code)
			}
			rec := do(s, http.MethodPost, "/invoke", tc.token, `{"tool":"list_buckets","arguments":{}}`)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("/invoke with %s -> %d, want 503 (fail-closed)", tc.name, rec.Code)
			}
		})
	}
	// The refusal must name the server's misconfiguration, not blame the
	// caller's credential — the caller did nothing wrong.
	body := do(s, http.MethodGet, "/tools", "anything", "").Body.String()
	if !strings.Contains(body, "MCP_AUTH_TOKEN") {
		t.Fatalf("503 body should name MCP_AUTH_TOKEN, got %s", body)
	}
}

// A whitespace-only token is not a token; treating it as one would reopen the
// exact hole the fail-closed gate exists to shut.
func TestWhitespaceTokenIsNotAToken(t *testing.T) {
	os.Setenv("MCP_AUTH_TOKEN", "   ")
	defer os.Unsetenv("MCP_AUTH_TOKEN")
	if cfg := loadConfig(); cfg.authSet {
		t.Fatal("a whitespace-only MCP_AUTH_TOKEN must not count as configured auth")
	}
}

// Health must stay reachable while the surface refuses tool traffic, so an
// orchestrator can tell "misconfigured" apart from "down".
func TestHealthReachableWhileFailClosed(t *testing.T) {
	s := testServer(t, "")
	if rec := do(s, http.MethodHead, "/", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("HEAD / while fail-closed -> %d, want 200", rec.Code)
	}
	rec := httptest.NewRecorder()
	s.handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz -> %d, want 200", rec.Code)
	}
	var out struct {
		Auth          string `json:"auth"`
		ReplayDurable bool   `json:"replay_durable"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if out.Auth != "fail-closed" {
		t.Fatalf("healthz auth = %q, want fail-closed", out.Auth)
	}
	// In-memory replay must be advertised as NOT durable — a silent downgrade
	// looks identical to durability unless it is reported.
	if out.ReplayDurable {
		t.Fatal("in-memory replay must report replay_durable=false")
	}
}

// The escape hatch has to exist for local dev, but only when typed out.
func TestAllowInsecureReopensExplicitly(t *testing.T) {
	s := testServerCfg(t, config{s3Endpoint: "minio:9000", s3AccessKey: "x", s3SecretKey: "y", allowInsecure: true})
	if rec := do(s, http.MethodGet, "/tools", "anything", ""); rec.Code != http.StatusOK {
		t.Fatalf("MCP_ALLOW_INSECURE: any bearer -> %d, want 200", rec.Code)
	}
	// Even wide open, a request with NO bearer at all is still refused.
	if rec := do(s, http.MethodGet, "/tools", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("MCP_ALLOW_INSECURE: no bearer -> %d, want 401", rec.Code)
	}
}

func TestBucketForTenantIsolation(t *testing.T) {
	b, herr := bucketFor("acme", "orders")
	if herr != nil || b != "t-acme-orders" {
		t.Fatalf("bucketFor(acme,orders) = %q,%v want t-acme-orders", b, herr)
	}
	// Case is normalised; that is safe because it cannot change which tenant
	// is addressed.
	if b, herr := bucketFor("ACME", "Orders"); herr != nil || b != "t-acme-orders" {
		t.Fatalf("bucketFor(ACME,Orders) = %q,%v want t-acme-orders", b, herr)
	}

	// Malformed segments are REFUSED, not repaired. This assertion previously
	// accepted repair — it only checked that the mangled result contained no
	// "/", ".." or " " — which let the collapse bug through: the old sanitiser
	// turned "../globex" into exactly "globex", a different real tenant.
	for _, bad := range []string{
		"", "   ", "acme/../globex", "../globex", "..globex", "/globex/", "___globex",
		"or ders", "-leading", "trailing-", "acme_corp", "acme.corp",
		strings.Repeat("a", 41),
	} {
		if got, herr := bucketFor(bad, "orders"); herr == nil {
			t.Errorf("bucketFor(%q,orders) = %q with no error; malformed tenants must be refused", bad, got)
		}
		if got, herr := bucketFor("acme", bad); herr == nil {
			t.Errorf("bucketFor(acme,%q) = %q with no error; malformed modules must be refused", bad, got)
		}
	}
}

// tenantPrefix must fail closed on a malformed tenant: a prefix that matches
// everything would turn list_buckets into a cross-tenant enumeration.
func TestTenantPrefixFailsClosed(t *testing.T) {
	if got := tenantPrefix("acme"); got != "t-acme-" {
		t.Fatalf("tenantPrefix(acme) = %q", got)
	}
	for _, bad := range []string{"", "../globex", "-x-"} {
		got := tenantPrefix(bad)
		if got == "t--" || got == "t-" || got == "" {
			t.Fatalf("tenantPrefix(%q) = %q — a permissive prefix enumerates other tenants", bad, got)
		}
		if strings.HasPrefix("t-globex-orders", got) {
			t.Fatalf("tenantPrefix(%q) = %q still matches another tenant's bucket", bad, got)
		}
	}
}

// sha256Sum is a small helper so test configs can be built without importing
// crypto/sha256 in every test file.
func sha256Sum(s string) [32]byte { return sha256.Sum256([]byte(s)) }
