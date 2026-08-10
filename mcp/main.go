// Command minio-mcp is the estate MCP sidecar for the shared MinIO object
// store. It speaks the Gridiron "Contract A" HTTP surface
// (erp_django_middleware/docs/MCP_STANDARD.md) so the LangGraph brain and any
// module can list + invoke storage tools uniformly — identical in shape to the
// openproject / pos / gdc MCP servers.
//
// It does NOT modify or wrap the MinIO engine's own S3 API; it is a thin,
// separate binary that talks S3 to MinIO with a scoped service account and
// re-exposes a curated tool set. Every object operation is forced into the
// caller's tenant namespace (bucket = "t-<tenant>-<module>") so a caller can
// never name a foreign tenant's bucket.
//
// Surface:
//
//	GET  /tools?server=minio   -> {"tools":[{name,description,input_schema,annotations}]}
//	POST /invoke               -> {"tool","result"[,"replayed":true]}
//	HEAD /                     -> 200 (health; unauthenticated so gateways can probe)
//
// Auth: shared-bearer caller gate via MCP_AUTH_TOKEN, verified constant-time.
// The token is REQUIRED: with it unset the server refuses to boot (exit 78,
// EX_CONFIG) unless MINIO_MCP_ALLOW_INSECURE=1, which starts a deny-all surface
// where every /tools and /invoke call answers 401. Tenant via X-Tenant-Id.
// Idempotency via Idempotency-Key (per-tenant replay cache).
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const serverName = "minio"

// allowInsecureVar opts a local-dev run out of the boot refusal when
// MCP_AUTH_TOKEN is unset. The server still authenticates no one: /tools and
// /invoke answer 401 for every caller. Never set it in staging/prod.
const allowInsecureVar = "MINIO_MCP_ALLOW_INSECURE"

// config is read once at boot from the environment.
type config struct {
	addr          string // MCP listen address
	authToken     string // MCP_AUTH_TOKEN — required caller gate
	authDigest    [32]byte
	authSet       bool
	allowInsecure bool   // MINIO_MCP_ALLOW_INSECURE — boot tokenless, deny everyone
	s3Endpoint    string // MINIO_ENDPOINT host:port (no scheme)
	s3AccessKey   string
	s3SecretKey   string
	s3Secure      bool
	presignExpiry time.Duration
}

func loadConfig() config {
	c := config{
		addr:          getenv("MINIO_MCP_ADDR", ":8090"),
		authToken:     os.Getenv("MCP_AUTH_TOKEN"),
		allowInsecure: truthy(os.Getenv(allowInsecureVar)),
		s3Endpoint:    getenv("MINIO_ENDPOINT", "minio:9000"),
		s3AccessKey:   os.Getenv("MINIO_ACCESS_KEY"),
		s3SecretKey:   os.Getenv("MINIO_SECRET_KEY"),
		s3Secure:      truthy(os.Getenv("MINIO_SECURE")),
		presignExpiry: durationEnv("MINIO_PRESIGN_EXPIRY", 15*time.Minute),
	}
	if c.authToken != "" {
		c.authDigest = sha256.Sum256([]byte(c.authToken))
		c.authSet = true
	}
	return c
}

// server holds the live dependencies.
type server struct {
	cfg    config
	minio  *minio.Client
	tools  []toolDef
	byName map[string]toolDef

	replayMu sync.Mutex
	replay   map[string]map[string]any // (tenant\x00key) -> stored payload
}

func newServer(cfg config) (*server, error) {
	mc, err := minio.New(cfg.s3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.s3AccessKey, cfg.s3SecretKey, ""),
		Secure: cfg.s3Secure,
	})
	if err != nil {
		return nil, err
	}
	s := &server{cfg: cfg, minio: mc, replay: map[string]map[string]any{}}
	s.tools = buildTools()
	s.byName = map[string]toolDef{}
	for _, t := range s.tools {
		s.byName[t.Name] = t
	}
	return s, nil
}

// bootRefusal reports why the process must not start, or "" when it may.
// An unset MCP_AUTH_TOKEN is fatal because the tools behind the gate
// (put_object, delete_object, presign_put, ensure_bucket) reach every tenant
// bucket this sidecar's service account can see: booting without a caller gate
// would hand blob write+delete to anyone who can reach the port.
func bootRefusal(cfg config) string {
	if cfg.authSet || cfg.allowInsecure {
		return ""
	}
	return "minio-mcp: refusing to start: MCP_AUTH_TOKEN is not set. Set it (and " +
		"match the brain's GATEWAY_TOKENS[minio-mcp]), or set " + allowInsecureVar +
		"=1 for a local-dev run that authenticates no one."
}

func main() {
	cfg := loadConfig()
	if msg := bootRefusal(cfg); msg != "" {
		log.Print(msg)
		os.Exit(78) // EX_CONFIG — same refusal as the other estate MCP servers.
	}
	s, err := newServer(cfg)
	if err != nil {
		log.Fatalf("minio-mcp: cannot init S3 client: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/tools", s.handleTools)
	mux.HandleFunc("/invoke", s.handleInvoke)
	mux.HandleFunc("/", s.handleRoot)

	if cfg.authSet {
		log.Printf("minio-mcp: listening on %s (auth: token-verify), s3=%s", cfg.addr, cfg.s3Endpoint)
	} else {
		log.Printf("minio-mcp: listening on %s (auth: DENY-ALL — %s=1 with no MCP_AUTH_TOKEN; /tools and /invoke answer 401), s3=%s", cfg.addr, allowInsecureVar, cfg.s3Endpoint)
	}
	srv := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// ---- Contract A handlers ---------------------------------------------------

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	// HEAD / is the unauthenticated health probe (gateways use it tokenlessly).
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	writeErr(w, http.StatusNotFound, "not found")
}

func (s *server) handleTools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	if !s.authOK(w, r) {
		return
	}
	// Unknown ?server= is an empty list, NOT a 404 (platform treats it as "no tools").
	if sv := r.URL.Query().Get("server"); sv != "" && sv != serverName {
		writeJSON(w, http.StatusOK, map[string]any{"tools": []any{}})
		return
	}
	out := make([]map[string]any, 0, len(s.tools))
	for _, t := range s.tools {
		out = append(out, map[string]any{
			"name":         t.Name,
			"description":  t.Description,
			"input_schema": t.InputSchema,
			"annotations":  map[string]any{"destructiveHint": t.Destructive},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tools": out})
}

type invokeBody struct {
	Server    string         `json:"server"`
	Tool      string         `json:"tool"`
	Arguments map[string]any `json:"arguments"`
	Args      map[string]any `json:"args"` // alias, matches the estate reference impls
}

func (s *server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	if !s.authOK(w, r) {
		return
	}
	var body invokeBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Server != "" && body.Server != serverName {
		writeErr(w, http.StatusNotFound, "unknown server: "+body.Server)
		return
	}
	if body.Tool == "" {
		writeErr(w, http.StatusBadRequest, "tool is required")
		return
	}
	tool, ok := s.byName[body.Tool]
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown tool: "+body.Tool)
		return
	}
	args := body.Arguments
	if args == nil {
		args = body.Args
	}
	if args == nil {
		args = map[string]any{}
	}
	if missing := missingRequired(tool.InputSchema, args); len(missing) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error": "schema validation failed", "errors": missing,
		})
		return
	}

	// Authoritative tenant from X-Tenant-Id (default "default"). Every handler
	// derives its bucket from (tenant, module) so a caller cannot escape it.
	tenant := r.Header.Get("X-Tenant-Id")
	if tenant == "" {
		tenant = "default"
	}
	tenant = sanitizeSegment(tenant)

	// Idempotent replay for mutating tools: same (tenant, key) returns the
	// stored result with "replayed":true instead of re-executing.
	idem := r.Header.Get("Idempotency-Key")
	replayKey := tenant + "\x00" + idem
	if idem != "" {
		s.replayMu.Lock()
		if prev, hit := s.replay[replayKey]; hit {
			s.replayMu.Unlock()
			payload := map[string]any{"replayed": true}
			for k, v := range prev {
				payload[k] = v
			}
			writeJSON(w, http.StatusOK, payload)
			return
		}
		s.replayMu.Unlock()
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	result, herr := tool.Handler(ctx, s, tenant, args)
	if herr != nil {
		writeErr(w, herr.status, herr.msg)
		return
	}
	payload := map[string]any{"tool": tool.Name, "result": result}
	if idem != "" {
		s.replayMu.Lock()
		s.replay[replayKey] = map[string]any{"tool": tool.Name, "result": result}
		s.replayMu.Unlock()
	}
	writeJSON(w, http.StatusOK, payload)
}

// ---- auth ------------------------------------------------------------------

// authOK gates /tools and /invoke. Returns false (and writes 401) when the
// caller is not authenticated. With no MCP_AUTH_TOKEN configured it denies
// everyone rather than falling open. HEAD / never calls this.
func (s *server) authOK(w http.ResponseWriter, r *http.Request) bool {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return false
	}
	token := strings.TrimSpace(h[len(p):])
	if token == "" {
		writeErr(w, http.StatusUnauthorized, "missing bearer token")
		return false
	}
	if !s.cfg.authSet {
		writeErr(w, http.StatusUnauthorized,
			"MCP_AUTH_TOKEN is not configured; this server authenticates no one")
		return false
	}
	got := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(got[:], s.cfg.authDigest[:]) != 1 {
		writeErr(w, http.StatusUnauthorized, "invalid bearer token")
		return false
	}
	return true
}

// ---- helpers ---------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func truthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func durationEnv(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
