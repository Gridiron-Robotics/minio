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
// Auth: shared-bearer caller gate via MCP_AUTH_TOKEN, constant-time compared.
// An UNSET token fails closed — /tools and /invoke answer 503 rather than
// serving an open surface that can read and delete every tenant's objects.
// Reopening it for local dev takes an explicit MCP_ALLOW_INSECURE=true and a
// loud boot warning. HEAD / stays unauthenticated so gateways can probe
// liveness, matching the estate MCP transport contract (see cx-automation
// CONTRACT.md).
//
// Tenant via X-Tenant-Id. Idempotency via Idempotency-Key, held in a bounded,
// optionally Redis-backed replay store (see replay.go).
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	otellog "go.opentelemetry.io/otel/log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const serverName = "minio"

// Replay-cache defaults. Bounded on both axes so the cache cannot grow without
// limit no matter the traffic shape (see replay.go).
const (
	defaultReplayMax = 10000
	defaultReplayTTL = 24 * time.Hour
)

// config is read once at boot from the environment.
type config struct {
	addr       string // MCP listen address
	authToken  string // MCP_AUTH_TOKEN — caller gate
	authDigest [32]byte
	authSet    bool
	// allowInsecure is the explicit, typed-out opt-in that lets the surface
	// serve WITHOUT a configured token. Absent it, an unset token fails closed.
	allowInsecure bool
	s3Endpoint    string // MINIO_ENDPOINT host:port (no scheme)
	s3AccessKey   string
	s3SecretKey   string
	s3Secure      bool
	presignExpiry time.Duration
	replayMax     int
	replayTTL     time.Duration
	replayRedis   string // MINIO_MCP_REPLAY_REDIS_URL — empty = in-memory only
}

func loadConfig() config {
	c := config{
		addr:          getenv("MINIO_MCP_ADDR", ":8090"),
		authToken:     os.Getenv("MCP_AUTH_TOKEN"),
		allowInsecure: truthy(os.Getenv("MCP_ALLOW_INSECURE")),
		s3Endpoint:    getenv("MINIO_ENDPOINT", "minio:9000"),
		s3AccessKey:   os.Getenv("MINIO_ACCESS_KEY"),
		s3SecretKey:   os.Getenv("MINIO_SECRET_KEY"),
		s3Secure:      truthy(os.Getenv("MINIO_SECURE")),
		presignExpiry: durationEnv("MINIO_PRESIGN_EXPIRY", 15*time.Minute),
		replayMax:     intEnv("MINIO_MCP_REPLAY_MAX", defaultReplayMax),
		replayTTL:     durationEnv("MINIO_MCP_REPLAY_TTL", defaultReplayTTL),
		replayRedis:   os.Getenv("MINIO_MCP_REPLAY_REDIS_URL"),
	}
	// A whitespace-only token is not a token. Treating it as one would be the
	// same fail-open this gate exists to prevent.
	if strings.TrimSpace(c.authToken) != "" {
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

	// replay is the idempotency store: bounded in-memory by default, durable
	// when MINIO_MCP_REPLAY_REDIS_URL is set. Which one is live is reported by
	// /healthz — see replay.go.
	replay replayStore
}

func newServer(cfg config) (*server, error) {
	mc, err := minio.New(cfg.s3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.s3AccessKey, cfg.s3SecretKey, ""),
		Secure: cfg.s3Secure,
	})
	if err != nil {
		return nil, err
	}
	s := &server{cfg: cfg, minio: mc, replay: newReplayStore(cfg)}
	s.tools = buildTools()
	s.byName = map[string]toolDef{}
	for _, t := range s.tools {
		s.byName[t.Name] = t
	}
	return s, nil
}

// newReplayStore picks the durable store when a Redis URL is configured and
// falls back to the bounded in-memory one otherwise. A Redis URL that does not
// parse is a configuration error worth shouting about, but it degrades to
// in-memory rather than refusing to serve storage — and /healthz then reports
// replay_durable=false so the downgrade is visible instead of assumed.
func newReplayStore(cfg config) replayStore {
	if cfg.replayRedis == "" {
		return newMemReplayStore(cfg.replayMax, cfg.replayTTL)
	}
	c, err := dialRedis(cfg.replayRedis)
	if err != nil {
		log.Printf("minio-mcp: MINIO_MCP_REPLAY_REDIS_URL is not usable (%v) — "+
			"idempotency replay falls back to in-memory and will NOT survive a restart", err)
		return newMemReplayStore(cfg.replayMax, cfg.replayTTL)
	}
	return newRedisReplayStore(c, cfg.replayTTL)
}

func main() {
	// Self-heal rail first, so even a boot failure reaches OpenObserve.
	shutdownObs := setupObservability(serverName)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownObs(ctx)
	}()

	cfg := loadConfig()
	s, err := newServer(cfg)
	if err != nil {
		emitError(context.Background(), "minio-mcp cannot init S3 client: "+err.Error(),
			otellog.String("phase", "boot"))
		log.Fatalf("minio-mcp: cannot init S3 client: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/tools", s.handleTools)
	mux.HandleFunc("/invoke", s.handleInvoke)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/", s.handleRoot)

	switch {
	case cfg.authSet:
		log.Printf("minio-mcp: listening on %s (auth: token-verify, replay durable=%v), s3=%s",
			cfg.addr, s.replay.Durable(), cfg.s3Endpoint)
	case cfg.allowInsecure:
		log.Printf("minio-mcp: WARNING listening on %s with NO caller authentication. "+
			"MCP_AUTH_TOKEN is unset and MCP_ALLOW_INSECURE=true was supplied, so any bearer "+
			"token is accepted on a surface that can read and delete every tenant's objects. "+
			"Never set MCP_ALLOW_INSECURE outside local development.", cfg.addr)
	default:
		log.Printf("minio-mcp: listening on %s FAIL-CLOSED — MCP_AUTH_TOKEN is unset, so "+
			"/tools and /invoke will answer 503 until it is configured "+
			"(set MCP_ALLOW_INSECURE=true to serve without auth in local dev)", cfg.addr)
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

	// Authoritative tenant from X-Tenant-Id (absent means "default"). Every
	// handler derives its bucket from (tenant, module) so a caller cannot
	// escape it.
	//
	// A malformed tenant is REFUSED, never repaired. Normalising it would risk
	// mapping one tenant's identity onto another's namespace — see
	// canonicalSegment in tools.go for the collapse this prevents.
	tenant := r.Header.Get("X-Tenant-Id")
	if strings.TrimSpace(tenant) == "" {
		tenant = "default"
	}
	canonTenant, ok := canonicalSegment(tenant)
	if !ok {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error": "invalid X-Tenant-Id: must be lowercase alphanumeric with interior dashes only " +
				"(no leading/trailing dash, max 40 chars)",
		})
		return
	}
	tenant = canonTenant

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Idempotent replay for mutating tools: same (tenant, key) returns the
	// stored result with "replayed":true instead of re-executing. The key is
	// tenant-scoped so one tenant's Idempotency-Key can never collide with —
	// or read back — another's stored result.
	idem := r.Header.Get("Idempotency-Key")
	replayKey := tenant + "\x00" + idem
	var release func()
	if idem != "" {
		if prev, hit := s.replay.Lookup(ctx, replayKey); hit {
			writeReplay(w, prev)
			return
		}
		// Single-flight within this process: if a concurrent request already
		// holds this key, wait for it and replay its result rather than
		// executing the mutation a second time.
		if mem, ok := s.replay.(*memReplayStore); ok {
			done, wait := mem.begin(replayKey)
			if wait != nil {
				select {
				case <-wait:
					if prev, hit := s.replay.Lookup(ctx, replayKey); hit {
						writeReplay(w, prev)
						return
					}
				case <-ctx.Done():
					writeErr(w, http.StatusGatewayTimeout, "timed out waiting on an in-flight request with the same Idempotency-Key")
					return
				}
			} else {
				release = done
				defer release()
			}
		}
	}

	result, herr := tool.Handler(ctx, s, tenant, args)
	if herr != nil {
		// A 5xx is a real fault in this service or its upstream, and is what
		// the OpenObserve level=error alert (and the langgraph self-heal loop)
		// exists to catch. 4xx is the caller's input being rejected as designed
		// — paging on those would be noise that trains everyone to ignore the
		// alert.
		if herr.status >= 500 {
			emitError(ctx, "minio-mcp tool failed: "+herr.msg,
				otellog.String("tool", tool.Name),
				otellog.String("tenant", tenant),
				otellog.Int("status", herr.status))
		}
		writeErr(w, herr.status, herr.msg)
		return
	}
	payload := map[string]any{"tool": tool.Name, "result": result}
	if idem != "" {
		s.replay.Remember(ctx, replayKey, map[string]any{"tool": tool.Name, "result": result})
	}
	writeJSON(w, http.StatusOK, payload)
}

// writeReplay emits a stored payload marked as a replay.
func writeReplay(w http.ResponseWriter, prev map[string]any) {
	payload := map[string]any{"replayed": true}
	for k, v := range prev {
		payload[k] = v
	}
	writeJSON(w, http.StatusOK, payload)
}

// ---- auth ------------------------------------------------------------------

// authOK gates /tools and /invoke. Returns false (and writes the refusal) when
// the caller is not authenticated. HEAD / and /healthz never call this.
//
// With no configured token the surface fails CLOSED. It previously accepted any
// non-empty bearer, which on a flat network is every container in the cluster,
// in front of tools that can list, read and delete every tenant's objects — so
// a deploy that simply forgot to set MCP_AUTH_TOKEN was silently world-open.
// The 503 names the SERVER's misconfiguration rather than implying the caller's
// credential was wrong, because the caller did nothing incorrect.
func (s *server) authOK(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.authSet && !s.cfg.allowInsecure {
		writeErr(w, http.StatusServiceUnavailable,
			"minio-mcp is not configured for authenticated access: MCP_AUTH_TOKEN is unset. "+
				"Refusing to serve object-storage tools. Set MCP_AUTH_TOKEN, or set "+
				"MCP_ALLOW_INSECURE=true for local development only.")
		return false
	}
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
	if s.cfg.authSet {
		got := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(got[:], s.cfg.authDigest[:]) != 1 {
			writeErr(w, http.StatusUnauthorized, "invalid bearer token")
			return false
		}
	}
	return true
}

// handleHealthz reports posture without requiring a credential: whether the
// caller gate is enforcing, and whether idempotency replay actually survives a
// restart. A silent downgrade to the in-memory store is indistinguishable from
// durability unless it is reported, so it is reported.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	authMode := "token-verify"
	switch {
	case !s.cfg.authSet && s.cfg.allowInsecure:
		authMode = "insecure-explicitly-allowed"
	case !s.cfg.authSet:
		authMode = "fail-closed"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"server":         serverName,
		"auth":           authMode,
		"replay_durable": s.replay.Durable(),
	})
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

func intEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
