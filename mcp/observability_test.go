package main

// Self-heal rail tests.
//
// The estate alert rule fires on `level=error` records in this module's
// OpenObserve stream, and that alert is what drives the langgraph diagnose →
// fix→PR loop. These tests pin the three traps that make an OTLP setup LOOK
// instrumented while paging nobody. See observability.go for the full write-up.

import (
	"context"
	"encoding/base64"
	"testing"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.opentelemetry.io/otel/log/global"
)

// ---- capture harness -------------------------------------------------------

type capturedRecord struct {
	severity     otellog.Severity
	severityText string
	body         string
	attrs        map[string]string
}

type captureLogger struct {
	embedded.Logger
	sink *[]capturedRecord
}

func (c *captureLogger) Emit(_ context.Context, r otellog.Record) {
	rec := capturedRecord{
		severity:     r.Severity(),
		severityText: r.SeverityText(),
		body:         r.Body().AsString(),
		attrs:        map[string]string{},
	}
	r.WalkAttributes(func(kv otellog.KeyValue) bool {
		rec.attrs[kv.Key] = kv.Value.AsString()
		return true
	})
	*c.sink = append(*c.sink, rec)
}

func (c *captureLogger) Enabled(context.Context, otellog.EnabledParameters) bool { return true }

type captureProvider struct {
	embedded.LoggerProvider
	sink *[]capturedRecord
}

func (c *captureProvider) Logger(string, ...otellog.LoggerOption) otellog.Logger {
	return &captureLogger{sink: c.sink}
}

// captureLogs swaps in a capturing provider for the duration of a test.
func captureLogs(t *testing.T) *[]capturedRecord {
	t.Helper()
	sink := &[]capturedRecord{}
	prev := global.GetLoggerProvider()
	global.SetLoggerProvider(&captureProvider{sink: sink})
	t.Cleanup(func() { global.SetLoggerProvider(prev) })
	return sink
}

// ---- the rail itself -------------------------------------------------------

// Trap #1: OpenObserve derives the `level` field the alert matches on from
// severity_TEXT. A record with only a numeric severity satisfies every OTel
// assertion and is invisible to the alert.
func TestEmitErrorSetsSeverityText(t *testing.T) {
	sink := captureLogs(t)
	emitError(context.Background(), "something broke")

	if len(*sink) != 1 {
		t.Fatalf("expected exactly 1 record, got %d", len(*sink))
	}
	rec := (*sink)[0]
	if rec.severityText != "ERROR" {
		t.Fatalf("severity_text = %q, want \"ERROR\" — OpenObserve reads `level` from the text, "+
			"so an empty one never matches the level=error alert", rec.severityText)
	}
	if rec.severity != otellog.SeverityError {
		t.Fatalf("severity = %v, want SeverityError", rec.severity)
	}
	if rec.body != "something broke" {
		t.Fatalf("body = %q", rec.body)
	}
}

// A real runtime failure — not a synthetic call — must reach the rail. This is
// trap #2 in practice: it exercises the actual handler error path rather than
// asserting that the emit helper works in isolation.
func TestUpstreamFailureEmitsErrorRecord(t *testing.T) {
	sink := captureLogs(t)
	s, fake := s3TestServer(t)
	fake.failObjectOps()

	code, _ := invoke(t, s, "acme", "",
		`{"tool":"put_object","arguments":{"module":"orders","key":"k","content_base64":"aGk="}}`)
	if code != 502 {
		t.Fatalf("expected the upstream failure to surface as 502, got %d", code)
	}
	if len(*sink) == 0 {
		t.Fatal("a 5xx tool failure emitted no OTLP record — it would never reach OpenObserve, " +
			"so the self-heal loop would never fire")
	}
	rec := (*sink)[0]
	if rec.severityText != "ERROR" {
		t.Fatalf("severity_text = %q, want ERROR", rec.severityText)
	}
	// The incident needs enough context to route and diagnose.
	if rec.attrs["tool"] != "put_object" {
		t.Errorf("record should carry the failing tool, got attrs %v", rec.attrs)
	}
	if rec.attrs["tenant"] != "acme" {
		t.Errorf("record should carry the tenant, got attrs %v", rec.attrs)
	}
}

// The mirror: a 4xx is the caller's input being rejected as designed. Paging on
// those is noise that trains people to ignore the alert.
func TestClientErrorsDoNotPage(t *testing.T) {
	sink := captureLogs(t)
	s, _ := s3TestServer(t)

	// 422 — schema validation.
	invoke(t, s, "acme", "", `{"tool":"stat_object","arguments":{"module":"orders"}}`)
	// 422 — refused tenant.
	invoke(t, s, "../globex", "", `{"tool":"stat_object","arguments":{"module":"orders","key":"k"}}`)
	// 404 — object missing.
	invoke(t, s, "acme", "", `{"tool":"stat_object","arguments":{"module":"orders","key":"nope"}}`)

	if len(*sink) != 0 {
		t.Fatalf("4xx responses should not emit error records, got %d: %v", len(*sink), *sink)
	}
}

// ---- env contract ----------------------------------------------------------

// Trap #3: base64 credentials end in "=" padding. Splitting a header pair on
// every "=" truncates the credential and produces a 401 that reads like a
// permissions problem rather than a parsing bug.
func TestParseOTLPHeadersPreservesBase64Padding(t *testing.T) {
	cred := base64.StdEncoding.EncodeToString([]byte("ops@gridironrobotics.com:s3cr3t"))
	if !hasSuffixEq(cred) {
		t.Fatalf("fixture is not exercising the trap: %q has no = padding", cred)
	}
	raw := "Authorization=Basic " + cred + ",organization=gridiron"

	got := parseOTLPHeaders(raw)
	if want := "Basic " + cred; got["Authorization"] != want {
		t.Fatalf("Authorization = %q, want %q — the = padding must survive parsing", got["Authorization"], want)
	}
	if got["organization"] != "gridiron" {
		t.Fatalf("organization = %q, want gridiron", got["organization"])
	}
}

func TestParseOTLPHeadersEdgeCases(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		want map[string]string
	}{
		"empty":            {"", map[string]string{}},
		"whitespace":       {"   ", map[string]string{}},
		"no equals":        {"garbage", map[string]string{}},
		"trailing comma":   {"a=1,", map[string]string{"a": "1"}},
		"spaces around":    {" a = 1 , b = 2 ", map[string]string{"a": "1", "b": "2"}},
		"empty key":        {"=novalue", map[string]string{}},
		"value with equal": {"k=a=b=c", map[string]string{"k": "a=b=c"}},
	} {
		t.Run(name, func(t *testing.T) {
			got := parseOTLPHeaders(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("parseOTLPHeaders(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("parseOTLPHeaders(%q)[%q] = %q, want %q", tc.in, k, got[k], v)
				}
			}
		})
	}
}

// With the env unset the rail must be a complete no-op: no exporter, no boot
// dependency, no cost in tests or local runs.
func TestObservabilityDisabledByDefault(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	if otelEnabled() {
		t.Fatal("observability must be off when neither OTEL_ENABLED nor an endpoint is set")
	}
	if err := setupObservability("minio")(context.Background()); err != nil {
		t.Fatalf("disabled setup should return a no-op shutdown, got %v", err)
	}
}

// Either switch turns it on — matching the Python drop-in's contract.
func TestObservabilityEnabledBySwitchOrEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_ENABLED", "1")
	if !otelEnabled() {
		t.Error("OTEL_ENABLED=1 should enable")
	}
	t.Setenv("OTEL_ENABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://openobserve:5080/api/gridiron/v1")
	if !otelEnabled() {
		t.Error("a configured endpoint should enable")
	}
}

// The service name is the OpenObserve stream and the incident's module name;
// they must be the same string or the alert routes to nothing.
func TestServiceNameMatchesStreamName(t *testing.T) {
	if serverName != "minio" {
		t.Fatalf("serverName = %q — service.name, the OpenObserve stream and the alert's "+
			"module name must all agree; changing one requires changing the alert rule", serverName)
	}
}

func hasSuffixEq(s string) bool { return len(s) > 0 && s[len(s)-1] == '=' }
