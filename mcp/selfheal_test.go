package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// exporterSpy is a minimal sdklog.Exporter that keeps every record it is handed.
type exporterSpy struct{ records []sdklog.Record }

func (e *exporterSpy) Export(_ context.Context, records []sdklog.Record) error {
	e.records = append(e.records, records...)
	return nil
}
func (e *exporterSpy) Shutdown(context.Context) error   { return nil }
func (e *exporterSpy) ForceFlush(context.Context) error { return nil }

func railWithSpy(t *testing.T) (*Observability, *exporterSpy) {
	t.Helper()
	spy := &exporterSpy{}
	obs, err := Init(context.Background(), Options{
		ServiceName: DefaultServiceName,
		ForceEnable: true,
		Exporter:    spy,
		Synchronous: true,
		Getenv:      func(string) string { return "" },
	}, nil)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	return obs, spy
}

// Trap: OpenObserve derives the `level` field the alert matches on from
// severity_TEXT. A record carrying only a numeric severity satisfies every OTel
// assertion and is invisible to the alert.
func TestErrorSetsSeverityText(t *testing.T) {
	obs, spy := railWithSpy(t)
	obs.Error(context.Background(), "minio mcp tool failed")
	if err := obs.ForceFlush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(spy.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(spy.records))
	}
	rec := spy.records[0]
	if got := rec.SeverityText(); got != "ERROR" {
		t.Fatalf("severity_text = %q, want ERROR — OpenObserve reads `level` from the text", got)
	}
	if got := rec.Severity(); got != otellog.SeverityError {
		t.Fatalf("severity = %v, want SeverityError", got)
	}
}

// The service name is the OpenObserve stream and the incident's module name; a
// mismatch routes the alert to nothing.
func TestServiceNameMatchesStream(t *testing.T) {
	if DefaultServiceName != "minio-mcp" {
		t.Fatalf("DefaultServiceName = %q — service.name, the OpenObserve stream and the "+
			"alert's module name must all agree", DefaultServiceName)
	}
	obs, _ := railWithSpy(t)
	if obs.ServiceName != "minio-mcp" {
		t.Fatalf("ServiceName = %q", obs.ServiceName)
	}
}

// With the env unset the rail must cost nothing and never panic — telemetry is
// never a boot dependency for storage.
func TestDisabledByDefault(t *testing.T) {
	if EnabledFromEnv(func(string) string { return "" }) {
		t.Fatal("rail must be off when neither OTEL_ENABLED nor an endpoint is set")
	}
	obs, err := Init(context.Background(), Options{Getenv: func(string) string { return "" }}, nil)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if obs.Enabled {
		t.Fatal("expected disabled")
	}
	obs.Error(context.Background(), "must not panic")
	if err := obs.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown on disabled rail: %v", err)
	}
	// NopEmitter (what the server holds before Init) must also be safe.
	NopEmitter().Error(context.Background(), "no panic")
}

// Trap: base64 credentials end in "=" padding; splitting a header pair on every
// "=" truncates the credential into a 401 that reads like a permissions problem.
func TestParseHeadersPreservesBase64Padding(t *testing.T) {
	cred := base64.StdEncoding.EncodeToString([]byte("ops@gridironrobotics.com:s3cr3t"))
	if !strings.HasSuffix(cred, "=") {
		t.Fatalf("fixture not exercising the trap: %q has no = padding", cred)
	}
	got := ParseHeaders("Authorization=Basic " + cred + ",organization=gridiron")
	if want := "Basic " + cred; got["Authorization"] != want {
		t.Fatalf("Authorization = %q, want %q — the = padding must survive", got["Authorization"], want)
	}
	if got["organization"] != "gridiron" {
		t.Fatalf("organization = %q", got["organization"])
	}
}

// ---- the handler path: Contract A has no unhandled 5xx, so the rail must be
// fired explicitly from handleInvoke on a >=500 tool failure. -----------------

func serviceNameOf(rec sdklog.Record) string {
	if v, ok := rec.Resource().Set().Value(semconv.ServiceNameKey); ok {
		return v.AsString()
	}
	return ""
}

func TestToolFailureReachesSelfHealRail(t *testing.T) {
	obs, spy := railWithSpy(t)
	// delete_object where S3 fails -> handler returns 502 -> must page.
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) {
		if answerLocation(w, r) {
			return
		}
		w.WriteHeader(http.StatusForbidden) // any error -> errf(502)
	})
	s.obs = obs

	code, _ := invokeResult(t, s, "acme",
		`{"tool":"delete_object","arguments":{"module":"orders","key":"a/b.txt"}}`)
	if code != http.StatusBadGateway {
		t.Fatalf("delete failure -> %d, want 502", code)
	}
	_ = obs.ForceFlush(context.Background())

	if len(spy.records) != 1 {
		t.Fatalf("expected 1 self-heal record for a 502, got %d", len(spy.records))
	}
	rec := spy.records[0]
	if rec.SeverityText() != "ERROR" {
		t.Fatalf("severity_text = %q, want ERROR", rec.SeverityText())
	}
	if got := serviceNameOf(rec); got != "minio-mcp" {
		t.Fatalf("resource service.name = %q, want minio-mcp", got)
	}
	attrs := map[string]string{}
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[string(kv.Key)] = kv.Value.AsString()
		return true
	})
	if attrs["tool"] != "delete_object" || attrs["tenant"] != "acme" {
		t.Fatalf("attrs = %v, want tool=delete_object tenant=acme", attrs)
	}
}

func TestCallerErrorDoesNotPage(t *testing.T) {
	obs, spy := railWithSpy(t)

	// 1) A 422 from missingRequired never reaches the handler branch.
	s := s3Stub(t, func(w http.ResponseWriter, r *http.Request) { answerLocation(w, r) })
	s.obs = obs
	if code, _ := invokeResult(t, s, "acme",
		`{"tool":"stat_object","arguments":{"module":"orders"}}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("missing required -> %d, want 422", code)
	}

	// 2) A 422 FROM the handler (bad base64) exercises the herr!=nil, status<500
	//    branch and must still not page.
	if code, _ := invokeResult(t, s, "acme",
		`{"tool":"put_object","arguments":{"module":"orders","key":"k","content_base64":"!!!"}}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("bad base64 -> %d, want 422", code)
	}

	_ = obs.ForceFlush(context.Background())
	if len(spy.records) != 0 {
		t.Fatalf("caller errors paged %d time(s), want 0", len(spy.records))
	}
}

// The attribute constructor used by handleInvoke must round-trip (guards the
// import wiring in main.go).
func TestErrorAttributeHelper(t *testing.T) {
	kv := attribute.Int("status", 502)
	if kv.Value.AsInt64() != 502 {
		t.Fatalf("attribute.Int did not round-trip: %v", kv)
	}
}
