package main

// gridiron_otel, Go edition — the estate OpenObserve/OTLP self-heal drop-in.
//
// This is the Go counterpart of
// langgraph-agents/deploy/observability/dropin/gridiron_otel.py. It reads the
// same env contract and ships the service's ERROR logs to OpenObserve over
// OTLP/HTTP. The estate alert rule fires on any `level=error` record in the
// module's stream, so an error here becomes an OpenObserve alert, which drives
// the langgraph self-heal loop (/webhooks/openobserve/alert → diagnose →
// fix→PR / runtime remediation).
//
// Contract (identical to the Python drop-in):
//
//	OTEL_ENABLED=1                       # master switch (also on if endpoint set)
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://openobserve:5080/api/<org>/v1
//	OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic <base64 email:password>
//	OTEL_SERVICE_NAME=<module>           # == the OpenObserve stream == incident 'module'
//
// It is env-gated and fully graceful: with the env unset every call no-ops and
// nothing is exported, so local runs and tests cost nothing.
//
// Three traps this estate has already paid for elsewhere, each pinned by a test
// in observability_test.go:
//
//  1. severity_text MUST be populated. OpenObserve fills `level` from the text,
//     not from the numeric severity — a record carrying only severity_number=17
//     satisfies every OTel assertion and still never matches a level=error alert.
//  2. Spans are not logs. Wiring traces alone looks instrumented and pages
//     nobody; the LOG pipeline is what the alert rule reads.
//  3. OTEL_EXPORTER_OTLP_HEADERS values contain "=". Every base64 credential
//     with padding does. The separator between headers is ",", and the split on
//     "=" must be on the FIRST one only or the padding is destroyed and auth
//     fails with a confusing 401.

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	obsOnce     sync.Once
	obsProvider *sdklog.LoggerProvider
)

// otelEnabled reports whether observability should initialise: the master
// switch, or simply having an endpoint configured.
func otelEnabled() bool {
	if truthy(os.Getenv("OTEL_ENABLED")) {
		return true
	}
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
}

// parseOTLPHeaders parses the W3C-style header list used by the OTLP env
// contract: comma-separated `key=value` pairs.
//
// It splits each pair on the FIRST "=" only. Splitting on every "=" is the
// classic failure here: a base64 Authorization value ends in "=" padding, and
// naive splitting truncates the credential into an invalid one, producing a 401
// that looks like a permissions problem rather than a parsing bug.
func parseOTLPHeaders(raw string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, found := strings.Cut(pair, "=") // Cut splits on the first "=" only
		if !found {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" {
			continue
		}
		out[k] = v
	}
	return out
}

// setupObservability initialises the OTLP log pipeline for serviceName, which
// must equal the OpenObserve stream and the incident's module name. Safe to
// call unconditionally; it no-ops when the env is unset. Returns a shutdown
// func that flushes pending records.
func setupObservability(serviceName string) func(context.Context) error {
	noop := func(context.Context) error { return nil }
	if !otelEnabled() {
		return noop
	}

	var shutdown func(context.Context) error = noop
	obsOnce.Do(func() {
		endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
		if endpoint == "" {
			return
		}
		if name := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); name != "" {
			serviceName = name
		}

		opts := []otlploghttp.Option{}
		if hdrs := parseOTLPHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")); len(hdrs) > 0 {
			opts = append(opts, otlploghttp.WithHeaders(hdrs))
		}
		// The SDK reads OTEL_EXPORTER_OTLP_ENDPOINT itself; passing the URL
		// explicitly keeps behaviour identical whether or not the env var is
		// inherited by a child process.
		u := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
		host, path, _ := strings.Cut(u, "/")
		opts = append(opts, otlploghttp.WithEndpoint(host))
		if path != "" {
			opts = append(opts, otlploghttp.WithURLPath("/"+strings.Trim(path, "/")+"/logs"))
		}
		if strings.HasPrefix(endpoint, "http://") {
			opts = append(opts, otlploghttp.WithInsecure())
		}

		exp, err := otlploghttp.New(context.Background(), opts...)
		if err != nil {
			// Observability must never be a boot dependency — degrade to
			// stdout-only rather than taking the storage surface down.
			log.Printf("minio-mcp: OTLP log exporter unavailable (%v) — errors will NOT reach OpenObserve", err)
			return
		}
		res, _ := resource.Merge(resource.Default(),
			resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)))
		obsProvider = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		)
		global.SetLoggerProvider(obsProvider)
		shutdown = func(ctx context.Context) error { return obsProvider.Shutdown(ctx) }
		log.Printf("minio-mcp: OTLP logs -> %s (service.name=%s)", endpoint, serviceName)
	})
	return shutdown
}

// emitError ships one ERROR-severity record to the OpenObserve stream. This is
// the call that turns a runtime failure into a self-heal incident.
//
// SeverityText is set explicitly and is not optional: OpenObserve derives the
// `level` field the alert rule matches on from the TEXT, so a record with only
// a numeric severity is invisible to the alert.
func emitError(ctx context.Context, msg string, attrs ...otellog.KeyValue) {
	logger := global.GetLoggerProvider().Logger(serverName)
	var rec otellog.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(otellog.SeverityError)
	rec.SetSeverityText("ERROR") // trap #1 — see the file comment
	rec.SetBody(otellog.StringValue(msg))
	rec.AddAttributes(attrs...)
	logger.Emit(ctx, rec)
}
