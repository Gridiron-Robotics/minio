// selfheal.go wires the estate OpenObserve/OTLP self-heal rail for this sidecar.
//
// Go counterpart of langgraph-agents/deploy/observability/dropin/gridiron_otel.py
// (and a sibling of Ach-payments' internal/observability). It ships this
// service's ERROR logs to OpenObserve over OTLP/HTTP. The estate alert fires on
// any `level=error` record in the module's stream, so a storage failure here
// becomes an OpenObserve alert → langgraph `/webhooks/openobserve/alert` →
// diagnose → fix.
//
// Contract A deliberately turns every storage failure into a structured non-2xx
// JSON body (tools.go errf(502, ...)), so there is NO unhandled 5xx for an alert
// to hang off — the rail has to be fired from the handler path explicitly (see
// handleInvoke). That is the whole reason this file exists rather than a bare
// log.Printf.
//
// Env contract (identical to the Python drop-in):
//
//	OTEL_ENABLED=1
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://openobserve:5080/api/<org>/v1
//	OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic <base64 email:password>
//	OTEL_SERVICE_NAME=minio-mcp      (== OpenObserve stream == incident 'module')
//
// Graceful by construction: with the env unset every call no-ops, and an exporter
// that fails to build degrades to log-only. Telemetry must never be a boot
// dependency — storage must not fail to start because the collector is down.
package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// DefaultServiceName is the OpenObserve stream this module reports into. It must
// match the stream name and the alert's module name; a test pins that.
const DefaultServiceName = "minio-mcp"

// Emitter ships ERROR-severity records to the self-heal rail.
type Emitter interface {
	// Error emits one ERROR-severity record. Never blocks the caller's work and
	// never panics; a telemetry failure must not become a storage failure.
	Error(ctx context.Context, msg string, attrs ...attribute.KeyValue)
}

// Observability is the initialized rail plus its shutdown hook.
type Observability struct {
	Enabled     bool
	ServiceName string

	provider *sdklog.LoggerProvider
	logger   otellog.Logger
}

// nopEmitter is what callers get when the rail is disabled.
type nopEmitter struct{}

func (nopEmitter) Error(context.Context, string, ...attribute.KeyValue) {}

// NopEmitter returns an Emitter that discards everything. Use in tests and in
// code paths constructed before Init.
func NopEmitter() Emitter { return nopEmitter{} }

// EnabledFromEnv reports whether the rail should initialize: the master switch,
// or an endpoint simply being configured.
func EnabledFromEnv(getenv func(string) string) bool {
	if truthy(getenv("OTEL_ENABLED")) {
		return true
	}
	return strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
}

// ParseHeaders parses the comma-separated `key=value` list from the OTLP env
// contract.
//
// Each pair splits on the FIRST "=" only. Splitting on every "=" is the classic
// bug here: a base64 Authorization value ends in "=" padding, and naive splitting
// truncates the credential into an invalid one, producing a 401 that reads like a
// permissions problem rather than a parsing mistake.
func ParseHeaders(raw string) map[string]string {
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

// Options configures Init. The zero value reads everything from the environment.
type Options struct {
	ServiceName string
	Env         string
	// ForceEnable builds the pipeline even with the env unset — the test hook.
	ForceEnable bool
	// Exporter injects an in-memory exporter so a test can assert what would ship.
	Exporter sdklog.Exporter
	// Synchronous uses a simple processor for a deterministic flush in tests.
	Synchronous bool
	// Getenv is injectable so tests need not mutate the process environment.
	Getenv func(string) string
}

// Init wires the OTLP log pipeline. Safe to call unconditionally; returns a
// disabled Observability (whose Error is a no-op) when the env is unset.
func Init(ctx context.Context, opts Options, logger *slog.Logger) (*Observability, error) {
	getenv := opts.Getenv
	if getenv == nil {
		getenv = osGetenv
	}
	serviceName := strings.TrimSpace(opts.ServiceName)
	if serviceName == "" {
		serviceName = strings.TrimSpace(getenv("OTEL_SERVICE_NAME"))
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	if !opts.ForceEnable && !EnabledFromEnv(getenv) {
		if logger != nil {
			logger.Info("self-heal rail disabled (set OTEL_EXPORTER_OTLP_ENDPOINT to enable)")
		}
		return &Observability{Enabled: false, ServiceName: serviceName}, nil
	}

	exporter := opts.Exporter
	if exporter == nil {
		exporterOpts := []otlploghttp.Option{}
		if hdrs := ParseHeaders(getenv("OTEL_EXPORTER_OTLP_HEADERS")); len(hdrs) > 0 {
			exporterOpts = append(exporterOpts, otlploghttp.WithHeaders(hdrs))
		}
		endpoint := strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
		if host, path, insecure := splitEndpoint(endpoint); host != "" {
			exporterOpts = append(exporterOpts, otlploghttp.WithEndpoint(host))
			if path != "" {
				exporterOpts = append(exporterOpts, otlploghttp.WithURLPath(path+"/logs"))
			}
			if insecure {
				exporterOpts = append(exporterOpts, otlploghttp.WithInsecure())
			}
		}
		var err error
		exporter, err = otlploghttp.New(ctx, exporterOpts...)
		if err != nil {
			// Degrade rather than fail the boot: a telemetry outage must not stop
			// the storage sidecar from serving.
			if logger != nil {
				logger.Error("self-heal rail unavailable; errors will NOT reach OpenObserve", "err", err)
			}
			return &Observability{Enabled: false, ServiceName: serviceName}, nil
		}
	}

	var processor sdklog.Processor
	if opts.Synchronous {
		processor = sdklog.NewSimpleProcessor(exporter)
	} else {
		processor = sdklog.NewBatchProcessor(exporter)
	}

	attrs := []attribute.KeyValue{semconv.ServiceName(serviceName)}
	if env := strings.TrimSpace(opts.Env); env != "" {
		attrs = append(attrs, attribute.String("deployment.environment", env))
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
	if err != nil {
		return nil, err
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(processor),
	)
	global.SetLoggerProvider(provider)

	if logger != nil {
		logger.Info("self-heal rail enabled", "service", serviceName)
	}
	return &Observability{
		Enabled:     true,
		ServiceName: serviceName,
		provider:    provider,
		logger:      provider.Logger(serviceName),
	}, nil
}

// splitEndpoint turns "http://host:5080/api/org/v1" into
// ("host:5080", "/api/org/v1", true).
func splitEndpoint(endpoint string) (host, path string, insecure bool) {
	if endpoint == "" {
		return "", "", false
	}
	insecure = strings.HasPrefix(endpoint, "http://")
	trimmed := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	host, rest, found := strings.Cut(trimmed, "/")
	if !found || rest == "" {
		return host, "", insecure
	}
	return host, "/" + rest, insecure
}

// Error emits one ERROR-severity record to the module's OpenObserve stream.
//
// SeverityText is set explicitly and is NOT optional: OpenObserve derives the
// `level` field the alert rule matches on from the text, so a record carrying
// only a numeric severity satisfies every OTel assertion and is invisible to the
// alert.
func (o *Observability) Error(ctx context.Context, msg string, attrs ...attribute.KeyValue) {
	if o == nil || !o.Enabled || o.logger == nil {
		return
	}
	var rec otellog.Record
	now := time.Now()
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(otellog.SeverityError)
	rec.SetSeverityText("ERROR")
	rec.SetBody(otellog.StringValue(msg))
	for _, a := range attrs {
		rec.AddAttributes(otellog.String(string(a.Key), a.Value.Emit()))
	}
	o.logger.Emit(ctx, rec)
}

func osGetenv(k string) string { return os.Getenv(k) }

// ForceFlush pushes pending records; used by tests and by shutdown.
func (o *Observability) ForceFlush(ctx context.Context) error {
	if o == nil || o.provider == nil {
		return nil
	}
	return o.provider.ForceFlush(ctx)
}

// Shutdown flushes and stops the pipeline.
func (o *Observability) Shutdown(ctx context.Context) error {
	if o == nil || o.provider == nil {
		return nil
	}
	return o.provider.Shutdown(ctx)
}
