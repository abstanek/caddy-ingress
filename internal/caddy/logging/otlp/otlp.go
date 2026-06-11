// Package otlp registers a Caddy log writer that ships records over OTLP.
//
// All connection settings (endpoint, headers, protocol, TLS, compression,
// timeout, etc.) come from the standard OTEL_EXPORTER_OTLP_* environment
// variables read by the OTel Go SDK. Service name and resource attributes
// come from OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES. Activation is
// driven by OTEL_LOGS_EXPORTER=otlp on the controller side; this writer
// itself takes no JSON configuration.
package otlp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

const scopeName = "github.com/caddyserver/ingress/internal/caddy/logging/otlp"

func init() {
	caddy.RegisterModule(Writer{})
}

// Writer is a Caddy log writer that emits each encoded log line as an OTLP
// log record. It has no fields; all knobs are read from OTEL_* env vars.
type Writer struct{}

// CaddyModule returns the Caddy module information.
func (Writer) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.otlp",
		New: func() caddy.Module { return new(Writer) },
	}
}

func (Writer) String() string    { return "otlp" }
func (Writer) WriterKey() string { return "otlp" }

// OpenWriter builds an env-configured OTLP log exporter and returns an
// adapter that turns each JSON-encoded Caddy log line into an OTel record.
func (Writer) OpenWriter() (io.WriteCloser, error) {
	ctx := context.Background()
	exp, err := autoexport.NewLogExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp log exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.Empty())
	if err != nil {
		return nil, fmt.Errorf("otlp resource: %w", err)
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		sdklog.WithResource(res),
	)
	return &otlpWriter{
		provider: provider,
		logger:   provider.Logger(scopeName),
	}, nil
}

type otlpWriter struct {
	provider *sdklog.LoggerProvider
	logger   otellog.Logger
}

func (w *otlpWriter) Write(b []byte) (int, error) {
	rec := buildRecord(b)
	w.logger.Emit(context.Background(), rec)
	return len(b), nil
}

func (w *otlpWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.provider.Shutdown(ctx)
}

// buildRecord turns one JSON log line from Caddy into an otellog.Record.
// Unrecognized input is emitted as a record whose body is the raw line.
func buildRecord(b []byte) otellog.Record {
	var rec otellog.Record
	rec.SetObservedTimestamp(time.Now())

	fields := map[string]any{}
	if err := json.Unmarshal(b, &fields); err != nil {
		rec.SetBody(otellog.StringValue(strings.TrimRight(string(b), "\n")))
		return rec
	}

	if v, ok := fields["msg"].(string); ok {
		rec.SetBody(otellog.StringValue(v))
		delete(fields, "msg")
	}
	if v, ok := fields["level"].(string); ok {
		sev, text := mapSeverity(v)
		rec.SetSeverity(sev)
		rec.SetSeverityText(text)
		delete(fields, "level")
	}
	if v, ok := fields["ts"].(float64); ok {
		sec, frac := splitTS(v)
		rec.SetTimestamp(time.Unix(sec, frac))
		delete(fields, "ts")
	}

	for k, v := range fields {
		rec.AddAttributes(otellog.KeyValue{Key: k, Value: toLogValue(v)})
	}
	return rec
}

func splitTS(v float64) (int64, int64) {
	sec := int64(v)
	frac := int64((v - float64(sec)) * 1e9)
	return sec, frac
}

func mapSeverity(level string) (otellog.Severity, string) {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return otellog.SeverityDebug, level
	case "INFO":
		return otellog.SeverityInfo, level
	case "WARN", "WARNING":
		return otellog.SeverityWarn, level
	case "ERROR":
		return otellog.SeverityError, level
	case "FATAL", "PANIC", "DPANIC":
		return otellog.SeverityFatal, level
	}
	return otellog.SeverityUndefined, level
}

func toLogValue(v any) otellog.Value {
	switch x := v.(type) {
	case nil:
		return otellog.Value{}
	case string:
		return otellog.StringValue(x)
	case bool:
		return otellog.BoolValue(x)
	case float64:
		if x == float64(int64(x)) {
			return otellog.Int64Value(int64(x))
		}
		return otellog.Float64Value(x)
	case []any:
		vals := make([]otellog.Value, 0, len(x))
		for _, e := range x {
			vals = append(vals, toLogValue(e))
		}
		return otellog.SliceValue(vals...)
	case map[string]any:
		kvs := make([]otellog.KeyValue, 0, len(x))
		for k, e := range x {
			kvs = append(kvs, otellog.KeyValue{Key: k, Value: toLogValue(e)})
		}
		return otellog.MapValue(kvs...)
	}
	enc, err := json.Marshal(v)
	if err != nil {
		return otellog.StringValue(fmt.Sprintf("%v", v))
	}
	return otellog.StringValue(string(enc))
}

// Interface guards
var (
	_ caddy.Module       = (*Writer)(nil)
	_ caddy.WriterOpener = (*Writer)(nil)
)
