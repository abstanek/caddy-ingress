package otlp

import (
	"testing"

	otellog "go.opentelemetry.io/otel/log"
)

func TestBuildRecord_TypicalJSONLine(t *testing.T) {
	line := []byte(`{"level":"info","ts":1700000000.5,"msg":"hello","status":200,"path":"/x","ok":true}`)
	rec := buildRecord(line)

	if got := rec.Body().AsString(); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
	if rec.Severity() != otellog.SeverityInfo {
		t.Errorf("severity = %v, want Info", rec.Severity())
	}
	if rec.Timestamp().Unix() != 1700000000 {
		t.Errorf("timestamp seconds = %d, want 1700000000", rec.Timestamp().Unix())
	}

	attrs := map[string]otellog.Value{}
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	if v, ok := attrs["status"]; !ok || v.AsInt64() != 200 {
		t.Errorf("status attr = %v (ok=%v), want int64 200", v, ok)
	}
	if v, ok := attrs["path"]; !ok || v.AsString() != "/x" {
		t.Errorf("path attr = %v (ok=%v), want string /x", v, ok)
	}
	if v, ok := attrs["ok"]; !ok || v.AsBool() != true {
		t.Errorf("ok attr = %v (ok=%v), want bool true", v, ok)
	}
	if _, present := attrs["msg"]; present {
		t.Errorf("msg should not appear as attribute (it's the body)")
	}
	if _, present := attrs["level"]; present {
		t.Errorf("level should not appear as attribute (it's the severity)")
	}
}

func TestBuildRecord_NonJSONFallsBackToBody(t *testing.T) {
	line := []byte("not json\n")
	rec := buildRecord(line)
	if got := rec.Body().AsString(); got != "not json" {
		t.Errorf("body = %q, want %q", got, "not json")
	}
}

func TestMapSeverity(t *testing.T) {
	cases := map[string]otellog.Severity{
		"debug":   otellog.SeverityDebug,
		"INFO":    otellog.SeverityInfo,
		"warn":    otellog.SeverityWarn,
		"WARNING": otellog.SeverityWarn,
		"error":   otellog.SeverityError,
		"fatal":   otellog.SeverityFatal,
		"weird":   otellog.SeverityUndefined,
	}
	for in, want := range cases {
		got, _ := mapSeverity(in)
		if got != want {
			t.Errorf("mapSeverity(%q) = %v, want %v", in, got, want)
		}
	}
}
