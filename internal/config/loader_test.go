package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseDotEnv(t *testing.T) {
	in := strings.NewReader(`
# a comment
export FOO=bar
QUOTED="hello world"
SINGLE='single quoted'
EMPTY=
WITH_HASH=value # not-a-comment
SPACED =  trimmed
`)
	got, err := ParseDotEnv(in)
	if err != nil {
		t.Fatalf("ParseDotEnv: %v", err)
	}
	want := map[string]string{
		"FOO":       "bar",
		"QUOTED":    "hello world",
		"SINGLE":    "single quoted",
		"EMPTY":     "",
		"WITH_HASH": "value # not-a-comment",
		"SPACED":    "trimmed",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestParseDotEnvMalformed(t *testing.T) {
	if _, err := ParseDotEnv(strings.NewReader("NOT_A_PAIR\n")); err == nil {
		t.Fatal("expected error for line without '='")
	}
	if _, err := ParseDotEnv(strings.NewReader("=novalue\n")); err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestLoadDotEnvDoesNotOverrideRealEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("MD_TEST_PRESET=fromfile\nMD_TEST_NEW=created\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	t.Setenv("MD_TEST_PRESET", "fromenv")

	applied, err := LoadDotEnv(path)
	if err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}
	if os.Getenv("MD_TEST_PRESET") != "fromenv" {
		t.Errorf("real env should win, got %q", os.Getenv("MD_TEST_PRESET"))
	}
	if os.Getenv("MD_TEST_NEW") != "created" {
		t.Errorf("new key should be set from .env, got %q", os.Getenv("MD_TEST_NEW"))
	}
	if !contains(applied, "MD_TEST_NEW") || contains(applied, "MD_TEST_PRESET") {
		t.Errorf("applied = %v, want only MD_TEST_NEW", applied)
	}
	_ = os.Unsetenv("MD_TEST_NEW")
}

func TestLoadDotEnvMissingFileOK(t *testing.T) {
	applied, err := LoadDotEnv(filepath.Join(t.TempDir(), "nope.env"))
	if err != nil || applied != nil {
		t.Fatalf("missing .env should be a no-op, got applied=%v err=%v", applied, err)
	}
}

func TestParseYAMLFlattensAndExpands(t *testing.T) {
	data := []byte(`
mode: stream-runtime
server:
  http_addr: ":19090"
database:
  dsn: "postgres://u:${DB_PASSWORD}@h:5432/db?sslmode=disable"
  max_conns: 20
  min_conns: 0
kafka_clusters:
  default:
    brokers:
      - kafka-a:9092
      - kafka-b:9092
runtime:
  lease_ttl: 45s
  lease_renew_interval: 5s
processing:
  status_report_interval: 250ms
  input_reconcile_interval: 750ms
  batch:
    trade:
      size: 50
      flush_interval: 25ms
    kline:
      size: 20
      flush_interval: 200ms
    orderbook_delta:
      size: 250
      flush_interval: 50ms
  orderbook:
    snapshot_interval: 2m
    delta_retention: 48h
    delta_partition_interval: 1h
    partition_maintenance_interval: 30m
    partition_precreate_horizon: 6h
log:
  level: debug
`)
	lookup := envFrom(map[string]string{"DB_PASSWORD": "s3cr3t"})
	got, err := ParseYAML(data, lookup)
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}

	checks := map[string]string{
		"MODE":                               "stream-runtime",
		"HTTP_ADDR":                          ":19090",
		"DATABASE_DSN":                       "postgres://u:s3cr3t@h:5432/db?sslmode=disable",
		"DB_MAX_CONNS":                       "20",
		"DB_MIN_CONNS":                       "0",
		"KAFKA_BROKERS":                      "kafka-a:9092,kafka-b:9092",
		"LEASE_TTL":                          "45s",
		"LEASE_RENEW_INTERVAL":               "5s",
		"INPUT_STATUS_REPORT_INTERVAL":       "250ms",
		"INPUT_RECONCILE_INTERVAL":           "750ms",
		"TRADE_BATCH_SIZE":                   "50",
		"TRADE_BATCH_FLUSH_INTERVAL":         "25ms",
		"KLINE_BATCH_SIZE":                   "20",
		"KLINE_BATCH_FLUSH_INTERVAL":         "200ms",
		"ORDERBOOK_DELTA_BATCH_SIZE":         "250",
		"ORDERBOOK_DELTA_FLUSH_INTERVAL":     "50ms",
		"ORDERBOOK_SNAPSHOT_INTERVAL":        "2m",
		"ORDERBOOK_DELTA_RETENTION":          "48h",
		"ORDERBOOK_DELTA_PARTITION_INTERVAL": "1h",
		"PARTITION_MAINTENANCE_INTERVAL":     "30m",
		"PARTITION_PRECREATE_HORIZON":        "6h",
		"LOG_LEVEL":                          "debug",
	}
	for k, v := range checks {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	// Keys not present in the YAML must not be emitted.
	if _, ok := got["SHUTDOWN_TIMEOUT"]; ok {
		t.Error("SHUTDOWN_TIMEOUT should not be emitted when absent")
	}
	if _, ok := got["METRICS_ADDR"]; ok {
		t.Error("METRICS_ADDR should not be emitted when absent")
	}
}

func TestParseYAMLEmpty(t *testing.T) {
	got, err := ParseYAML([]byte("   \n# only a comment\n"), envFrom(nil))
	if err != nil {
		t.Fatalf("ParseYAML empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestParseYAMLUnknownFieldRejected(t *testing.T) {
	_, err := ParseYAML([]byte("database:\n  bogus: 1\n"), envFrom(nil))
	if err == nil {
		t.Fatal("expected error for unknown YAML field")
	}
}

func TestLoadFileEnvOverridesYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
mode: stream-runtime
database:
  dsn: "postgres://u:${DB_PASSWORD}@h:5432/db?sslmode=disable"
runtime:
  lease_ttl: 45s
  lease_renew_interval: 5s
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Env supplies the secret for ${DB_PASSWORD} and overrides MODE.
	lookup := envFrom(map[string]string{
		"DB_PASSWORD": "pw",
		"MODE":        ModeControlPlane,
	})
	cfg, err := LoadFile(path, lookup)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Mode != ModeControlPlane {
		t.Errorf("Mode = %q, want env override %q", cfg.Mode, ModeControlPlane)
	}
	if cfg.DatabaseDSN != "postgres://u:pw@h:5432/db?sslmode=disable" {
		t.Errorf("DSN = %q, want expanded password", cfg.DatabaseDSN)
	}
	if cfg.LeaseTTL != 45*time.Second {
		t.Errorf("LeaseTTL = %s, want 45s from YAML", cfg.LeaseTTL)
	}
}

func TestLoadFileDatabaseDSNEnvWinsOverYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("database:\n  dsn: postgres://yaml@h/db\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	lookup := envFrom(map[string]string{"DATABASE_DSN": "postgres://env@h/db"})
	cfg, err := LoadFile(path, lookup)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.DatabaseDSN != "postgres://env@h/db" {
		t.Errorf("DSN = %q, want env value", cfg.DatabaseDSN)
	}
}

func TestLoadFileMissingPathFallsBackToEnv(t *testing.T) {
	lookup := envFrom(map[string]string{
		"DATABASE_DSN": "postgres://u:p@h:5432/db?sslmode=disable",
	})
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "absent.yaml"), lookup)
	if err != nil {
		t.Fatalf("LoadFile missing path: %v", err)
	}
	if cfg.DatabaseDSN == "" {
		t.Error("expected env-only load when file is missing")
	}
}

func TestLoadFileEmptyPathIsEnvOnly(t *testing.T) {
	lookup := envFrom(map[string]string{
		"DATABASE_DSN": "postgres://u:p@h:5432/db?sslmode=disable",
	})
	cfg, err := LoadFile("", lookup)
	if err != nil {
		t.Fatalf("LoadFile empty path: %v", err)
	}
	if cfg.DatabaseDSN == "" {
		t.Error("expected env-only load for empty path")
	}
}

// contains is a small test helper shared by the loader tests.
func contains(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}
