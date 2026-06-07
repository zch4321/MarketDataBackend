package config

import (
	"testing"
	"time"
)

func envFrom(m map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestDefaultValues(t *testing.T) {
	d := Default()
	if d.Mode != ModeControlPlane {
		t.Errorf("Mode = %q, want %q", d.Mode, ModeControlPlane)
	}
	if d.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", d.HTTPAddr)
	}
	if d.MetricsAddr != ":9090" {
		t.Errorf("MetricsAddr = %q, want :9090", d.MetricsAddr)
	}
	if d.DBMaxConns != 10 {
		t.Errorf("DBMaxConns = %d, want 10", d.DBMaxConns)
	}
	if d.LeaseTTL != 30*time.Second {
		t.Errorf("LeaseTTL = %s, want 30s", d.LeaseTTL)
	}
	if d.LeaseRenewInterval != 10*time.Second {
		t.Errorf("LeaseRenewInterval = %s, want 10s", d.LeaseRenewInterval)
	}
	if d.RuntimeMaxGroups != 10 {
		t.Errorf("RuntimeMaxGroups = %d, want 10", d.RuntimeMaxGroups)
	}
	if d.RuntimeMaxWeight != 100 {
		t.Errorf("RuntimeMaxWeight = %d, want 100", d.RuntimeMaxWeight)
	}
	if len(d.KafkaBrokers) != 1 || d.KafkaBrokers[0] != "127.0.0.1:19092" {
		t.Errorf("KafkaBrokers = %v", d.KafkaBrokers)
	}
	if d.InputStatusReportInterval != time.Second || d.InputReconcileInterval != 2*time.Second {
		t.Errorf("input intervals = %s/%s", d.InputStatusReportInterval, d.InputReconcileInterval)
	}
	if d.TradeBatchSize != 500 || d.TradeBatchFlushInterval != 100*time.Millisecond ||
		d.KlineBatchSize != 100 || d.KlineBatchFlushInterval != 500*time.Millisecond ||
		d.OrderBookDeltaBatchSize != 2000 ||
		d.OrderBookDeltaFlushInterval != 100*time.Millisecond {
		t.Errorf("batch defaults = trade %d/%s kline %d/%s delta %d/%s",
			d.TradeBatchSize, d.TradeBatchFlushInterval,
			d.KlineBatchSize, d.KlineBatchFlushInterval,
			d.OrderBookDeltaBatchSize, d.OrderBookDeltaFlushInterval)
	}
	if d.OrderBookSnapshotInterval != time.Minute ||
		d.OrderBookDeltaRetention != 72*time.Hour ||
		d.OrderBookDeltaPartitionInterval != time.Hour ||
		d.PartitionMaintenanceInterval != time.Hour ||
		d.PartitionPrecreateHorizon != 3*time.Hour {
		t.Errorf("orderbook defaults = %s/%s/%s/%s/%s",
			d.OrderBookSnapshotInterval, d.OrderBookDeltaRetention,
			d.OrderBookDeltaPartitionInterval,
			d.PartitionMaintenanceInterval, d.PartitionPrecreateHorizon)
	}
	if d.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want json", d.LogFormat)
	}
	if d.DatabaseDSN != "" {
		t.Errorf("DatabaseDSN default should be empty, got %q", d.DatabaseDSN)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	cfg, err := Load(envFrom(map[string]string{
		"MODE":                           ModeStreamRuntime,
		"HTTP_ADDR":                      ":18080",
		"DATABASE_DSN":                   "postgres://u:p@localhost:5432/db?sslmode=disable",
		"DB_MAX_CONNS":                   "42",
		"DB_MIN_CONNS":                   "5",
		"DB_CONNECT_TIMEOUT":             "3s",
		"LEASE_TTL":                      "15s",
		"LEASE_RENEW_INTERVAL":           "4s",
		"RUNTIME_MAX_GROUPS":             "3",
		"RUNTIME_MAX_WEIGHT":             "7",
		"RUNTIME_NODE_ID":                "node-a",
		"KAFKA_BROKERS":                  "kafka-a:9092, kafka-b:9092",
		"INPUT_STATUS_REPORT_INTERVAL":   "250ms",
		"INPUT_RECONCILE_INTERVAL":       "750ms",
		"TRADE_BATCH_SIZE":               "50",
		"TRADE_BATCH_FLUSH_INTERVAL":     "25ms",
		"KLINE_BATCH_SIZE":               "20",
		"KLINE_BATCH_FLUSH_INTERVAL":     "200ms",
		"ORDERBOOK_DELTA_BATCH_SIZE":     "250",
		"ORDERBOOK_DELTA_FLUSH_INTERVAL": "50ms",
		"ORDERBOOK_SNAPSHOT_INTERVAL":    "2m",
		"ORDERBOOK_DELTA_RETENTION":      "48h",
		"PARTITION_MAINTENANCE_INTERVAL": "30m",
		"PARTITION_PRECREATE_HORIZON":    "6h",
		"LOG_LEVEL":                      "debug",
		"LOG_FORMAT":                     "text",
	}))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Mode != ModeStreamRuntime {
		t.Errorf("Mode = %q, want %q", cfg.Mode, ModeStreamRuntime)
	}
	if cfg.HTTPAddr != ":18080" {
		t.Errorf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if cfg.DBMaxConns != 42 {
		t.Errorf("DBMaxConns = %d, want 42", cfg.DBMaxConns)
	}
	if cfg.DBMinConns != 5 {
		t.Errorf("DBMinConns = %d, want 5", cfg.DBMinConns)
	}
	if cfg.DBConnectTimeout != 3*time.Second {
		t.Errorf("DBConnectTimeout = %s, want 3s", cfg.DBConnectTimeout)
	}
	if cfg.LeaseTTL != 15*time.Second {
		t.Errorf("LeaseTTL = %s, want 15s", cfg.LeaseTTL)
	}
	if cfg.LeaseRenewInterval != 4*time.Second {
		t.Errorf("LeaseRenewInterval = %s, want 4s", cfg.LeaseRenewInterval)
	}
	if cfg.RuntimeMaxGroups != 3 {
		t.Errorf("RuntimeMaxGroups = %d, want 3", cfg.RuntimeMaxGroups)
	}
	if cfg.RuntimeMaxWeight != 7 {
		t.Errorf("RuntimeMaxWeight = %d, want 7", cfg.RuntimeMaxWeight)
	}
	if cfg.RuntimeNodeID != "node-a" {
		t.Errorf("RuntimeNodeID = %q", cfg.RuntimeNodeID)
	}
	if len(cfg.KafkaBrokers) != 2 ||
		cfg.KafkaBrokers[0] != "kafka-a:9092" || cfg.KafkaBrokers[1] != "kafka-b:9092" {
		t.Errorf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if cfg.InputStatusReportInterval != 250*time.Millisecond ||
		cfg.InputReconcileInterval != 750*time.Millisecond {
		t.Errorf("input intervals = %s/%s",
			cfg.InputStatusReportInterval, cfg.InputReconcileInterval)
	}
	if cfg.TradeBatchSize != 50 || cfg.TradeBatchFlushInterval != 25*time.Millisecond ||
		cfg.KlineBatchSize != 20 || cfg.KlineBatchFlushInterval != 200*time.Millisecond ||
		cfg.OrderBookDeltaBatchSize != 250 ||
		cfg.OrderBookDeltaFlushInterval != 50*time.Millisecond {
		t.Errorf("batch env settings were not loaded: %+v", cfg)
	}
	if cfg.OrderBookSnapshotInterval != 2*time.Minute ||
		cfg.OrderBookDeltaRetention != 48*time.Hour ||
		cfg.PartitionMaintenanceInterval != 30*time.Minute ||
		cfg.PartitionPrecreateHorizon != 6*time.Hour {
		t.Errorf("partition env settings were not loaded: %+v", cfg)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("log settings = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestLoadStorageDSNFallback(t *testing.T) {
	cfg, err := Load(envFrom(map[string]string{
		"STORAGE_DSN": "postgres://u:p@localhost:5432/db?sslmode=disable",
	}))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.DatabaseDSN == "" {
		t.Error("expected STORAGE_DSN to populate DatabaseDSN")
	}
}

func TestLoadMissingDSN(t *testing.T) {
	_, err := Load(envFrom(map[string]string{}))
	if err == nil {
		t.Fatal("expected error when DSN is missing")
	}
}

func TestLoadInvalidInt(t *testing.T) {
	_, err := Load(envFrom(map[string]string{
		"DATABASE_DSN": "postgres://u:p@localhost:5432/db",
		"DB_MAX_CONNS": "not-a-number",
	}))
	if err == nil {
		t.Fatal("expected error for invalid DB_MAX_CONNS")
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	_, err := Load(envFrom(map[string]string{
		"DATABASE_DSN": "postgres://u:p@localhost:5432/db",
		"LEASE_TTL":    "abc",
	}))
	if err == nil {
		t.Fatal("expected error for invalid LEASE_TTL")
	}
}

func TestValidateMinGreaterThanMax(t *testing.T) {
	_, err := Load(envFrom(map[string]string{
		"DATABASE_DSN": "postgres://u:p@localhost:5432/db",
		"DB_MAX_CONNS": "2",
		"DB_MIN_CONNS": "5",
	}))
	if err == nil {
		t.Fatal("expected error when DB_MIN_CONNS > DB_MAX_CONNS")
	}
}

func TestValidateInvalidMode(t *testing.T) {
	cfg := Default()
	cfg.DatabaseDSN = "postgres://u:p@localhost/db"
	cfg.Mode = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestValidateRenewIntervalNotBelowTTL(t *testing.T) {
	_, err := Load(envFrom(map[string]string{
		"DATABASE_DSN":         "postgres://u:p@localhost:5432/db",
		"LEASE_TTL":            "10s",
		"LEASE_RENEW_INTERVAL": "10s",
	}))
	if err == nil {
		t.Fatal("expected error when LEASE_RENEW_INTERVAL >= LEASE_TTL")
	}
}

func TestValidateRuntimeCadenceAndCapacity(t *testing.T) {
	cases := map[string]string{
		"RUNTIME_MAX_GROUPS":             "0",
		"RUNTIME_MAX_WEIGHT":             "0",
		"RECONCILE_INTERVAL":             "0s",
		"HEARTBEAT_INTERVAL":             "0s",
		"SHUTDOWN_TIMEOUT":               "0s",
		"DB_CONNECT_TIMEOUT":             "0s",
		"INPUT_STATUS_REPORT_INTERVAL":   "0s",
		"INPUT_RECONCILE_INTERVAL":       "0s",
		"TRADE_BATCH_SIZE":               "0",
		"KLINE_BATCH_SIZE":               "0",
		"ORDERBOOK_DELTA_BATCH_SIZE":     "0",
		"TRADE_BATCH_FLUSH_INTERVAL":     "0s",
		"KLINE_BATCH_FLUSH_INTERVAL":     "0s",
		"ORDERBOOK_DELTA_FLUSH_INTERVAL": "0s",
		"ORDERBOOK_SNAPSHOT_INTERVAL":    "0s",
		"ORDERBOOK_DELTA_RETENTION":      "0s",
		"PARTITION_MAINTENANCE_INTERVAL": "0s",
	}
	for key, value := range cases {
		t.Run(key, func(t *testing.T) {
			env := map[string]string{
				"DATABASE_DSN": "postgres://u:p@localhost:5432/db",
				key:            value,
			}
			if _, err := Load(envFrom(env)); err == nil {
				t.Fatalf("expected validation error for %s=%s", key, value)
			}
		})
	}
}

func TestValidateDeltaPartitionIntervalIsHourly(t *testing.T) {
	_, err := Load(envFrom(map[string]string{
		"DATABASE_DSN":                       "postgres://u:p@localhost:5432/db",
		"ORDERBOOK_DELTA_PARTITION_INTERVAL": "30m",
	}))
	if err == nil {
		t.Fatal("expected non-hourly delta partition interval to fail")
	}
}

func TestValidateKafkaBrokers(t *testing.T) {
	cfg := Default()
	cfg.DatabaseDSN = "postgres://u:p@localhost/db"
	cfg.KafkaBrokers = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected empty Kafka brokers to fail validation")
	}

	cfg.KafkaBrokers = []string{"broker:9092", ""}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected blank Kafka broker to fail validation")
	}
}
