// Package config loads runtime configuration from defaults, an optional YAML
// file and environment variables (optionally seeded from a .env file). The
// environment always has the final say so secrets such as the database password
// can be injected by the platform without being committed to a config file.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Process modes. The project ships two binaries but they share one config
// structure; Mode only affects logging / bookkeeping for now.
const (
	ModeControlPlane  = "control-plane"
	ModeStreamRuntime = "stream-runtime"
)

// Config holds all runtime configuration for the control-plane and the
// stream-runtime binaries.
type Config struct {
	Mode        string
	HTTPAddr    string
	MetricsAddr string

	DatabaseDSN      string
	DBMaxConns       int32
	DBMinConns       int32
	DBConnectTimeout time.Duration

	KafkaBrokers []string

	RuntimeNodeID      string
	RuntimeMaxGroups   int
	RuntimeMaxWeight   int
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	ReconcileInterval  time.Duration
	HeartbeatInterval  time.Duration
	ShutdownTimeout    time.Duration

	InputStatusReportInterval time.Duration
	InputReconcileInterval    time.Duration

	TradeBatchSize                  int
	TradeBatchFlushInterval         time.Duration
	KlineBatchSize                  int
	KlineBatchFlushInterval         time.Duration
	OrderBookDeltaBatchSize         int
	OrderBookDeltaFlushInterval     time.Duration
	OrderBookSnapshotInterval       time.Duration
	OrderBookDeltaRetention         time.Duration
	OrderBookDeltaPartitionInterval time.Duration
	PartitionMaintenanceInterval    time.Duration
	PartitionPrecreateHorizon       time.Duration

	LogLevel  string
	LogFormat string
}

// LookupFunc mirrors os.LookupEnv so configuration loading can be tested
// without mutating the real process environment.
type LookupFunc func(key string) (string, bool)

// Default returns the baseline configuration before any environment overrides
// are applied. DatabaseDSN is intentionally empty so that a missing DSN is a
// validation error rather than a silent fallback.
func Default() Config {
	return Config{
		Mode:        ModeControlPlane,
		HTTPAddr:    ":8080",
		MetricsAddr: ":9090",

		DatabaseDSN:      "",
		DBMaxConns:       10,
		DBMinConns:       0,
		DBConnectTimeout: 5 * time.Second,

		KafkaBrokers: []string{"127.0.0.1:19090"},

		RuntimeNodeID:      "",
		RuntimeMaxGroups:   10,
		RuntimeMaxWeight:   100,
		LeaseTTL:           30 * time.Second,
		LeaseRenewInterval: 10 * time.Second,
		ReconcileInterval:  5 * time.Second,
		HeartbeatInterval:  10 * time.Second,
		ShutdownTimeout:    20 * time.Second,

		InputStatusReportInterval: time.Second,
		InputReconcileInterval:    2 * time.Second,

		TradeBatchSize:                  500,
		TradeBatchFlushInterval:         100 * time.Millisecond,
		KlineBatchSize:                  100,
		KlineBatchFlushInterval:         500 * time.Millisecond,
		OrderBookDeltaBatchSize:         2000,
		OrderBookDeltaFlushInterval:     100 * time.Millisecond,
		OrderBookSnapshotInterval:       time.Minute,
		OrderBookDeltaRetention:         72 * time.Hour,
		OrderBookDeltaPartitionInterval: time.Hour,
		PartitionMaintenanceInterval:    time.Hour,
		PartitionPrecreateHorizon:       3 * time.Hour,

		LogLevel:  "info",
		LogFormat: "json",
	}
}

// Load reads configuration using the provided lookup function (typically
// os.LookupEnv) and validates the result.
func Load(lookup LookupFunc) (Config, error) {
	cfg := Default()

	cfg.Mode = stringEnv(lookup, "MODE", cfg.Mode)
	cfg.HTTPAddr = stringEnv(lookup, "HTTP_ADDR", cfg.HTTPAddr)
	cfg.MetricsAddr = stringEnv(lookup, "METRICS_ADDR", cfg.MetricsAddr)

	// Accept DATABASE_DSN first, fall back to STORAGE_DSN to match the design doc.
	cfg.DatabaseDSN = stringEnv(lookup, "DATABASE_DSN", cfg.DatabaseDSN)
	cfg.DatabaseDSN = stringEnv(lookup, "STORAGE_DSN", cfg.DatabaseDSN)

	var err error
	if cfg.DBMaxConns, err = int32Env(lookup, "DB_MAX_CONNS", cfg.DBMaxConns); err != nil {
		return Config{}, err
	}
	if cfg.DBMinConns, err = int32Env(lookup, "DB_MIN_CONNS", cfg.DBMinConns); err != nil {
		return Config{}, err
	}
	if cfg.DBConnectTimeout, err = durationEnv(lookup, "DB_CONNECT_TIMEOUT", cfg.DBConnectTimeout); err != nil {
		return Config{}, err
	}

	cfg.KafkaBrokers = stringSliceEnv(lookup, "KAFKA_BROKERS", cfg.KafkaBrokers)

	cfg.RuntimeNodeID = stringEnv(lookup, "RUNTIME_NODE_ID", cfg.RuntimeNodeID)
	if cfg.RuntimeMaxGroups, err = intEnv(lookup, "RUNTIME_MAX_GROUPS", cfg.RuntimeMaxGroups); err != nil {
		return Config{}, err
	}
	if cfg.RuntimeMaxWeight, err = intEnv(lookup, "RUNTIME_MAX_WEIGHT", cfg.RuntimeMaxWeight); err != nil {
		return Config{}, err
	}
	if cfg.LeaseTTL, err = durationEnv(lookup, "LEASE_TTL", cfg.LeaseTTL); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRenewInterval, err = durationEnv(lookup, "LEASE_RENEW_INTERVAL", cfg.LeaseRenewInterval); err != nil {
		return Config{}, err
	}
	if cfg.ReconcileInterval, err = durationEnv(lookup, "RECONCILE_INTERVAL", cfg.ReconcileInterval); err != nil {
		return Config{}, err
	}
	if cfg.HeartbeatInterval, err = durationEnv(lookup, "HEARTBEAT_INTERVAL", cfg.HeartbeatInterval); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationEnv(lookup, "SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.InputStatusReportInterval, err = durationEnv(
		lookup, "INPUT_STATUS_REPORT_INTERVAL", cfg.InputStatusReportInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.InputReconcileInterval, err = durationEnv(
		lookup, "INPUT_RECONCILE_INTERVAL", cfg.InputReconcileInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.TradeBatchSize, err = intEnv(lookup, "TRADE_BATCH_SIZE", cfg.TradeBatchSize); err != nil {
		return Config{}, err
	}
	if cfg.TradeBatchFlushInterval, err = durationEnv(
		lookup, "TRADE_BATCH_FLUSH_INTERVAL", cfg.TradeBatchFlushInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.KlineBatchSize, err = intEnv(lookup, "KLINE_BATCH_SIZE", cfg.KlineBatchSize); err != nil {
		return Config{}, err
	}
	if cfg.KlineBatchFlushInterval, err = durationEnv(
		lookup, "KLINE_BATCH_FLUSH_INTERVAL", cfg.KlineBatchFlushInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.OrderBookDeltaBatchSize, err = intEnv(
		lookup, "ORDERBOOK_DELTA_BATCH_SIZE", cfg.OrderBookDeltaBatchSize,
	); err != nil {
		return Config{}, err
	}
	if cfg.OrderBookDeltaFlushInterval, err = durationEnv(
		lookup, "ORDERBOOK_DELTA_FLUSH_INTERVAL", cfg.OrderBookDeltaFlushInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.OrderBookSnapshotInterval, err = durationEnv(
		lookup, "ORDERBOOK_SNAPSHOT_INTERVAL", cfg.OrderBookSnapshotInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.OrderBookDeltaRetention, err = durationEnv(
		lookup, "ORDERBOOK_DELTA_RETENTION", cfg.OrderBookDeltaRetention,
	); err != nil {
		return Config{}, err
	}
	if cfg.OrderBookDeltaPartitionInterval, err = durationEnv(
		lookup, "ORDERBOOK_DELTA_PARTITION_INTERVAL", cfg.OrderBookDeltaPartitionInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.PartitionMaintenanceInterval, err = durationEnv(
		lookup, "PARTITION_MAINTENANCE_INTERVAL", cfg.PartitionMaintenanceInterval,
	); err != nil {
		return Config{}, err
	}
	if cfg.PartitionPrecreateHorizon, err = durationEnv(
		lookup, "PARTITION_PRECREATE_HORIZON", cfg.PartitionPrecreateHorizon,
	); err != nil {
		return Config{}, err
	}

	cfg.LogLevel = stringEnv(lookup, "LOG_LEVEL", cfg.LogLevel)
	cfg.LogFormat = stringEnv(lookup, "LOG_FORMAT", cfg.LogFormat)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadFile loads configuration from defaults, an optional YAML file and the
// environment, with this precedence (low to high):
//
//	Default()  <  YAML file  <  environment variables
//
// configPath is the YAML file to read; an empty path or a missing file means
// "environment only" so the loader stays backward compatible. ${VAR} references
// inside the YAML are expanded from the environment (which may include values
// loaded from a .env file via LoadDotEnv), letting secrets such as the database
// password live outside the committed config file.
func LoadFile(configPath string, lookup LookupFunc) (Config, error) {
	if configPath == "" {
		return Load(lookup)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return Load(lookup)
		}
		return Config{}, fmt.Errorf("config: read %q: %w", configPath, err)
	}

	fileValues, err := ParseYAML(data, lookup)
	if err != nil {
		return Config{}, err
	}

	// Environment variables override file values; a non-empty env value wins,
	// otherwise the YAML-derived value is used.
	combined := func(key string) (string, bool) {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return v, true
		}
		if v, ok := fileValues[key]; ok {
			return v, true
		}
		return "", false
	}
	return Load(combined)
}

// Bootstrap is the process-level loader used by the binaries. It first loads an
// optional .env file into the environment (never overriding already-set real
// variables), then resolves the YAML config path and returns the merged
// configuration. The returned slice lists the keys that were applied from .env
// so the caller can log provenance without exposing values.
//
// Config path resolution, highest priority first:
//
//	explicitFile (e.g. a -config flag)  >  $CONFIG_FILE  >  ./config.yaml (if present)
//
// .env path resolution: $DOTENV_FILE, otherwise ./.env. Both files are optional.
func Bootstrap(explicitFile string) (Config, []string, error) {
	dotenvPath := os.Getenv("DOTENV_FILE")
	if dotenvPath == "" {
		dotenvPath = ".env"
	}
	applied, err := LoadDotEnv(dotenvPath)
	if err != nil {
		return Config{}, nil, err
	}

	cfg, err := LoadFile(resolveConfigPath(explicitFile), os.LookupEnv)
	if err != nil {
		return Config{}, applied, err
	}
	return cfg, applied, nil
}

func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if v := os.Getenv("CONFIG_FILE"); v != "" {
		return v
	}
	const def = "config.yaml"
	if _, err := os.Stat(def); err == nil {
		return def
	}
	return ""
}

// Validate checks that required fields are present and values are coherent.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseDSN) == "" {
		return fmt.Errorf("config: database DSN is required (set DATABASE_DSN or STORAGE_DSN)")
	}
	if c.Mode != ModeControlPlane && c.Mode != ModeStreamRuntime {
		return fmt.Errorf("config: invalid mode %q", c.Mode)
	}
	if c.DBMaxConns <= 0 {
		return fmt.Errorf("config: DB_MAX_CONNS must be > 0, got %d", c.DBMaxConns)
	}
	if c.DBMinConns < 0 || c.DBMinConns > c.DBMaxConns {
		return fmt.Errorf("config: DB_MIN_CONNS must be between 0 and DB_MAX_CONNS")
	}
	if c.DBConnectTimeout <= 0 {
		return fmt.Errorf("config: DB_CONNECT_TIMEOUT must be > 0")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("config: KAFKA_BROKERS must contain at least one broker")
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("config: KAFKA_BROKERS cannot contain an empty broker")
		}
	}
	if c.RuntimeMaxGroups <= 0 {
		return fmt.Errorf("config: RUNTIME_MAX_GROUPS must be > 0")
	}
	if c.RuntimeMaxWeight <= 0 {
		return fmt.Errorf("config: RUNTIME_MAX_WEIGHT must be > 0")
	}
	if c.LeaseTTL <= 0 {
		return fmt.Errorf("config: LEASE_TTL must be > 0")
	}
	if c.LeaseRenewInterval <= 0 {
		return fmt.Errorf("config: LEASE_RENEW_INTERVAL must be > 0")
	}
	if c.LeaseRenewInterval >= c.LeaseTTL {
		return fmt.Errorf("config: LEASE_RENEW_INTERVAL must be < LEASE_TTL")
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("config: RECONCILE_INTERVAL must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("config: HEARTBEAT_INTERVAL must be > 0")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("config: SHUTDOWN_TIMEOUT must be > 0")
	}
	if c.InputStatusReportInterval <= 0 {
		return fmt.Errorf("config: INPUT_STATUS_REPORT_INTERVAL must be > 0")
	}
	if c.InputReconcileInterval <= 0 {
		return fmt.Errorf("config: INPUT_RECONCILE_INTERVAL must be > 0")
	}
	for name, value := range map[string]int{
		"TRADE_BATCH_SIZE":           c.TradeBatchSize,
		"KLINE_BATCH_SIZE":           c.KlineBatchSize,
		"ORDERBOOK_DELTA_BATCH_SIZE": c.OrderBookDeltaBatchSize,
	} {
		if value <= 0 {
			return fmt.Errorf("config: %s must be > 0", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"TRADE_BATCH_FLUSH_INTERVAL":     c.TradeBatchFlushInterval,
		"KLINE_BATCH_FLUSH_INTERVAL":     c.KlineBatchFlushInterval,
		"ORDERBOOK_DELTA_FLUSH_INTERVAL": c.OrderBookDeltaFlushInterval,
		"ORDERBOOK_SNAPSHOT_INTERVAL":    c.OrderBookSnapshotInterval,
		"ORDERBOOK_DELTA_RETENTION":      c.OrderBookDeltaRetention,
		"PARTITION_MAINTENANCE_INTERVAL": c.PartitionMaintenanceInterval,
	} {
		if value <= 0 {
			return fmt.Errorf("config: %s must be > 0", name)
		}
	}
	if c.OrderBookDeltaPartitionInterval != time.Hour {
		return fmt.Errorf("config: ORDERBOOK_DELTA_PARTITION_INTERVAL must be 1h")
	}
	if c.PartitionPrecreateHorizon < 0 {
		return fmt.Errorf("config: PARTITION_PRECREATE_HORIZON must be >= 0")
	}
	return nil
}

func stringEnv(lookup LookupFunc, key, def string) string {
	if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func stringSliceEnv(lookup LookupFunc, key string, def []string) []string {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return append([]string(nil), def...)
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, strings.TrimSpace(part))
	}
	return out
}

func int32Env(lookup LookupFunc, key string, def int32) (int32, error) {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("config: %s must be an integer: %w", key, err)
	}
	return int32(n), nil
}

func intEnv(lookup LookupFunc, key string, def int) (int, error) {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("config: %s must be an integer: %w", key, err)
	}
	return n, nil
}

func durationEnv(lookup LookupFunc, key string, def time.Duration) (time.Duration, error) {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("config: %s must be a duration: %w", key, err)
	}
	return d, nil
}
