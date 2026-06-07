package config

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// yamlConfig is the on-disk YAML schema. It is authored with human-friendly
// nesting but is flattened to the same canonical env keys used by Load so the
// YAML path and the environment path share one parsing/validation code path.
//
// Scalars that have a meaningful zero value (the *int fields) use pointers so an
// omitted key is distinguishable from an explicit 0 and simply falls back to the
// default instead of overriding it.
type yamlConfig struct {
	Mode   string `yaml:"mode"`
	Server struct {
		HTTPAddr    string `yaml:"http_addr"`
		MetricsAddr string `yaml:"metrics_addr"`
	} `yaml:"server"`
	Database struct {
		DSN            string `yaml:"dsn"`
		MaxConns       *int   `yaml:"max_conns"`
		MinConns       *int   `yaml:"min_conns"`
		ConnectTimeout string `yaml:"connect_timeout"`
	} `yaml:"database"`
	KafkaClusters map[string]struct {
		Brokers []string `yaml:"brokers"`
	} `yaml:"kafka_clusters"`
	Runtime struct {
		NodeID             string `yaml:"node_id"`
		MaxGroups          *int   `yaml:"max_groups"`
		MaxWeight          *int   `yaml:"max_weight"`
		LeaseTTL           string `yaml:"lease_ttl"`
		LeaseRenewInterval string `yaml:"lease_renew_interval"`
		ReconcileInterval  string `yaml:"reconcile_interval"`
		HeartbeatInterval  string `yaml:"heartbeat_interval"`
		ShutdownTimeout    string `yaml:"shutdown_timeout"`
	} `yaml:"runtime"`
	Processing struct {
		StatusReportInterval string `yaml:"status_report_interval"`
		InputReconcile       string `yaml:"input_reconcile_interval"`
		Batch                struct {
			Trade struct {
				Size          *int   `yaml:"size"`
				FlushInterval string `yaml:"flush_interval"`
			} `yaml:"trade"`
			Kline struct {
				Size          *int   `yaml:"size"`
				FlushInterval string `yaml:"flush_interval"`
			} `yaml:"kline"`
			OrderBookDelta struct {
				Size          *int   `yaml:"size"`
				FlushInterval string `yaml:"flush_interval"`
			} `yaml:"orderbook_delta"`
		} `yaml:"batch"`
		OrderBook struct {
			SnapshotInterval             string `yaml:"snapshot_interval"`
			DeltaRetention               string `yaml:"delta_retention"`
			DeltaPartitionInterval       string `yaml:"delta_partition_interval"`
			PartitionMaintenanceInterval string `yaml:"partition_maintenance_interval"`
			PartitionPrecreateHorizon    string `yaml:"partition_precreate_horizon"`
		} `yaml:"orderbook"`
	} `yaml:"processing"`
	Log struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"log"`
}

// ParseYAML expands ${VAR} / $VAR references in data using lookup (so secrets
// such as the database password can be supplied from the environment or a .env
// file), unmarshals the YAML, and flattens it to a map keyed by the canonical
// env names. Only keys explicitly present in the file are emitted, so YAML acts
// as an override layer above the built-in defaults.
func ParseYAML(data []byte, lookup LookupFunc) (map[string]string, error) {
	expanded := os.Expand(string(data), func(name string) string {
		if v, ok := lookup(name); ok {
			return v
		}
		return ""
	})

	var yc yamlConfig
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true) // reject unknown keys so typos surface as errors
	if err := dec.Decode(&yc); err != nil {
		// An empty file decodes to io.EOF; treat it as "no overrides".
		if err == io.EOF {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	out := make(map[string]string)
	put := func(key, value string) {
		if value != "" {
			out[key] = value
		}
	}
	putInt := func(key string, value *int) {
		if value != nil {
			out[key] = strconv.Itoa(*value)
		}
	}

	put("MODE", yc.Mode)
	put("HTTP_ADDR", yc.Server.HTTPAddr)
	put("METRICS_ADDR", yc.Server.MetricsAddr)

	put("DATABASE_DSN", yc.Database.DSN)
	putInt("DB_MAX_CONNS", yc.Database.MaxConns)
	putInt("DB_MIN_CONNS", yc.Database.MinConns)
	put("DB_CONNECT_TIMEOUT", yc.Database.ConnectTimeout)

	if cluster, ok := yc.KafkaClusters["default"]; ok && len(cluster.Brokers) > 0 {
		put("KAFKA_BROKERS", strings.Join(cluster.Brokers, ","))
	}

	put("RUNTIME_NODE_ID", yc.Runtime.NodeID)
	putInt("RUNTIME_MAX_GROUPS", yc.Runtime.MaxGroups)
	putInt("RUNTIME_MAX_WEIGHT", yc.Runtime.MaxWeight)
	put("LEASE_TTL", yc.Runtime.LeaseTTL)
	put("LEASE_RENEW_INTERVAL", yc.Runtime.LeaseRenewInterval)
	put("RECONCILE_INTERVAL", yc.Runtime.ReconcileInterval)
	put("HEARTBEAT_INTERVAL", yc.Runtime.HeartbeatInterval)
	put("SHUTDOWN_TIMEOUT", yc.Runtime.ShutdownTimeout)

	put("INPUT_STATUS_REPORT_INTERVAL", yc.Processing.StatusReportInterval)
	put("INPUT_RECONCILE_INTERVAL", yc.Processing.InputReconcile)
	putInt("TRADE_BATCH_SIZE", yc.Processing.Batch.Trade.Size)
	put("TRADE_BATCH_FLUSH_INTERVAL", yc.Processing.Batch.Trade.FlushInterval)
	putInt("KLINE_BATCH_SIZE", yc.Processing.Batch.Kline.Size)
	put("KLINE_BATCH_FLUSH_INTERVAL", yc.Processing.Batch.Kline.FlushInterval)
	putInt("ORDERBOOK_DELTA_BATCH_SIZE", yc.Processing.Batch.OrderBookDelta.Size)
	put("ORDERBOOK_DELTA_FLUSH_INTERVAL", yc.Processing.Batch.OrderBookDelta.FlushInterval)
	put("ORDERBOOK_SNAPSHOT_INTERVAL", yc.Processing.OrderBook.SnapshotInterval)
	put("ORDERBOOK_DELTA_RETENTION", yc.Processing.OrderBook.DeltaRetention)
	put("ORDERBOOK_DELTA_PARTITION_INTERVAL", yc.Processing.OrderBook.DeltaPartitionInterval)
	put("PARTITION_MAINTENANCE_INTERVAL",
		yc.Processing.OrderBook.PartitionMaintenanceInterval)
	put("PARTITION_PRECREATE_HORIZON", yc.Processing.OrderBook.PartitionPrecreateHorizon)

	put("LOG_LEVEL", yc.Log.Level)
	put("LOG_FORMAT", yc.Log.Format)

	return out, nil
}
