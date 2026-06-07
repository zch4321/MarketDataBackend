// Command md-stream-runtime hosts the lease scheduler and Kafka input workers.
// It owns a group lease before consuming any of that group's inputs.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"MarketDataBackend/internal/config"
	"MarketDataBackend/internal/db"
	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/logging"
	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/observability"
	"MarketDataBackend/internal/runtime"
	"MarketDataBackend/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	configFile := flag.String("config", "", "path to YAML config file (default: ./config.yaml if present)")
	flag.Parse()

	cfg, dotenvKeys, err := config.Bootstrap(*configFile)
	if err != nil {
		return err
	}
	cfg.Mode = config.ModeStreamRuntime

	logger := logging.New(cfg.LogLevel, cfg.LogFormat)
	if len(dotenvKeys) > 0 {
		logger.Info("loaded .env", "keys", dotenvKeys)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, db.PoolConfig{
		DSN:            cfg.DatabaseDSN,
		MaxConns:       cfg.DBMaxConns,
		MinConns:       cfg.DBMinConns,
		ConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("connected to postgres")

	hostname, _ := os.Hostname()
	podName := os.Getenv("POD_NAME")
	nodeID := resolveNodeID(cfg.RuntimeNodeID, podName, hostname)

	store := metadata.NewPostgresStore(pool)
	marketStorage := storage.NewPostgresStorage(pool)
	partitionManager, err := storage.NewOrderBookDeltaPartitionManager(
		pool,
		storage.DeltaPartitionConfig{
			Retention:           cfg.OrderBookDeltaRetention,
			MaintenanceInterval: cfg.PartitionMaintenanceInterval,
			PrecreateHorizon:    cfg.PartitionPrecreateHorizon,
		},
		logger,
	)
	if err != nil {
		return err
	}
	if err := partitionManager.Maintain(ctx); err != nil {
		return err
	}
	go partitionManager.Run(ctx)

	node := runtime.NewNode(store, runtime.NodeConfig{
		NodeID:            nodeID,
		Hostname:          hostname,
		PodName:           podName,
		MaxGroups:         cfg.RuntimeMaxGroups,
		MaxWeight:         cfg.RuntimeMaxWeight,
		LeaseTTL:          cfg.LeaseTTL,
		RenewInterval:     cfg.LeaseRenewInterval,
		ReconcileInterval: cfg.ReconcileInterval,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}, logger)
	node.EnableConsumptionWithConfig(
		marketStorage,
		kafka.NewReaderFactory(),
		cfg.KafkaBrokers,
		runtime.ConsumptionConfig{
			StatusReportEvery:   cfg.InputStatusReportInterval,
			InputReconcileEvery: cfg.InputReconcileInterval,
			TradeBatch: runtime.BatchConfig{
				Size:          cfg.TradeBatchSize,
				FlushInterval: cfg.TradeBatchFlushInterval,
			},
			KlineBatch: runtime.BatchConfig{
				Size:          cfg.KlineBatchSize,
				FlushInterval: cfg.KlineBatchFlushInterval,
			},
			OrderBookDeltaBatch: runtime.BatchConfig{
				Size:          cfg.OrderBookDeltaBatchSize,
				FlushInterval: cfg.OrderBookDeltaFlushInterval,
			},
			SnapshotInterval: cfg.OrderBookSnapshotInterval,
		},
	)

	// M10: health / readiness / metrics server.
	metricsReg := observability.NewRegistry(map[string]string{"service": "md-stream-runtime"})
	readyCheck := func() error { return pool.Ping(ctx) }
	healthMux := http.NewServeMux()
	healthMux.Handle("/healthz", observability.HealthHandler())
	healthMux.Handle("/readyz", observability.ReadinessHandler(readyCheck))
	healthMux.Handle("/metrics", metricsReg.Handler())
	healthServer := &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: healthMux,
	}
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server failed", "err", err)
		}
	}()

	logger.Info("md-stream-runtime started",
		"node_id", nodeID,
		"metrics_addr", cfg.MetricsAddr,
	)
	if err := node.Run(ctx); err != nil {
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("health shutdown: %w", err)
	}
	logger.Info("md-stream-runtime stopped")
	return nil
}

// resolveNodeID picks a stable, unique runtime node id. An explicit configured
// id wins; otherwise it derives one from POD_NAME (K8s) or the hostname plus a
// short random suffix so two processes on the same host never collide.
func resolveNodeID(configured, podName, hostname string) string {
	if configured != "" {
		return configured
	}
	base := podName
	if base == "" {
		base = hostname
	}
	if base == "" {
		base = "md-stream-runtime"
	}
	return base + "-" + randomSuffix()
}

func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(b[:])
}
