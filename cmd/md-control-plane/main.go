// Command md-control-plane hosts the management API and (optionally) runs
// database migrations. At M0/M1 it wires config, logging, the pgx pool and the
// migration runner together with graceful shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"MarketDataBackend/internal/api"
	"MarketDataBackend/internal/config"
	"MarketDataBackend/internal/db"
	"MarketDataBackend/internal/logging"
	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/storage"
	"MarketDataBackend/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	configFile := flag.String("config", "", "path to YAML config file (default: ./config.yaml if present)")
	migrateUp := flag.Bool("migrate", false, "apply pending migrations then exit")
	migrateDown := flag.Bool("migrate-down", false, "roll back all migrations then exit")
	flag.Parse()

	cfg, dotenvKeys, err := config.Bootstrap(*configFile)
	if err != nil {
		return err
	}
	cfg.Mode = config.ModeControlPlane

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

	if *migrateUp || *migrateDown {
		migrator, err := db.NewMigrator(pool, migrations.FS)
		if err != nil {
			return err
		}
		if *migrateDown {
			if err := migrator.Down(ctx); err != nil {
				return err
			}
			logger.Info("migrations rolled back")
			return nil
		}
		if err := migrator.Up(ctx); err != nil {
			return err
		}
		version, err := migrator.Version(ctx)
		if err != nil {
			return err
		}
		logger.Info("migrations applied", "version", version)
		return nil
	}

	logger.Info("md-control-plane started", "http_addr", cfg.HTTPAddr)

	store := metadata.NewPostgresStore(pool)
	queryStore := storage.NewPostgresStorage(pool)
	server := api.New(store, queryStore, logger)
	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: server.Handler(),
	}

	serveErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutting down md-control-plane")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
