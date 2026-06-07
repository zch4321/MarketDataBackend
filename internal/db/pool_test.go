package db

import (
	"context"
	"testing"
	"time"
)

// TestNewPoolInvalidDSN verifies the pool init failure path without any
// network access: an unparseable DSN must error immediately.
func TestNewPoolInvalidDSN(t *testing.T) {
	ctx := context.Background()
	_, err := NewPool(ctx, PoolConfig{
		DSN:            "://not a valid dsn",
		MaxConns:       4,
		ConnectTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

// TestNewPoolUnreachable verifies that a syntactically valid DSN pointing at an
// unreachable host fails on Ping within the connect timeout.
func TestNewPoolUnreachable(t *testing.T) {
	ctx := context.Background()
	_, err := NewPool(ctx, PoolConfig{
		// Reserved TEST-NET-1 address; should never accept connections.
		DSN:            "postgres://u:p@192.0.2.1:5432/db?sslmode=disable&connect_timeout=1",
		MaxConns:       4,
		ConnectTimeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("expected error pinging unreachable host")
	}
}
