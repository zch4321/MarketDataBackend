package db

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration is a single versioned up/down pair loaded from the embedded SQL
// files.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

// Migrator applies and rolls back ordered SQL migrations against a pool. It
// tracks applied versions in a schema_migrations bookkeeping table.
type Migrator struct {
	pool       *pgxpool.Pool
	migrations []Migration
}

// NewMigrator loads migrations from fsys (expecting files named like
// "000001_init_metadata.up.sql" / "000001_init_metadata.down.sql").
func NewMigrator(pool *pgxpool.Pool, fsys fs.FS) (*Migrator, error) {
	migs, err := loadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	return &Migrator{pool: pool, migrations: migs}, nil
}

// Migrations returns the loaded migrations in ascending version order.
func (m *Migrator) Migrations() []Migration { return m.migrations }

// Up applies every migration that has not yet been recorded, in ascending
// order. Each migration runs atomically (BEGIN/COMMIT) together with its
// bookkeeping insert.
func (m *Migrator) Up(ctx context.Context) error {
	if err := m.ensureBookkeeping(ctx); err != nil {
		return err
	}
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return err
	}
	for _, mig := range m.migrations {
		if _, ok := applied[mig.Version]; ok {
			continue
		}
		script := fmt.Sprintf(
			"BEGIN;\n%s\n;\nINSERT INTO schema_migrations (version, name) VALUES (%d, %s);\nCOMMIT;",
			mig.Up, mig.Version, quoteLiteral(mig.Name),
		)
		if err := m.execScript(ctx, script); err != nil {
			return fmt.Errorf("db: apply migration %d (%s): %w", mig.Version, mig.Name, err)
		}
	}
	return nil
}

// Down rolls back every applied migration in descending order, leaving an empty
// schema (the bookkeeping table itself is preserved).
func (m *Migrator) Down(ctx context.Context) error {
	if err := m.ensureBookkeeping(ctx); err != nil {
		return err
	}
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return err
	}
	for i := len(m.migrations) - 1; i >= 0; i-- {
		mig := m.migrations[i]
		if _, ok := applied[mig.Version]; !ok {
			continue
		}
		script := fmt.Sprintf(
			"BEGIN;\n%s\n;\nDELETE FROM schema_migrations WHERE version = %d;\nCOMMIT;",
			mig.Down, mig.Version,
		)
		if err := m.execScript(ctx, script); err != nil {
			return fmt.Errorf("db: rollback migration %d (%s): %w", mig.Version, mig.Name, err)
		}
	}
	return nil
}

// Version returns the highest applied migration version, or 0 when none have
// been applied.
func (m *Migrator) Version(ctx context.Context) (int, error) {
	if err := m.ensureBookkeeping(ctx); err != nil {
		return 0, err
	}
	var v *int64
	if err := m.pool.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&v); err != nil {
		return 0, fmt.Errorf("db: read schema version: %w", err)
	}
	if v == nil {
		return 0, nil
	}
	return int(*v), nil
}

func (m *Migrator) ensureBookkeeping(ctx context.Context) error {
	const ddl = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    bigint PRIMARY KEY,
    name       text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`
	if _, err := m.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("db: ensure schema_migrations: %w", err)
	}
	return nil
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[int]struct{}, error) {
	rows, err := m.pool.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("db: list applied migrations: %w", err)
	}
	defer rows.Close()

	applied := map[int]struct{}{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[int(v)] = struct{}{}
	}
	return applied, rows.Err()
}

// execScript runs a multi-statement SQL script using the simple query protocol,
// which (unlike the extended protocol) allows several statements in one round
// trip. This is required for DDL scripts and embedded BEGIN/COMMIT blocks.
func (m *Migrator) execScript(ctx context.Context, script string) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	mrr := conn.Conn().PgConn().Exec(ctx, script)
	return mrr.Close()
}

func loadMigrations(fsys fs.FS) ([]Migration, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("db: glob migrations: %w", err)
	}

	byVersion := map[int]*Migration{}
	for _, name := range names {
		base := path.Base(name)
		version, shortName, direction, err := parseMigrationName(base)
		if err != nil {
			return nil, err
		}
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("db: read migration %s: %w", base, err)
		}

		mig := byVersion[version]
		if mig == nil {
			mig = &Migration{Version: version, Name: shortName}
			byVersion[version] = mig
		}
		switch direction {
		case "up":
			mig.Up = string(content)
		case "down":
			mig.Down = string(content)
		}
	}

	out := make([]Migration, 0, len(byVersion))
	for _, mig := range byVersion {
		if strings.TrimSpace(mig.Up) == "" {
			return nil, fmt.Errorf("db: migration %d (%s) missing up script", mig.Version, mig.Name)
		}
		if strings.TrimSpace(mig.Down) == "" {
			return nil, fmt.Errorf("db: migration %d (%s) missing down script", mig.Version, mig.Name)
		}
		out = append(out, *mig)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// parseMigrationName splits "000001_init_metadata.up.sql" into
// (1, "init_metadata", "up").
func parseMigrationName(base string) (version int, name string, direction string, err error) {
	parts := strings.Split(base, ".")
	if len(parts) != 3 || parts[2] != "sql" {
		return 0, "", "", fmt.Errorf("db: invalid migration filename %q", base)
	}
	direction = parts[1]
	if direction != "up" && direction != "down" {
		return 0, "", "", fmt.Errorf("db: migration %q must be .up.sql or .down.sql", base)
	}

	stem := parts[0]
	underscore := strings.IndexByte(stem, '_')
	if underscore <= 0 {
		return 0, "", "", fmt.Errorf("db: migration %q must be <version>_<name>", base)
	}
	version, err = strconv.Atoi(stem[:underscore])
	if err != nil {
		return 0, "", "", fmt.Errorf("db: migration %q has non-numeric version: %w", base, err)
	}
	name = stem[underscore+1:]
	return version, name, direction, nil
}

// quoteLiteral safely single-quotes a string literal for inline SQL. Migration
// names are controlled (alphanumeric + underscore) but we quote defensively.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
