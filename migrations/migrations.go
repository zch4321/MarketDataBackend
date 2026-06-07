// Package migrations embeds the ordered SQL migration files so they can be
// applied by internal/db.Migrator.
package migrations

import "embed"

// FS holds every *.sql migration file.
//
//go:embed *.sql
var FS embed.FS
