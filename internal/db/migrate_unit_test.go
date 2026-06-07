package db

import (
	"testing"
	"testing/fstest"
)

func TestParseMigrationName(t *testing.T) {
	cases := []struct {
		in      string
		version int
		name    string
		dir     string
		wantErr bool
	}{
		{in: "000001_init_metadata.up.sql", version: 1, name: "init_metadata", dir: "up"},
		{in: "000002_init_market_data.down.sql", version: 2, name: "init_market_data", dir: "down"},
		{in: "10_x.up.sql", version: 10, name: "x", dir: "up"},
		{in: "bad.up.sql", wantErr: true},
		{in: "000001_init.sideways.sql", wantErr: true},
		{in: "000001_init.up.txt", wantErr: true},
		{in: "000001.up.sql", wantErr: true},
	}
	for _, c := range cases {
		v, n, d, err := parseMigrationName(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.in, err)
			continue
		}
		if v != c.version || n != c.name || d != c.dir {
			t.Errorf("%s: got (%d,%q,%q), want (%d,%q,%q)", c.in, v, n, d, c.version, c.name, c.dir)
		}
	}
}

func TestLoadMigrationsOrderingAndPairing(t *testing.T) {
	fsys := fstest.MapFS{
		"000002_b.up.sql":   {Data: []byte("CREATE TABLE b ();")},
		"000002_b.down.sql": {Data: []byte("DROP TABLE b;")},
		"000001_a.up.sql":   {Data: []byte("CREATE TABLE a ();")},
		"000001_a.down.sql": {Data: []byte("DROP TABLE a;")},
	}
	migs, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migs) != 2 {
		t.Fatalf("got %d migrations, want 2", len(migs))
	}
	if migs[0].Version != 1 || migs[1].Version != 2 {
		t.Fatalf("migrations not sorted ascending: %d, %d", migs[0].Version, migs[1].Version)
	}
	if migs[0].Name != "a" || migs[0].Up == "" || migs[0].Down == "" {
		t.Errorf("migration 1 not paired correctly: %+v", migs[0])
	}
}

func TestLoadMigrationsMissingDown(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_a.up.sql": {Data: []byte("CREATE TABLE a ();")},
	}
	if _, err := loadMigrations(fsys); err == nil {
		t.Fatal("expected error for migration missing its down script")
	}
}
