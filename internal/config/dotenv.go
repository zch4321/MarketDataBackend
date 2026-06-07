// .env support for local development. The parser is intentionally small and
// dependency free: it understands KEY=VALUE lines, comments, blank lines, an
// optional leading "export ", and single or double quoted values. Real process
// environment variables always win over .env so secrets injected by the
// platform (e.g. Kubernetes) are never overridden.
package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// ParseDotEnv reads KEY=VALUE pairs from r. It is pure (no environment side
// effects) so it can be unit tested in isolation. Later keys override earlier
// ones. A malformed line (no '=') is reported with its line number.
func ParseDotEnv(r io.Reader) (map[string]string, error) {
	out := make(map[string]string)
	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("config: .env line %d is not KEY=VALUE: %q", lineNo, line)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("config: .env line %d has an empty key", lineNo)
		}
		out[key] = unquoteEnvValue(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("config: read .env: %w", err)
	}
	return out, nil
}

// unquoteEnvValue strips a single matching pair of surrounding quotes. An
// unquoted value keeps any inline "#" since only a full-line comment is honored.
func unquoteEnvValue(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}
	return v
}

// LoadDotEnv loads an optional .env file into the process environment without
// overriding variables that are already set. A missing file is not an error so
// callers can always attempt it. It returns the set of keys it applied.
func LoadDotEnv(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("config: open .env %q: %w", path, err)
	}
	defer f.Close()

	pairs, err := ParseDotEnv(f)
	if err != nil {
		return nil, err
	}
	var applied []string
	for k, v := range pairs {
		if _, exists := os.LookupEnv(k); exists {
			continue // real environment wins
		}
		if err := os.Setenv(k, v); err != nil {
			return nil, fmt.Errorf("config: set %s from .env: %w", k, err)
		}
		applied = append(applied, k)
	}
	return applied, nil
}
