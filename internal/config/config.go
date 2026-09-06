// Package config handles CLI flag parsing for CounterGhost.
// Maps directly to PRD §4.3's configuration flags.
package config

import "flag"

// Config holds all runtime configuration parsed from CLI flags.
type Config struct {
	Port        int    // HTTP/WebSocket port (default 8080)
	Nodes       int    // Number of simulated nodes (default 3)
	Seed        int64  // RNG seed for deterministic crash injection (default 42)
	DBDir       string // Directory for per-node SQLite files (default "./data")
	Reset       bool   // Wipe DBDir on startup for a clean demo run
	PostgresDSN string // Postgres connection string (empty = use SQLite coordinator)
	KafkaBroker string // Kafka broker address (empty = direct outbox sync)
}

// Parse reads CLI flags and returns a Config.
// Uses Go's standard flag package — no external dependency needed.
func Parse() Config {
	cfg := Config{}
	flag.IntVar(&cfg.Port, "port", 8080, "Dashboard/WebSocket HTTP port")
	flag.IntVar(&cfg.Nodes, "nodes", 3, "Number of simulated nodes")
	flag.Int64Var(&cfg.Seed, "seed", 42, "RNG seed for reproducible crash injection")
	flag.StringVar(&cfg.DBDir, "db-dir", "./data", "Directory for per-node SQLite files")
	flag.BoolVar(&cfg.Reset, "reset", false, "Wipe db-dir on startup for a clean demo run")
	flag.StringVar(&cfg.PostgresDSN, "postgres", "", "Postgres DSN (e.g. postgres://user:pass@localhost:5432/db?sslmode=disable)")
	flag.StringVar(&cfg.KafkaBroker, "kafka", "", "Kafka broker address (e.g. localhost:9092)")
	flag.Parse()
	return cfg
}
