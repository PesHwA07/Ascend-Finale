package main

import (
	"embed"
	"fmt"
	"log"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/server"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// Embed the dashboard directory into the binary.
// This directive MUST live in a .go file at the project root (or wherever
// the dashboard/ directory is relative to). It can't live inside internal/server
// because go:embed paths are relative to the file's own package directory.
//
//go:embed dashboard/*
var dashboardFS embed.FS

func main() {
	// 1. Parse configuration from CLI flags
	cfg := config.Parse()
	log.Printf("CounterGhost starting: nodes=%d, seed=%d, port=%d, db-dir=%s, reset=%v",
		cfg.Nodes, cfg.Seed, cfg.Port, cfg.DBDir, cfg.Reset)

	// 2. Prepare data directory (wipe if -reset flag is set)
	if err := db.EnsureDir(cfg.DBDir, cfg.Reset); err != nil {
		log.Fatalf("Failed to prepare data directory: %v", err)
	}

	// 3. Open coordinator database (authoritative operation log)
	coordDB, err := db.OpenCoordinatorDB(cfg.DBDir)
	if err != nil {
		log.Fatalf("Failed to open coordinator DB: %v", err)
	}
	defer coordDB.Close()
	log.Println("Coordinator DB ready")

	// 4. Initialize simulation — creates per-node DBs and node engines,
	//    recovering any existing state from durable storage.
	sim, err := simulation.NewSimulation(cfg, coordDB)
	if err != nil {
		log.Fatalf("Failed to initialize simulation: %v", err)
	}
	defer sim.Close()

	// Log initial state
	for _, s := range sim.AllStates() {
		log.Printf("  %s: epoch=%d, localVal=%d, alive=%v",
			s.NodeID, s.Epoch, s.LocalValue, s.IsAlive)
	}
	naive := sim.NaiveGlobalValue()
	auth, _ := sim.AuthoritativeGlobalValue()
	log.Printf("Global values — naive: %d, authoritative: %d", naive, auth)

	// 5. Start HTTP server (blocks)
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("CounterGhost dashboard: http://localhost:%d", cfg.Port)
	if err := server.Start(addr, dashboardFS, sim); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
