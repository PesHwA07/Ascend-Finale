package main

import (
	"embed"
	"fmt"
	"log"

	"github.com/PesHwA07/Ascend-Finale/internal/config"
	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/server"
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

	// 4. Open per-node databases
	for i := 0; i < cfg.Nodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		nodeDB, err := db.OpenNodeDB(cfg.DBDir, nodeID)
		if err != nil {
			log.Fatalf("Failed to open DB for %s: %v", nodeID, err)
		}
		defer nodeDB.Close()
		log.Printf("Node DB ready: %s", nodeID)
	}

	// 5. Start HTTP server (blocks)
	log.Println("Starting HTTP server...")
	if err := server.Start(cfg.Port, dashboardFS); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
