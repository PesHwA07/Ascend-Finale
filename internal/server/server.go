// Package server provides the HTTP server that serves the embedded
// dashboard and will later host WebSocket connections for live updates.
// Uses go:embed to bake the HTML/JS/CSS directly into the binary —
// no separate file serving, no build step, zero deployment risk.
package server

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
)

// dashboardFS embeds the entire dashboard/ directory from the project root.
// The go:embed directive is relative to THIS file's location, but we
// reference it from the package that imports this — see the note in Start().
//
// NOTE: go:embed cannot reference files outside the package directory.
// We'll embed from main.go instead and pass the FS in. See Start() signature.

// Start launches the HTTP server on the given port.
// dashboardFS is the embedded filesystem containing dashboard assets,
// passed in from main.go where the go:embed directive lives.
func Start(port int, dashboardFS embed.FS) error {
	// Serve the dashboard directory from the embedded FS.
	// Sub into "dashboard" because the embed path includes the directory name.
	sub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		return fmt.Errorf("failed to sub into dashboard FS: %w", err)
	}

	mux := http.NewServeMux()

	// Serve static dashboard files
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// Health check endpoint — useful for verifying the server is up
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("CounterGhost dashboard: http://localhost:%d", port)
	return http.ListenAndServe(addr, mux)
}
