// Package server provides the HTTP server that serves the embedded
// dashboard and hosts API endpoints for the simulation.
// Uses go:embed to bake the HTML/JS/CSS directly into the binary —
// no separate file serving, no build step, zero deployment risk.
package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"github.com/PesHwA07/Ascend-Finale/internal/model"
	"github.com/PesHwA07/Ascend-Finale/internal/simulation"
)

// stateResponse is the JSON shape for GET /api/state.
type stateResponse struct {
	Nodes              []model.NodeState `json:"nodes"`
	NaiveGlobal        int64             `json:"naive_global"`
	AuthoritativeGlobal int64            `json:"authoritative_global"`
}

// applyRequest is the JSON body for POST /api/apply.
type applyRequest struct {
	NodeID string `json:"node_id"`
	Amount int64  `json:"amount"`
	Count  int    `json:"count"` // how many operations to apply
}

// applyResponse is the JSON shape for POST /api/apply.
type applyResponse struct {
	Applied             int               `json:"applied"`
	NodeState           model.NodeState   `json:"node_state"`
	NaiveGlobal         int64             `json:"naive_global"`
	AuthoritativeGlobal int64             `json:"authoritative_global"`
}

// Start launches the HTTP server on the given address.
// dashboardFS is the embedded filesystem containing dashboard assets,
// passed in from main.go where the go:embed directive lives.
// sim is the simulation engine providing node access and aggregation.
func Start(addr string, dashboardFS embed.FS, sim *simulation.Simulation) error {
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

	// GET /api/state — returns all node states + both global aggregations
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		authGlobal, err := sim.AuthoritativeGlobalValue()
		if err != nil {
			log.Printf("ERROR: authoritative global value: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		resp := stateResponse{
			Nodes:               sim.AllStates(),
			NaiveGlobal:         sim.NaiveGlobalValue(),
			AuthoritativeGlobal: authGlobal,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// POST /api/apply — apply N operations to a specific node
	mux.HandleFunc("/api/apply", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req applyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		// Validate
		if req.NodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}
		if req.Count <= 0 {
			req.Count = 1 // default to 1 operation
		}
		if req.Amount == 0 {
			req.Amount = 1 // default to +1 delta
		}

		n := sim.GetNode(req.NodeID)
		if n == nil {
			http.Error(w, fmt.Sprintf("node %q not found", req.NodeID), http.StatusNotFound)
			return
		}

		ops, err := n.ApplyN(req.Count, req.Amount)
		if err != nil {
			log.Printf("ERROR: apply to %s: %v", req.NodeID, err)
			http.Error(w, fmt.Sprintf("apply failed: %v", err), http.StatusInternalServerError)
			return
		}

		authGlobal, _ := sim.AuthoritativeGlobalValue()

		resp := applyResponse{
			Applied:             len(ops),
			NodeState:           n.GetState(),
			NaiveGlobal:         sim.NaiveGlobalValue(),
			AuthoritativeGlobal: authGlobal,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// POST /api/crash — simulate partial state loss on a node
	mux.HandleFunc("/api/crash", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			NodeID      string `json:"node_id"`
			DeleteCount int    `json:"delete_count"` // 0 = auto (~40%)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if req.NodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}

		result, err := sim.CrashNode(req.NodeID, req.DeleteCount)
		if err != nil {
			log.Printf("ERROR: crash %s: %v", req.NodeID, err)
			http.Error(w, fmt.Sprintf("crash failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// GET /api/manifest — compute and return a node's manifest (derived from op log)
	mux.HandleFunc("/api/manifest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		nodeID := r.URL.Query().Get("node_id")
		if nodeID == "" {
			http.Error(w, "node_id query param required", http.StatusBadRequest)
			return
		}

		manifest, err := sim.BuildManifest(nodeID)
		if err != nil {
			log.Printf("ERROR: manifest %s: %v", nodeID, err)
			http.Error(w, fmt.Sprintf("manifest failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(manifest)
	})

	// POST /api/reconcile — detect and repair partial state loss on a node
	mux.HandleFunc("/api/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if req.NodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}

		result, err := sim.Reconcile(req.NodeID)
		if err != nil {
			log.Printf("ERROR: reconcile %s: %v", req.NodeID, err)
			http.Error(w, fmt.Sprintf("reconcile failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// POST /api/replay — simulate delayed re-delivery of pre-crash operations (Scenario B)
	mux.HandleFunc("/api/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if req.NodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}

		result, err := sim.ReplayDelayedOps(req.NodeID)
		if err != nil {
			log.Printf("ERROR: replay %s: %v", req.NodeID, err)
			http.Error(w, fmt.Sprintf("replay failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
