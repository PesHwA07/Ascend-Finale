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
	"time"

	"github.com/PesHwA07/Ascend-Finale/internal/events"
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
// bus is the event bus for real-time WebSocket push to the dashboard.
func Start(addr string, dashboardFS embed.FS, sim *simulation.Simulation, bus *events.Bus) error {
	// Serve the dashboard directory from the embedded FS.
	// Sub into "dashboard" because the embed path includes the directory name.
	sub, err := fs.Sub(dashboardFS, "dashboard")
	if err != nil {
		return fmt.Errorf("failed to sub into dashboard FS: %w", err)
	}

	// Start WebSocket hub for real-time event push
	hub := newWSHub(bus)
	go hub.run()

	mux := http.NewServeMux()

	// Serve static dashboard files
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// WebSocket endpoint for real-time events
	mux.HandleFunc("/ws", hub.handleWebSocket)

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

		bus.Publish(events.Event{
			Type:    events.EventOperationApplied,
			NodeID:  req.NodeID,
			Message: fmt.Sprintf("Applied %d ops (amount=%d) to %s", len(ops), req.Amount, req.NodeID),
			Data: map[string]interface{}{
				"applied":       len(ops),
				"naive_global":  resp.NaiveGlobal,
				"auth_global":   resp.AuthoritativeGlobal,
			},
		})

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

		bus.Publish(events.Event{
			Type:    events.EventCrashInjected,
			NodeID:  req.NodeID,
			Message: fmt.Sprintf("CRASH %s: deleted %d ops, localVal %d → %d, epoch %d → %d",
				req.NodeID, result.DeletedCount, result.OldLocalValue, result.NewLocalValue, result.OldEpoch, result.NewEpoch),
			Data: map[string]interface{}{
				"deleted_count":   result.DeletedCount,
				"old_local_value": result.OldLocalValue,
				"new_local_value": result.NewLocalValue,
				"old_epoch":       result.OldEpoch,
				"new_epoch":       result.NewEpoch,
			},
		})

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

		bus.Publish(events.Event{
			Type:    events.EventReconciliationDone,
			NodeID:  req.NodeID,
			Message: fmt.Sprintf("RECONCILE %s: found %d missing, replayed %d, localVal %d → %d",
				req.NodeID, result.MissingCount, result.ReplayedCount, result.OldLocalValue, result.RecoveredValue),
			Data: map[string]interface{}{
				"missing_count":   result.MissingCount,
				"replayed_count":  result.ReplayedCount,
				"old_local_value": result.OldLocalValue,
				"recovered_value": result.RecoveredValue,
			},
		})

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

		bus.Publish(events.Event{
			Type:    events.EventReplayRejected,
			NodeID:  req.NodeID,
			Message: fmt.Sprintf("REPLAY %s: attempted %d, dups_ignored=%d, new_inserts=%d",
				req.NodeID, result.AttemptedCount, result.DuplicatesIgnored, result.NewInserts),
			Data: map[string]interface{}{
				"attempted_count":    result.AttemptedCount,
				"duplicates_ignored": result.DuplicatesIgnored,
				"new_inserts":        result.NewInserts,
			},
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// POST /api/rebuild — projection rebuild: wipe node DB, replay from coordinator
	mux.HandleFunc("/api/rebuild", func(w http.ResponseWriter, r *http.Request) {
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

		result, err := sim.RebuildFromLog(req.NodeID)
		if err != nil {
			log.Printf("ERROR: rebuild %s: %v", req.NodeID, err)
			http.Error(w, fmt.Sprintf("rebuild failed: %v", err), http.StatusInternalServerError)
			return
		}

		bus.Publish(events.Event{
			Type:    events.EventProjectionRebuilt,
			NodeID:  req.NodeID,
			Message: fmt.Sprintf("REBUILD %s: wiped %d ops, replayed %d from coordinator in %dms — localVal %d → %d",
				req.NodeID, result.OpsDeleted, result.OpsReplayed, result.RebuildDuration,
				result.OldLocalValue, result.NewLocalValue),
			Data: map[string]interface{}{
				"ops_deleted":      result.OpsDeleted,
				"ops_replayed":     result.OpsReplayed,
				"old_local_value":  result.OldLocalValue,
				"new_local_value":  result.NewLocalValue,
				"rebuild_duration": result.RebuildDuration,
			},
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// GET /api/operations — audit trail: list all coordinator operations
	mux.HandleFunc("/api/operations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ops, err := sim.GetCoordinatorOperations()
		if err != nil {
			log.Printf("ERROR: get operations: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ops)
	})

	// GET /api/temporal?as_of=RFC3339 — time-travel: what was the counter at time T?
	mux.HandleFunc("/api/temporal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		asOfStr := r.URL.Query().Get("as_of")
		if asOfStr == "" {
			http.Error(w, "as_of query parameter required (RFC3339 format or relative like '-30s')", http.StatusBadRequest)
			return
		}

		// Try RFC3339 first, then relative duration
		var asOf time.Time
		var err error
		asOf, err = time.Parse(time.RFC3339, asOfStr)
		if err != nil {
			// Try parsing as relative duration (e.g., "-30s", "-5m")
			d, dErr := time.ParseDuration(asOfStr)
			if dErr != nil {
				http.Error(w, "invalid as_of: use RFC3339 (e.g., 2024-01-01T00:00:00Z) or relative (e.g., -30s)", http.StatusBadRequest)
				return
			}
			asOf = time.Now().Add(d)
		}

		result, err := sim.TemporalQuery(asOf)
		if err != nil {
			log.Printf("ERROR: temporal query: %v", err)
			http.Error(w, fmt.Sprintf("temporal query failed: %v", err), http.StatusInternalServerError)
			return
		}

		bus.Publish(events.Event{
			Type:    events.EventTemporalQuery,
			Message: fmt.Sprintf("TEMPORAL: query as_of=%s → global=%d (current=%d)", asOf.Format(time.RFC3339), result.GlobalValue, result.CurrentValue),
			Data: map[string]interface{}{
				"as_of":        asOf.Format(time.RFC3339),
				"global_value": result.GlobalValue,
				"current":      result.CurrentValue,
			},
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
