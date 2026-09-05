package server

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PesHwA07/Ascend-Finale/internal/events"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for local demo
	},
}

// wsClient tracks a single WebSocket connection.
type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

// wsHub manages all active WebSocket connections.
type wsHub struct {
	mu      sync.RWMutex
	clients map[*wsClient]bool
	bus     *events.Bus
}

func newWSHub(bus *events.Bus) *wsHub {
	return &wsHub{
		clients: make(map[*wsClient]bool),
		bus:     bus,
	}
}

// run subscribes to the event bus and broadcasts events to all WS clients.
func (h *wsHub) run() {
	ch := h.bus.Subscribe()
	for event := range ch {
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("WS: marshal error: %v", err)
			continue
		}

		h.mu.RLock()
		for client := range h.clients {
			select {
			case client.send <- data:
			default:
				// Buffer full, skip this client
			}
		}
		h.mu.RUnlock()
	}
}

// addClient registers a new WebSocket connection.
func (h *wsHub) addClient(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
}

// removeClient unregisters and closes a WebSocket connection.
func (h *wsHub) removeClient(c *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
}

// handleWebSocket is the HTTP handler for /ws.
func (h *wsHub) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS: upgrade error: %v", err)
		return
	}

	client := &wsClient{
		conn: conn,
		send: make(chan []byte, 128),
	}
	h.addClient(client)

	log.Printf("WS: client connected (%d total)", h.clientCount())

	// Send recent history on connect
	history := h.bus.History(50)
	for _, event := range history {
		data, err := json.Marshal(event)
		if err == nil {
			client.send <- data
		}
	}

	// Writer goroutine: reads from send channel, writes to WebSocket
	go func() {
		defer func() {
			conn.Close()
			h.removeClient(client)
			log.Printf("WS: client disconnected (%d remaining)", h.clientCount())
		}()

		for msg := range client.send {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	// Reader goroutine: handles pings/pongs, detects disconnection
	go func() {
		defer func() {
			h.removeClient(client)
			conn.Close()
		}()

		conn.SetReadLimit(512)
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Ping ticker to keep connection alive
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}()
}

func (h *wsHub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
