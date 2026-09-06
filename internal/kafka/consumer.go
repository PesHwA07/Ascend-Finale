package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/PesHwA07/Ascend-Finale/internal/db"
	"github.com/PesHwA07/Ascend-Finale/internal/model"
)

// Consumer reads operations from Kafka and writes them to the coordinator DB.
// It is the final stage of the pipeline:
//
//	Node (SQLite) → Outbox → OutboxSyncer → Kafka → Consumer → Postgres
type Consumer struct {
	reader *kafkago.Reader
	broker string
}

// NewConsumer creates a Kafka consumer for the counter.operations topic.
func NewConsumer(broker, topic, groupID string) (*Consumer, error) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,              // Fetch as soon as there's data
		MaxBytes:       10e6,           // 10MB max
		CommitInterval: time.Second,    // Auto-commit every second
		StartOffset:    kafkago.FirstOffset,
	})

	log.Printf("Kafka consumer started: broker=%s topic=%s group=%s", broker, topic, groupID)
	return &Consumer{reader: reader, broker: broker}, nil
}

// Start runs the consumer loop, reading messages and inserting into the coordinator.
// This function blocks until the context is cancelled.
func (c *Consumer) Start(ctx context.Context, coordDB *sql.DB, usePostgres bool) {
	log.Println("Kafka consumer loop running...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer stopped")
			c.reader.Close()
			return
		default:
		}

		// Read next message (blocks until available or context cancelled)
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled — clean shutdown
			}
			log.Printf("Kafka consumer read error: %v", err)
			time.Sleep(time.Second) // Back off on error
			continue
		}

		// Deserialize the operation
		var opMsg OperationMessage
		if err := json.Unmarshal(msg.Value, &opMsg); err != nil {
			log.Printf("Kafka consumer: invalid message on partition %d offset %d: %v",
				msg.Partition, msg.Offset, err)
			continue
		}

		// Convert to model.Operation
		op := model.Operation{
			OperationID: opMsg.OperationID,
			CounterID:   opMsg.CounterID,
			NodeID:      opMsg.NodeID,
			Epoch:       opMsg.Epoch,
			Sequence:    opMsg.Sequence,
			Amount:      opMsg.Amount,
			CreatedAt:   opMsg.CreatedAt,
		}

		// Insert into coordinator DB (Postgres or SQLite)
		var inserted bool
		if usePostgres {
			inserted, err = db.InsertOperationPG(coordDB, op)
		} else {
			inserted, err = db.InsertOperation(coordDB, op)
		}

		if err != nil {
			log.Printf("Kafka consumer: DB insert error for %s: %v", op.OperationID, err)
			continue
		}

		if inserted {
			log.Printf("Kafka → DB: op %s (node=%s seq=%d) inserted", op.OperationID[:8], op.NodeID, op.Sequence)
		}
		// Duplicates are silently ignored (ON CONFLICT DO NOTHING)
	}
}

// Close shuts down the consumer.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
