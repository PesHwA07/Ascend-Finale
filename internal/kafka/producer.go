// Package kafka provides Kafka producer and consumer for CounterGhost.
//
// The producer serializes operations as JSON and publishes them to the
// "counter.operations" topic. The OutboxSyncer agent uses this instead
// of writing directly to the coordinator DB.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/PesHwA07/Ascend-Finale/internal/model"
)

// Producer wraps a kafka-go writer for publishing operations.
type Producer struct {
	writer *kafkago.Writer
	broker string
}

// NewProducer creates a Kafka producer connected to the given broker.
// Topic "counter.operations" is auto-created if it doesn't exist.
func NewProducer(broker string) (*Producer, error) {
	// Ensure topic exists
	conn, err := kafkago.DialLeader(context.Background(), "tcp", broker, "counter.operations", 0)
	if err != nil {
		// Topic might not exist yet — try to create it
		log.Printf("Kafka: topic dial failed (will auto-create): %v", err)
		adminConn, dialErr := kafkago.Dial("tcp", broker)
		if dialErr != nil {
			return nil, fmt.Errorf("kafka connect failed: %w", dialErr)
		}
		defer adminConn.Close()

		topicConfig := kafkago.TopicConfig{
			Topic:             "counter.operations",
			NumPartitions:     3,
			ReplicationFactor: 1,
		}
		if createErr := adminConn.CreateTopics(topicConfig); createErr != nil {
			log.Printf("Kafka: topic create warning: %v", createErr)
			// Not fatal — topic might already exist
		}
	} else {
		conn.Close()
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(broker),
		Topic:        "counter.operations",
		Balancer:     &kafkago.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // Low latency for demo
		RequiredAcks: kafkago.RequireAll,     // acks=all for durability
	}

	log.Printf("Kafka producer connected to %s", broker)
	return &Producer{writer: writer, broker: broker}, nil
}

// OperationMessage is the JSON payload published to Kafka.
type OperationMessage struct {
	OperationID string    `json:"operation_id"`
	CounterID   string    `json:"counter_id"`
	NodeID      string    `json:"node_id"`
	Epoch       int64     `json:"epoch"`
	Sequence    int64     `json:"sequence"`
	Amount      int64     `json:"amount"`
	CreatedAt   time.Time `json:"created_at"`
}

// Publish sends an operation to the Kafka topic.
// Uses the node_id as the message key for partition affinity.
func (p *Producer) Publish(op model.Operation) error {
	msg := OperationMessage{
		OperationID: op.OperationID,
		CounterID:   op.CounterID,
		NodeID:      op.NodeID,
		Epoch:       op.Epoch,
		Sequence:    op.Sequence,
		Amount:      op.Amount,
		CreatedAt:   op.CreatedAt,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}

	err = p.writer.WriteMessages(context.Background(),
		kafkago.Message{
			Key:   []byte(op.NodeID), // Partition by node for ordering
			Value: data,
		},
	)
	if err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

// Close shuts down the Kafka producer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// Broker returns the broker address (for status display).
func (p *Producer) Broker() string {
	return p.broker
}
