package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DistributionService defines the contract we need from the app layer.
type DistributionService interface {
	CalculateDistribution(ctx context.Context, requestID string) error
}

// Consumer handles RabbitMQ connection and message processing.
type Consumer struct {
	connURL string
	queue   string
	service DistributionService
	logger  *slog.Logger
}

// NewConsumer creates a new RabbitMQ consumer instance.
func NewConsumer(url, queueName string, svc DistributionService, l *slog.Logger) *Consumer {
	return &Consumer{
		connURL: url,
		queue:   queueName,
		service: svc,
		logger:  l,
	}
}

// MessageBody represents the expected JSON payload from the queue.
// According to spec: "Trigger... contains only Request ID and parameters".
type MessageBody struct {
	RequestID string `json:"request_id"`
}

// Start begins listening for messages. This is a blocking operation.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("connecting to rabbitmq", "url", c.connURL)

	// 1. Connect to RabbitMQ
	conn, err := amqp.Dial(c.connURL)
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// 2. Declare Queue (Idempotent)
	// We ensure the queue exists before listening.
	_, err = ch.QueueDeclare(
		c.queue, // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 3. Start Consuming
	msgs, err := ch.Consume(
		c.queue, // queue
		"",      // consumer tag (empty = auto-generated)
		false,   // auto-ack (FALSE! We want manual ack after processing)
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	c.logger.Info("waiting for calculation requests", "queue", c.queue)

	// 4. Message Loop
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("stopping consumer...")
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("rabbitmq channel closed")
			}

			// Process the message in a separate function to handle Acks correctly
			c.handleMessage(ctx, d)
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, d amqp.Delivery) {
	var body MessageBody
	if err := json.Unmarshal(d.Body, &body); err != nil {
		c.logger.Error("failed to unmarshal message", "error", err)
		// If JSON is invalid, retrying won't help. Reject without requeue.
		_ = d.Nack(false, false)
		return
	}

	c.logger.Info("received calculation request", "request_id", body.RequestID)

	// Set a timeout for the calculation logic
	calcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// CALL THE APP LAYER
	err := c.service.CalculateDistribution(calcCtx, body.RequestID)

	if err != nil {
		c.logger.Error("calculation failed", "request_id", body.RequestID, "error", err)
		// Logic: Should we requeue?
		// For now, let's Nack with requeue=true so another worker can try (transient error),
		// or requeue=false if it's a permanent error.
		// Let's assume transient for DB issues.
		_ = d.Nack(false, true)
	} else {
		// Success! Acknowledge the message.
		if err := d.Ack(false); err != nil {
			c.logger.Error("failed to ack message", "error", err)
		}
	}
}
