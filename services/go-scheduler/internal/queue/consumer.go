package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"go-scheduler/internal/models"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// NewConsumer connects to RabbitMQ and declares the queue (durable, so
// messages survive a broker restart).
func NewConsumer(url, queueName string) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &Consumer{conn: conn, channel: ch, queue: q}, nil
}

// Consume starts consuming messages and returns a channel of decoded events.
// Ack happens only after the event is successfully handed off downstream;
// malformed messages are logged and discarded (nacked, not requeued).
func (c *Consumer) Consume(ctx context.Context) (<-chan models.UserRegistrationEvent, error) {
	msgs, err := c.channel.Consume(
		c.queue.Name,
		"",    // consumer tag
		false, // auto-ack -- we ack manually after successful processing
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	events := make(chan models.UserRegistrationEvent)

	go func() {
		defer close(events)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				var evt models.UserRegistrationEvent
				if err := json.Unmarshal(d.Body, &evt); err != nil {
					log.Printf("queue: dropping malformed message: %v", err)
					d.Nack(false, false) // discard, don't requeue
					continue
				}
				select {
				case events <- evt:
					d.Ack(false)
				case <-ctx.Done():
					d.Nack(false, true) // requeue, we're shutting down
					return
				}
			}
		}
	}()

	return events, nil
}

func (c *Consumer) Close() error {
	if err := c.channel.Close(); err != nil {
		return err
	}
	return c.conn.Close()
}
