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
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &Consumer{conn: conn, channel: ch, queue: q}, nil
}

// envelope is unmarshaled first, on every message, so we know which event
// type we're actually holding before picking a destination struct. The
// queue carries more than one shape (UserRegistrationEvent,
// JoinNextStageEvent) -- unmarshaling straight into UserRegistrationEvent,
// as this file used to, silently zero-values a JoinNextStageEvent instead
// of erroring, which then looked like a registration bug downstream.
type envelope struct {
	EventID string `json:"eventId"`
}

func (c *Consumer) Consume(ctx context.Context) (<-chan models.UserRegistrationEvent, error) {
	msgs, err := c.channel.Consume(
		c.queue.Name,
		"",
		false,
		false,
		false,
		false,
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

				var env envelope
				if err := json.Unmarshal(d.Body, &env); err != nil {
					log.Printf("queue: dropping malformed message (no eventId): %v", err)
					d.Nack(false, false)
					continue
				}

				switch env.EventID {
				case "UserRegistrationEvent":
					var evt models.UserRegistrationEvent
					if err := json.Unmarshal(d.Body, &evt); err != nil {
						log.Printf("queue: dropping malformed UserRegistrationEvent: %v", err)
						d.Nack(false, false)
						continue
					}
					select {
					case events <- evt:
						d.Ack(false)
					case <-ctx.Done():
						d.Nack(false, true)
						return
					}

				case "JoinNextStageEvent":
					// TODO(stage-advancement): the scheduler has no logic
					// yet to move an existing ticket to its next stage.
					// Discarding on purpose, loudly, so this gap stays
					// visible instead of being silently swallowed as an
					// empty registration (the previous behavior).
					log.Printf("queue: JoinNextStageEvent received but not yet handled by scheduler -- discarding: %s", string(d.Body))
					d.Nack(false, false)

				default:
					log.Printf("queue: dropping message with unknown eventId %q", env.EventID)
					d.Nack(false, false)
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
