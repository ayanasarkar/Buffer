package queue

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Notifier publishes alert payloads (StudentStuckAlert, CounterIdleAlert,
// CounterAutoPausedAlert, QueueDelayAlert -- see internal/models/staff.go)
// to buffer_notifications_queue. Same gap as StaffConsumer: Express never
// declares this queue, so this process must have run at least once before
// any alert can be delivered.
type Notifier struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewNotifier(url, queueName string) (*Notifier, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq for notifications: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open notifications channel: %w", err)
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
		return nil, fmt.Errorf("failed to declare notifications queue %q: %w", queueName, err)
	}

	return &Notifier{conn: conn, channel: ch, queue: q}, nil
}

// Publish marshals payload to JSON and publishes it as a persistent
// message. payload should be one of the alert structs in
// internal/models/staff.go (or anything else JSON-serializable Express
// is expecting on this queue).
func (n *Notifier) Publish(ctx context.Context, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal notification payload: %w", err)
	}

	err = n.channel.PublishWithContext(ctx,
		"",          // default exchange
		n.queue.Name,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish notification: %w", err)
	}
	return nil
}

func (n *Notifier) Close() error {
	if err := n.channel.Close(); err != nil {
		return err
	}
	return n.conn.Close()
}
