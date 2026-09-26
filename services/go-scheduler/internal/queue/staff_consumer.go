package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"go-scheduler/internal/models"
)

// StaffConsumer listens on buffer_staff_queue. Unlike Consumer (the
// registration-queue consumer, keyed on "eventId"), staff events are keyed
// on "type" -- see models.StaffEventEnvelope's comment for why these are
// two separate envelope shapes instead of one.
//
// GAP CONFIRMED IN A PREVIOUS REVIEW: neither buffer_staff_queue nor
// buffer_notifications_queue is ever declared by Express. In RabbitMQ, a
// message published to a queue that doesn't exist yet is simply dropped --
// there's no error on the publishing side. That means this consumer's
// QueueDeclare call is not just idempotent boilerplate here: if this
// process hasn't run at least once before a staff button is clicked,
// that click's message is gone, not queued. Make sure go-scheduler is
// running before demoing the staff panel.
type StaffConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewStaffConsumer(url, queueName string) (*StaffConsumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq for staff queue: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open staff channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare staff queue %q: %w", queueName, err)
	}

	return &StaffConsumer{conn: conn, channel: ch, queue: q}, nil
}

// StaffHandlers is the set of callbacks StaffConsumer.Run dispatches to.
// A nil handler for an event type that arrives is logged and the message
// is still acked (dropping it), rather than crashing the process or
// redelivering forever -- wire up every field before relying on this in
// a demo.
type StaffHandlers struct {
	OnComplete     func(ctx context.Context, evt models.StudentActionEvent)
	OnSkip         func(ctx context.Context, evt models.StudentActionEvent)
	OnRestore      func(ctx context.Context, evt models.StudentActionEvent)
	OnCallNext     func(ctx context.Context, evt models.StudentActionEvent)
	OnCounterState func(ctx context.Context, evt models.CounterStatusEvent)
}

// Run blocks, consuming and dispatching until ctx is cancelled or the
// underlying delivery channel closes (e.g. connection lost).
func (c *StaffConsumer) Run(ctx context.Context, h StaffHandlers) error {
	msgs, err := c.channel.Consume(
		c.queue.Name,
		"",
		false, // manual ack -- we want to nack malformed/unknown messages
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register staff consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("staff queue delivery channel closed")
			}

			var env models.StaffEventEnvelope
			if err := json.Unmarshal(d.Body, &env); err != nil {
				log.Printf("staff-queue: dropping malformed message (no type field): %v", err)
				d.Nack(false, false)
				continue
			}

			switch env.Type {
			case models.EventCompleteStudent, models.EventSkipStudent,
				models.EventRestoreStudent, models.EventCallNext:
				var evt models.StudentActionEvent
				if err := json.Unmarshal(d.Body, &evt); err != nil {
					log.Printf("staff-queue: dropping malformed %s: %v", env.Type, err)
					d.Nack(false, false)
					continue
				}
				dispatchStudentAction(ctx, h, evt)
				d.Ack(false)

			case models.EventUpdateCounterStat:
				var evt models.CounterStatusEvent
				if err := json.Unmarshal(d.Body, &evt); err != nil {
					log.Printf("staff-queue: dropping malformed UPDATE_COUNTER_STATUS: %v", err)
					d.Nack(false, false)
					continue
				}
				if h.OnCounterState != nil {
					h.OnCounterState(ctx, evt)
				} else {
					log.Printf("staff-queue: UPDATE_COUNTER_STATUS received but no handler wired up")
				}
				d.Ack(false)

			default:
				log.Printf("staff-queue: dropping message with unknown type %q", env.Type)
				d.Nack(false, false)
			}
		}
	}
}

func dispatchStudentAction(ctx context.Context, h StaffHandlers, evt models.StudentActionEvent) {
	var fn func(context.Context, models.StudentActionEvent)
	switch evt.Type {
	case models.EventCompleteStudent:
		fn = h.OnComplete
	case models.EventSkipStudent:
		fn = h.OnSkip
	case models.EventRestoreStudent:
		fn = h.OnRestore
	case models.EventCallNext:
		fn = h.OnCallNext
	}
	if fn == nil {
		log.Printf("staff-queue: %s received but no handler wired up (ticketId=%s)", evt.Type, evt.TicketID)
		return
	}
	fn(ctx, evt)
}

func (c *StaffConsumer) Close() error {
	if err := c.channel.Close(); err != nil {
		return err
	}
	return c.conn.Close()
}
