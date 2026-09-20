package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"go-scheduler/internal/models"
)

type Processor interface {
	ProcessBufferBatch(batch []models.UserRegistrationEvent)
	ProcessFCFSUser(evt models.UserRegistrationEvent)
}

type BufferWindow struct {
	Period    time.Duration
	Processor Processor

	mu     sync.Mutex
	buffer []models.UserRegistrationEvent
	closed bool
}

func NewBufferWindow(period time.Duration, p Processor) *BufferWindow {
	return &BufferWindow{
		Period:    period,
		Processor: p,
	}
}

// Run starts the buffer timer and feeds it from events. It blocks until
// events closes or ctx is cancelled -- either way, it flushes whatever's
// left in the buffer before returning, so a graceful shutdown (Ctrl+C)
// never silently drops registered users who hadn't been scheduled yet.
func (b *BufferWindow) Run(ctx context.Context, events <-chan models.UserRegistrationEvent) {
	timer := time.NewTimer(b.Period)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			b.flush()
			return

		case <-timer.C:
			b.flush()

		case evt, ok := <-events:
			if !ok {
				b.flush()
				return
			}
			b.mu.Lock()
			isClosed := b.closed
			if !isClosed {
				b.buffer = append(b.buffer, evt)
			}
			b.mu.Unlock()

			if isClosed {
				log.Printf("scheduler: FCFS registration user_id=%s", evt.UserID)
				b.Processor.ProcessFCFSUser(evt)
			}
		}
	}
}

func (b *BufferWindow) flush() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	batch := b.buffer
	b.buffer = nil
	b.mu.Unlock()

	log.Printf("scheduler: buffer window closed, processing batch of %d users", len(batch))
	b.Processor.ProcessBufferBatch(batch)
}
