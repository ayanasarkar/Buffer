package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"go-scheduler/internal/models"
)

// BatchHandler runs fairness, swap, slot assignment and the DB write for a
// group of users.
type BatchHandler func(ctx context.Context, phase models.AlgorithmPhase, users []models.UserRegistrationEvent, includeTravel bool)

// PhaseRouter holds PHASE_1 and PHASE_2 tickets in RAM until their batch
// time, and sends FCFS tickets straight through.
type PhaseRouter struct {
	mu         sync.Mutex
	queueStart time.Time
	handler    BatchHandler
	phase1     []models.UserRegistrationEvent
	phase2     []models.UserRegistrationEvent
	fired1     bool
	fired2     bool
	seen       map[string]struct{}
	timers     []*time.Timer
	stopped    bool
	wg         sync.WaitGroup
}

func NewPhaseRouter(queueStart time.Time, handler BatchHandler) *PhaseRouter {
	return &PhaseRouter{
		queueStart: queueStart,
		handler:    handler,
		seen:       make(map[string]struct{}),
	}
}

// Start arms Batch 1 at start-1h and Batch 2 at start-10m. A time already in
// the past fires immediately.
func (r *PhaseRouter) Start(ctx context.Context) {
	r.arm(ctx, models.PhaseOne, r.queueStart.Add(-Batch1Lead), true)
	r.arm(ctx, models.PhaseTwo, r.queueStart.Add(-Batch2Lead), false)
}

func (r *PhaseRouter) arm(ctx context.Context, phase models.AlgorithmPhase, at time.Time, includeTravel bool) {
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.timers = append(r.timers, time.AfterFunc(delay, func() {
		r.fire(ctx, phase, includeTravel)
	}))
}

func (r *PhaseRouter) fire(ctx context.Context, phase models.AlgorithmPhase, includeTravel bool) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	var users []models.UserRegistrationEvent
	switch phase {
	case models.PhaseOne:
		users, r.phase1, r.fired1 = r.phase1, nil, true
	case models.PhaseTwo:
		users, r.phase2, r.fired2 = r.phase2, nil, true
	}
	if len(users) == 0 {
		r.mu.Unlock()
		log.Printf("router: %s batch fired with 0 users", phase)
		return
	}
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()

	log.Printf("router: %s batch firing with %d users", phase, len(users))
	r.handler(ctx, phase, users, includeTravel)
}

// Add routes one incoming ticket by its Express-assigned phase.
func (r *PhaseRouter) Add(ctx context.Context, u models.UserRegistrationEvent) {
	if u.UserID == "" {
		log.Printf("router: dropping event with empty user_id (check JSON field names against Express)")
		return
	}

	phase := u.AlgorithmPhase
	includeTravel := false
	runNow := false

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	if _, dup := r.seen[u.UserID]; dup {
		r.mu.Unlock()
		log.Printf("router: duplicate user_id=%s ignored", u.UserID)
		return
	}
	r.seen[u.UserID] = struct{}{}

	switch phase {
	case models.PhaseOne:
		includeTravel = true
		if r.fired1 {
			runNow = true
		} else {
			r.phase1 = append(r.phase1, u)
		}
	case models.PhaseTwo:
		if r.fired2 {
			runNow = true
		} else {
			r.phase2 = append(r.phase2, u)
		}
	default:
		if phase != models.PhaseFCFS {
			log.Printf("router: user_id=%s has unknown algorithmPhase %q, treating as FCFS", u.UserID, phase)
			phase = models.PhaseFCFS
		}
		runNow = true
	}
	if runNow {
		r.wg.Add(1)
	}
	r.mu.Unlock()

	if runNow {
		defer r.wg.Done()
		r.handler(ctx, phase, []models.UserRegistrationEvent{u}, includeTravel)
	}
}

// Stop cancels pending timers and waits for any batch already running.
// Tickets still held in RAM are dropped; they stay WAITING in Postgres.
func (r *PhaseRouter) Stop() {
	r.mu.Lock()
	r.stopped = true
	for _, t := range r.timers {
		t.Stop()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

// Batch1Lead and Batch2Lead are how long before queue start each batch runs.
// They are vars only so tests can compress them (BATCH1_LEAD_SECONDS in main.go).
var (
	Batch1Lead = time.Hour
	Batch2Lead = 10 * time.Minute
)
