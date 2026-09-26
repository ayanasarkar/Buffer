package staff

import (
	"context"
	"log"
	"sync"
	"time"

	"go-scheduler/internal/db"
	"go-scheduler/internal/models"
	"go-scheduler/internal/queue"
)

const (
	// delayWindow is how far back AvgProcessingMinutes looks when
	// computing a counter's recent performance for the peer-comparison
	// check. ASSUMPTION: 1 hour is a guess at a reasonable "recent
	// performance" window, not specified in Souvik's spec.
	delayWindow = 1 * time.Hour
	// delayCooldown stops QUEUE_DELAY_ALERT from firing every single
	// minute once a counter crosses the threshold -- not in the spec,
	// added so one slow counter doesn't flood buffer_notifications_queue.
	delayCooldown = 5 * time.Minute
	// delayMinSample is the minimum completed-ticket count (for the
	// counter AND at least one peer) before trusting an average enough to
	// alert on it -- avoids a false "3x slower" alert off a single
	// completion. Not specified in the spec.
	delayMinSample = 2
	// delayThreshold matches Souvik's "running 3x slower" language.
	delayThreshold = 3.0
)

// Ticker runs Souvik's "Zombie Counter" background loop: every interval,
// check every OPEN counter for stuck (staff forgot Complete), idle (staff
// forgot Call Next), and -- across each stage's counters -- unusually slow
// relative to peers.
//
// All threshold-tracking state (stuckAlerted, idleSince, idleAlerted,
// delayCooldown) lives in memory only. A restart of go-scheduler forgets
// which thresholds have already fired and starts re-evaluating from
// scratch -- acceptable for a hackathon demo, but flag it if this needs to
// survive restarts later (would need a Postgres table instead of maps).
type Ticker struct {
	store    *db.TicketStore
	notifier *queue.Notifier
	interval time.Duration

	mu            sync.Mutex
	stuckAlerted  map[string]int       // ticketID -> highest overrun threshold (minutes) already alerted
	idleSince     map[string]time.Time // counterID -> when it was first observed empty-with-waiters
	idleAlerted   map[string]int       // counterID -> highest idle threshold (5 or 10) already alerted
	delayCooldown map[string]time.Time // counterID -> last time a QUEUE_DELAY_ALERT fired
}

func NewTicker(store *db.TicketStore, notifier *queue.Notifier, interval time.Duration) *Ticker {
	return &Ticker{
		store:         store,
		notifier:      notifier,
		interval:      interval,
		stuckAlerted:  make(map[string]int),
		idleSince:     make(map[string]time.Time),
		idleAlerted:   make(map[string]int),
		delayCooldown: make(map[string]time.Time),
	}
}

// Run blocks until ctx is cancelled.
func (t *Ticker) Run(ctx context.Context) {
	tk := time.NewTicker(t.interval)
	defer tk.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.tick(ctx)
		}
	}
}

func (t *Ticker) tick(ctx context.Context) {
	counters, err := t.store.ListOpenCounters(ctx)
	if err != nil {
		log.Printf("ticker: failed to list open counters: %v", err)
		return
	}

	byStage := make(map[string][]models.Counter)
	for _, c := range counters {
		t.checkStuck(ctx, c)
		t.checkIdle(ctx, c)
		byStage[c.StageID] = append(byStage[c.StageID], c)
	}
	for stageID, group := range byStage {
		t.checkDelay(ctx, stageID, group)
	}
}

// checkStuck implements 4A: a student sitting with staff who forgot to
// click Complete. Fires at overrun+5min, then again every further 5min
// ("Loop: Bump the threshold to 10 mins... push ETAs by another 5 mins
// and fire the alert again" -- read as repeating indefinitely, not just
// once at 10min).
func (t *Ticker) checkStuck(ctx context.Context, c models.Counter) {
	proc, err := t.store.GetProcessingTicket(ctx, c.ID)
	if err != nil {
		log.Printf("ticker: stuck-check counter=%s: %v", c.ID, err)
		return
	}
	if proc == nil || proc.ProcessingStartedAt == nil {
		return
	}

	stage, err := t.store.GetStage(ctx, c.StageID)
	if err != nil {
		log.Printf("ticker: stuck-check counter=%s: %v", c.ID, err)
		return
	}

	elapsed := time.Since(*proc.ProcessingStartedAt)
	overrun := elapsed - time.Duration(stage.EstimatedDurationMins)*time.Minute
	if overrun < 5*time.Minute {
		return
	}
	threshold := int(overrun/(5*time.Minute)) * 5

	t.mu.Lock()
	already := t.stuckAlerted[proc.ID]
	if threshold <= already {
		t.mu.Unlock()
		return
	}
	t.stuckAlerted[proc.ID] = threshold
	t.mu.Unlock()

	if _, err := t.store.BumpCounterETAs(ctx, c.ID, 5); err != nil {
		log.Printf("ticker: stuck counter=%s ticket=%s: eta bump failed: %v", c.ID, proc.ID, err)
	}
	if err := t.notifier.Publish(ctx, models.StudentStuckAlert{
		Type:      "STUDENT_STUCK_ALERT",
		CounterID: c.ID,
		TicketID:  proc.ID,
	}); err != nil {
		log.Printf("ticker: stuck counter=%s ticket=%s: alert publish failed: %v", c.ID, proc.ID, err)
	}
	log.Printf("ticker: STUCK counter=%s ticket=%s overrun=%s threshold=%dm", c.ID, proc.ID, elapsed, threshold)
}

// checkIdle implements 4B: staff clicked Complete but never Call Next,
// leaving people waiting with no one being served. Auto-pauses the
// counter at 10 minutes.
func (t *Ticker) checkIdle(ctx context.Context, c models.Counter) {
	proc, err := t.store.GetProcessingTicket(ctx, c.ID)
	if err != nil {
		log.Printf("ticker: idle-check counter=%s: %v", c.ID, err)
		return
	}
	if proc != nil {
		t.mu.Lock()
		delete(t.idleSince, c.ID)
		delete(t.idleAlerted, c.ID)
		t.mu.Unlock()
		return
	}

	waiting, err := t.store.ListWaitingTickets(ctx, c.ID)
	if err != nil {
		log.Printf("ticker: idle-check counter=%s: %v", c.ID, err)
		return
	}
	if len(waiting) == 0 {
		return // empty AND nobody waiting isn't a problem
	}

	t.mu.Lock()
	since, tracked := t.idleSince[c.ID]
	if !tracked {
		t.idleSince[c.ID] = time.Now()
		t.mu.Unlock()
		return
	}
	elapsed := time.Since(since)
	already := t.idleAlerted[c.ID]
	t.mu.Unlock()

	if elapsed >= 10*time.Minute {
		if already >= 10 {
			return
		}
		t.mu.Lock()
		t.idleAlerted[c.ID] = 10
		delete(t.idleSince, c.ID)
		t.mu.Unlock()

		if _, err := t.store.BumpCounterETAs(ctx, c.ID, 5); err != nil {
			log.Printf("ticker: idle-10 counter=%s: eta bump failed: %v", c.ID, err)
		}
		if err := t.store.SetCounterStatus(ctx, c.ID, models.CounterPaused); err != nil {
			log.Printf("ticker: idle-10 counter=%s: auto-pause failed: %v", c.ID, err)
		}
		if err := t.notifier.Publish(ctx, models.CounterAutoPausedAlert{
			Type:      "COUNTER_AUTO_PAUSED_CRITICAL",
			CounterID: c.ID,
		}); err != nil {
			log.Printf("ticker: idle-10 counter=%s: alert publish failed: %v", c.ID, err)
		}
		log.Printf("ticker: IDLE counter=%s hit 10min -- auto-paused", c.ID)
		return
	}

	if elapsed >= 5*time.Minute && already < 5 {
		t.mu.Lock()
		t.idleAlerted[c.ID] = 5
		t.mu.Unlock()

		if _, err := t.store.BumpCounterETAs(ctx, c.ID, 5); err != nil {
			log.Printf("ticker: idle-5 counter=%s: eta bump failed: %v", c.ID, err)
		}
		if err := t.notifier.Publish(ctx, models.CounterIdleAlert{
			Type:      "COUNTER_IDLE_ALERT",
			CounterID: c.ID,
		}); err != nil {
			log.Printf("ticker: idle-5 counter=%s: alert publish failed: %v", c.ID, err)
		}
		log.Printf("ticker: IDLE counter=%s hit 5min", c.ID)
	}
}

// checkDelay implements 4C: peer comparison within a stage. group is
// every OPEN counter sharing stageID.
func (t *Ticker) checkDelay(ctx context.Context, stageID string, group []models.Counter) {
	if len(group) < 2 {
		return // no peers to compare against
	}

	since := time.Now().Add(-delayWindow)
	avgs := make(map[string]float64, len(group))
	samples := make(map[string]int, len(group))

	for _, c := range group {
		avg, n, err := t.store.AvgProcessingMinutes(ctx, c.ID, since)
		if err != nil {
			log.Printf("ticker: delay-check counter=%s: %v", c.ID, err)
			continue
		}
		avgs[c.ID] = avg
		samples[c.ID] = n
	}

	for _, c := range group {
		mine, ok := avgs[c.ID]
		if !ok || samples[c.ID] < delayMinSample {
			continue
		}

		var peerTotal float64
		var peerCount int
		for _, other := range group {
			if other.ID == c.ID {
				continue
			}
			if avg, ok := avgs[other.ID]; ok && samples[other.ID] >= delayMinSample {
				peerTotal += avg
				peerCount++
			}
		}
		if peerCount == 0 || peerTotal <= 0 {
			continue
		}
		peerAvg := peerTotal / float64(peerCount)
		if peerAvg <= 0 || mine < peerAvg*delayThreshold {
			continue
		}

		t.mu.Lock()
		last, cooling := t.delayCooldown[c.ID]
		if cooling && time.Since(last) < delayCooldown {
			t.mu.Unlock()
			continue
		}
		t.delayCooldown[c.ID] = time.Now()
		t.mu.Unlock()

		delayMins := int(mine - peerAvg)
		if err := t.notifier.Publish(ctx, models.QueueDelayAlert{
			Type:      "QUEUE_DELAY_ALERT",
			CounterID: c.ID,
			StageID:   stageID,
			DelayMins: delayMins,
		}); err != nil {
			log.Printf("ticker: delay counter=%s: alert publish failed: %v", c.ID, err)
		}
		log.Printf("ticker: DELAY counter=%s avg=%.1fm peer_avg=%.1fm stage=%s", c.ID, mine, peerAvg, stageID)
	}
}
