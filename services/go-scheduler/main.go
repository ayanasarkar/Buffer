package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"go-scheduler/internal/db"
	"go-scheduler/internal/models"
	"go-scheduler/internal/queue"
	"go-scheduler/internal/scheduler"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("main: no .env file found, relying on real environment variables (%v)", err)
	}

	rabbitURL := requireEnv("RABBITMQ_URL")
	postgresURL := requireEnv("POSTGRES_URL")
	queueName := requireEnv("QUEUE_NAME")
	slotMinutes := envInt("SLOT_DURATION_MINUTES", 10)
	queueStart := queueStartFromEnv()
	scheduler.Batch1Lead = time.Duration(envInt("BATCH1_LEAD_SECONDS", 3600)) * time.Second
	scheduler.Batch2Lead = time.Duration(envInt("BATCH2_LEAD_SECONDS", 600)) * time.Second

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := queue.NewConsumer(rabbitURL, queueName)
	if err != nil {
		log.Fatalf("main: failed to start rabbitmq consumer: %v", err)
	}
	defer consumer.Close()

	events, err := consumer.Consume(ctx)
	if err != nil {
		log.Fatalf("main: failed to consume queue: %v", err)
	}
	log.Printf("main: listening on queue %q", queueName)

	writer, err := db.NewWriter(ctx, postgresURL)
	if err != nil {
		log.Fatalf("main: failed to connect to postgres: %v", err)
	}
	defer writer.Close()

	if err := writer.Init(ctx); err != nil {
		log.Fatalf("main: failed to init postgres schema: %v", err)
	}
	log.Printf("main: postgres connected and schema ready")

	// ASSUMPTION: one queue, 8 hours long, no breaks. Replace with the real
	// queue record (start, end, breaks) once the queues table is known.
	schedule := scheduler.DaySchedule{
		Open:         queueStart,
		Close:        queueStart.Add(8 * time.Hour),
		SlotDuration: time.Duration(slotMinutes) * time.Minute,
		Breaks:       nil,
	}
	log.Printf("main: queue starts %s | batch 1 at %s | batch 2 at %s | slot=%dm",
		queueStart.Format(time.RFC3339),
		queueStart.Add(-scheduler.Batch1Lead).Format(time.Kitchen),
		queueStart.Add(-scheduler.Batch2Lead).Format(time.Kitchen),
		slotMinutes)

	results := make(chan models.ScheduledUser, 32)
	pipeline := scheduler.NewPipeline(schedule, scheduler.DefaultWeights, results)

	// context.Background() on purpose: each write applies its own timeout.
	writerErrs := writer.Run(context.Background(), results)
	writerDone := make(chan struct{})
	go func() {
		for err := range writerErrs {
			log.Printf("main: postgres write error: %v", err)
		}
		close(writerDone)
	}()

	handler := func(ctx context.Context, phase models.AlgorithmPhase, users []models.UserRegistrationEvent, includeTravel bool) {
		if phase == models.PhaseFCFS {
			for _, u := range users {
				pipeline.ProcessFCFSUser(u)
			}
			return
		}
		pipeline.ProcessPhaseBatch(users, scheduler.WeightsFor(scheduler.DefaultWeights, includeTravel))
	}

	router := scheduler.NewPhaseRouter(queueStart, handler)
	router.Start(ctx)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case evt, ok := <-events:
			if !ok {
				break loop
			}
			router.Add(ctx, evt)
		}
	}

	log.Printf("main: shutting down")
	router.Stop()
	close(results)
	<-writerDone
}

// queueStartFromEnv reads QUEUE_START (RFC3339) or, for local testing,
// QUEUE_START_IN_MINUTES (default 65).
func queueStartFromEnv() time.Time {
	if v := os.Getenv("QUEUE_START"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			log.Fatalf("main: QUEUE_START must be RFC3339, for example 2026-09-20T09:00:00+05:30: %v", err)
		}
		return t
	}
	mins := envInt("QUEUE_START_IN_MINUTES", 65)
	return time.Now().Add(time.Duration(mins) * time.Minute)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("main: required environment variable %s is not set", key)
	}
	return v
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("main: invalid int for %s=%q, using fallback %d", key, v, fallback)
		return fallback
	}
	return n
}
