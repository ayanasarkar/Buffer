package models

import "time"

// --- Staff event contract (buffer_staff_queue) ---
//
// ASSUMPTION (confirm against Express's staffController.js / rabbitmq.js):
// Staff events are keyed by "type", not "eventId" like UserRegistrationEvent
// on the registration queue -- this was confirmed for the discriminator
// itself in a previous review, but the exact payload field names below
// (ticketId, counterId, status, delayMins) are inferred from Souvik's spec
// message, not read directly off the wire. If a field name is wrong, Go's
// json.Unmarshal will NOT error -- it will silently zero-value that field.
// Treat every empty string / zero value coming out of these structs as
// "maybe wrong field name", not "maybe legitimately absent", until this is
// confirmed once against a real captured message.

// StaffEventType is the "type" discriminator on every message published
// to buffer_staff_queue.
type StaffEventType string

const (
	EventCompleteStudent   StaffEventType = "COMPLETE_STUDENT"
	EventSkipStudent       StaffEventType = "SKIP_STUDENT"
	EventRestoreStudent    StaffEventType = "RESTORE_STUDENT"
	EventUpdateCounterStat StaffEventType = "UPDATE_COUNTER_STATUS"

	// EventCallNext is NOT in Souvik's checklist message. It's added
	// because the ticker's "stuck" check and the peer-comparison delay
	// check both need to know the moment a ticket actually entered
	// PROCESSING, and nothing else on the wire currently tells us that.
	// Confirm Express actually publishes this on "Call Next" -- if it
	// doesn't yet, that's a message to send Souvik, not a Go bug.
	EventCallNext StaffEventType = "CALL_NEXT"
)

// StaffEventEnvelope is unmarshaled first on every buffer_staff_queue
// message, mirroring the eventId-envelope pattern already used for the
// registration queue (see queue/consumer.go's `envelope` type).
type StaffEventEnvelope struct {
	Type StaffEventType `json:"type"`
}

// StudentActionEvent covers COMPLETE_STUDENT, SKIP_STUDENT,
// RESTORE_STUDENT, and CALL_NEXT -- all four name a ticket and the
// counter it's sitting at.
type StudentActionEvent struct {
	Type      StaffEventType `json:"type"`
	TicketID  string         `json:"ticketId"`
	CounterID string         `json:"counterId"`
	LegacyUserID string `json:"user_id"` // CONFIRMED: completeStudent (feature/express-gateway @4e52b5c) sends the ticket ID under this key instead of "ticketId" -- OnComplete falls back to it; ask Souvik to rename the key so this field can be deleted
}

// CounterStatus is a Counter's claimed/unclaimed state.
type CounterStatus string

const (
	CounterOpen   CounterStatus = "OPEN"
	CounterPaused CounterStatus = "PAUSED"
)

// CounterStatusEvent covers UPDATE_COUNTER_STATUS.
type CounterStatusEvent struct {
	Type      StaffEventType `json:"type"`
	CounterID string         `json:"counterId"`
	Status    CounterStatus  `json:"status"`
}

// --- Notifications (buffer_notifications_queue) ---
// These payload shapes are copied verbatim from Souvik's spec message for
// the first three -- field names here should NOT need confirmation.
// CounterAutoPausedAlert's "type" string was NOT given verbatim by Souvik
// (he only described the behavior: "publish a final Critical Alert"), so
// that one type string IS a guess -- confirm the exact string he wants
// Express to match on.

type StudentStuckAlert struct {
	Type      string `json:"type"` // "STUDENT_STUCK_ALERT" -- given verbatim
	CounterID string `json:"counterId"`
	TicketID  string `json:"ticketId"`
}

type CounterIdleAlert struct {
	Type      string `json:"type"` // "COUNTER_IDLE_ALERT" -- given verbatim
	CounterID string `json:"counterId"`
}

// CounterAutoPausedAlert is the "final Critical Alert" fired when a counter
// hits the 10-minute idle auto-pause. ASSUMPTION: the type string itself.
type CounterAutoPausedAlert struct {
	Type      string `json:"type"` // GUESS: "COUNTER_AUTO_PAUSED_CRITICAL"
	CounterID string `json:"counterId"`
}

type QueueDelayAlert struct {
	Type      string `json:"type"` // "QUEUE_DELAY_ALERT" -- given verbatim
	CounterID string `json:"counterId"`
	StageID   string `json:"stageId"` // NEW: checkDelay already has this in scope, Express needs it to message correctly
	DelayMins int    `json:"delayMins"`
}

// --- Ticket / Counter / Stage read models ---
//
// ASSUMPTION: every table and column name referenced by these structs (and
// by the raw SQL in internal/db/tickets.go that fills them) is INFERRED
// from domain terms in Souvik's spec (Ticket, Counter, Stage,
// assignedCounterId) -- I have not read the real schema.prisma. Prisma
// quotes identifiers in the exact case of the model/field name it was
// given, so raw SQL against these tables must double-quote them in that
// same camelCase, or Postgres will silently look for the all-lowercase
// version instead and fail with "relation/column does not exist". If the
// real names differ at all, internal/models/staff.go (these structs) and
// internal/db/tickets.go (the SQL) are the only two places that need to
// change to match.

type TicketStatus string

const (
	TicketWaiting    TicketStatus = "WAITING"
	TicketProcessing TicketStatus = "PROCESSING"
	TicketCompleted  TicketStatus = "COMPLETED"
	TicketSkipped    TicketStatus = "SKIPPED"
)

type Ticket struct {
	ID                  string
	UserID              string
	Status              TicketStatus
	AssignedCounterID   *string
	CurrentStageID      *string
	ProcessingStartedAt *time.Time
	CreatedAt           time.Time
}

type Counter struct {
	ID      string
	StageID string
	Status  CounterStatus
}

type Stage struct {
	ID   string
	Name string
	// EstimatedDurationMins is NOT read from the database -- see the
	// DefaultEstimatedDurationMins constant below. Confirmed absent from
	// schema.prisma as of the last review; a one-line migration to add it
	// is included in the accompanying chat message.
	EstimatedDurationMins int
}

// DefaultEstimatedDurationMins stands in for Stage.EstimatedDurationMins
// until that column exists for real. This is the single place to change
// once the migration lands, aside from wiring GetStage() in tickets.go to
// actually select the real column.
const DefaultEstimatedDurationMins = 10
