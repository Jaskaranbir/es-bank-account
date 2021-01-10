package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gopkg.in/validator.v2"
)

// EventAction represents an Event-action.
type EventAction string

// String returns string-representation of a EventAction.
func (ea EventAction) String() string {
	return string(ea)
}

// Domain events
const (
	TxnRead EventAction = "TxnRead"

	TxnCreated      EventAction = "TxnCreated"
	TxnCreateFailed EventAction = "TxnCreateFailed"

	AccountDeposited     EventAction = "AccountDeposited"
	AccountWithdrawn     EventAction = "AccountWithdrawn"
	AccountLimitExceeded EventAction = "AccountLimitExceeded"
	DuplicateTxn         EventAction = "DuplicateTxn"

	DataWritten EventAction = "DataWritten"
)

// Event represents a Command.
// Use #NewEvent to create new instance.
type Event struct {
	id             string
	aggregateID    string
	correlationKey string

	time     time.Time
	action   EventAction
	data     []byte
	isReplay bool
}

// EventCfg is config for Event.
type EventCfg struct {
	AggregateID    string `validate:"nonzero"`
	CorrelationKey string

	Time     time.Time
	Action   EventAction `validate:"nonzero"`
	Data     interface{}
	IsReplay bool
}

// NewEvent validates provided
// config and creates a new Event.
// Uses current UTC-time if time is not set.
func NewEvent(cfg *EventCfg) (Event, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return Event{}, errors.Wrap(err, "error validating config")
	}
	if cfg.Time.IsZero() {
		cfg.Time = time.Now().UTC()
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return Event{}, errors.Wrap(err, "error generating event-id")
	}

	var dataBytes []byte
	switch v := cfg.Data.(type) {
	case []byte:
		dataBytes = v
	default:
		dataBytes, err = json.Marshal(cfg.Data)
		if err != nil {
			return Event{}, errors.Wrap(err, "error json-marshalling data")
		}
	}

	return Event{
		id:             id.String(),
		aggregateID:    cfg.AggregateID,
		correlationKey: cfg.CorrelationKey,

		time:     cfg.Time,
		action:   cfg.Action,
		data:     dataBytes,
		isReplay: cfg.IsReplay,
	}, nil
}

// ID return Event-ID.
func (e Event) ID() string {
	return e.id
}

// AggregateID return Event-AggregateID.
func (e Event) AggregateID() string {
	return e.aggregateID
}

// CorrelationKey return Event-CorrelationKey.
func (e Event) CorrelationKey() string {
	return e.correlationKey
}

// Time return Event-Time.
func (e Event) Time() time.Time {
	return e.time
}

// Action return Event-Action.
func (e Event) Action() EventAction {
	return e.action
}

// Data return Event-Data.
func (e Event) Data() []byte {
	return e.data
}

// IsReplay return Event-IsReplay.
func (e Event) IsReplay() bool {
	return e.isReplay
}
