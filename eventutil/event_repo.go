package eventutil

import (
	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/model"
)

// EventRepo handles inserting, publishing,
// and fetching events for specific aggregate.
type EventRepo interface {
	InsertAndPublish(event model.Event) error
	FetchByIndex(index int) ([]model.Event, error)
	Fetch(aggID string) ([]model.Event, error)
}

// LoggedEventRepo is EventRepo backed by internal-log which
// ensures that event being stored is published on Bus.
// Use #NewLoggedEventRepo to create new instance.
type LoggedEventRepo struct {
	bus            Bus
	eventStore     EventStore
	unpublishedLog UnpublishedLog
}

// LoggedEventRepoCfg is config for LoggedEventRepo.
type LoggedEventRepoCfg struct {
	Bus            Bus            `validate:"nonnil"`
	EventStore     EventStore     `validate:"nonnil"`
	UnpublishedLog UnpublishedLog `validate:"nonnil"`
}

// NewLoggedEventRepo validates provided config and
// creates new instance of LoggedEventRepoCfg.
func NewLoggedEventRepo(cfg *LoggedEventRepoCfg) (*LoggedEventRepo, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error validating config")
	}

	repo := &LoggedEventRepo{
		bus:            cfg.Bus,
		eventStore:     cfg.EventStore,
		unpublishedLog: cfg.UnpublishedLog,
	}
	// Initial hydration from unpublished-log,
	// in case there was service-failure and
	// unpublished-log still has events yet
	// to be published.
	err = repo.insertAndPubFromlog()
	if err != nil {
		return nil, errors.Wrap(err, "error hydrating from unpublished-log")
	}
	return repo, nil
}

// InsertAndPublish stores provided event into
// event-store and publishes it on the Bus.
func (er *LoggedEventRepo) InsertAndPublish(event model.Event) error {
	er.unpublishedLog.Insert(event)

	err := er.insertAndPubFromlog()
	return errors.Wrap(err, "error hydrating from unpublished-log")
}

func (er *LoggedEventRepo) insertAndPubFromlog() error {
	events, err := er.unpublishedLog.Events()
	if err != nil {
		return errors.Wrap(err, "error fetching events from unpublished-log")
	}

	for _, event := range events {
		err := er.eventStore.Insert(event)
		if err != nil {
			return errors.Wrapf(err, "error inserting event in event-store: %s", event.ID())
		}

		err = er.bus.Publish(event)
		if err != nil {
			return errors.Wrapf(err, "error publishing event to bus: %s", event.ID())
		}

		err = er.unpublishedLog.Pop(event)
		if err != nil {
			return errors.Wrapf(err, "error popping event from unpublished-log: %s", event.ID())
		}
	}

	return nil
}

// Fetch provides all events for a specific aggregate.
func (er *LoggedEventRepo) Fetch(aggID string) ([]model.Event, error) {
	events, err := er.eventStore.Fetch(aggID)
	return events, errors.Wrap(err, "error fetching events from event-store")
}

// FetchByIndex allows fetching the events
// with index greater than provided index.
func (er *LoggedEventRepo) FetchByIndex(index int) ([]model.Event, error) {
	events, err := er.eventStore.FetchByIndex(index)
	return events, errors.Wrap(err, "error fetching events from event-store")
}
