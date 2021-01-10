package eventutil

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/Jaskaranbir/es-bank-account/model"
)

// EventStore is event-storage for a specific aggregate.
type EventStore interface {
	Insert(event model.Event) error
	Fetch(aggID string) ([]model.Event, error)
	// FetchByIndex allows fetching the events with index greater
	// than provided index.
	// Index is incremented on every event-insertion into event-store.
	FetchByIndex(index int) ([]model.Event, error)
}

// MemoryEventStore is in-memory EventStore without persistence.
// Use #NewMemoryEventStore to create new instance.
type MemoryEventStore struct {
	store       map[string][]model.Event
	eventsIndex []model.Event

	lock *sync.RWMutex
}

// NewMemoryEventStore creates a new
// instance of MemoryEventStore.
func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		store:       make(map[string][]model.Event),
		eventsIndex: make([]model.Event, 0),

		lock: &sync.RWMutex{},
	}
}

// Insert validates and inserts provided event into event-store.
// Duplicate events are ignored.
func (s *MemoryEventStore) Insert(event model.Event) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, e := range s.eventsIndex {
		if e.ID() == event.ID() {
			return nil
		}
	}

	if event.AggregateID() == "" {
		return errors.New("aggregate-id is blank")
	}
	if event.Time().IsZero() {
		return errors.New("time not specified")
	}

	aggEvents := s.store[event.AggregateID()]
	s.store[event.AggregateID()] = append(aggEvents, event)

	s.eventsIndex = append(s.eventsIndex, event)
	return nil
}

// Fetch provides all events for a specific aggregate.
func (s *MemoryEventStore) Fetch(aggID string) ([]model.Event, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.store[aggID], nil
}

// FetchByIndex allows fetching the events
// with index greater than provided index.
// Index is incremented on every event-insertion
// into event-store.
// Pagination/limits are absent given simplicity
// of use-case.
func (s *MemoryEventStore) FetchByIndex(index int) ([]model.Event, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	numEvents := len(s.eventsIndex)
	if index == numEvents || numEvents == 0 {
		return make([]model.Event, 0), nil
	}
	events := make([]model.Event, numEvents-index)

	for i := 0; i < numEvents-index; i++ {
		events[i] = s.eventsIndex[i+index]
	}

	return events, nil
}
