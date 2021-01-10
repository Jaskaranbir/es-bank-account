package eventutil

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/Jaskaranbir/es-bank-account/model"
)

// UnpublishedLog keeps track of events that have not
// been stored into EventStore or published on Bus yet.
// This interface is intended to resolve lack of atomicity
// between storing and publishing events.
type UnpublishedLog interface {
	// Inserts an event into log.
	Insert(event model.Event) error
	// Removes an event from log.
	Pop(event model.Event) error
	// Returns all stored events in log.
	Events() ([]model.Event, error)
}

// MemoryUnpublishedLog is an in-memory
// UnpublishedLog without persistence.
// Use #NewMemoryUnpublishedLog to create new instance.
type MemoryUnpublishedLog struct {
	events []model.Event
	lock   *sync.RWMutex
}

// NewMemoryUnpublishedLog creates new instance of MemoryUnpublishedLog.
func NewMemoryUnpublishedLog() *MemoryUnpublishedLog {
	return &MemoryUnpublishedLog{
		events: make([]model.Event, 0),
		lock:   &sync.RWMutex{},
	}
}

// Insert inserts an event into log.
func (p *MemoryUnpublishedLog) Insert(event model.Event) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.events = append(p.events, event)
	return nil
}

// Pop removes an event from log.
func (p *MemoryUnpublishedLog) Pop(event model.Event) error {
	// Find element
	p.lock.RLock()
	index := -1
	for i, storedEvent := range p.events {
		if event.ID() == storedEvent.ID() {
			index = i
			break
		}
	}
	if index == -1 {
		p.lock.RUnlock()
		return errors.New("event not found in log")
	}
	p.lock.RUnlock()

	// Remove element
	p.lock.Lock()
	p.events = append(p.events[:index], p.events[index+1:]...)
	p.lock.Unlock()

	return nil
}

// Events returns all stored events in log.
func (p *MemoryUnpublishedLog) Events() ([]model.Event, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	events := make([]model.Event, len(p.events))
	index := 0

	for _, event := range p.events {
		events[index] = event
		index++
	}

	return events, nil
}
