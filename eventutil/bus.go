package eventutil

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// Bus provides interface for publishing and
// subscribing data to/from specific topics.
type Bus interface {
	Publish(msg interface{}) error
	Subscribe(action string) (<-chan interface{}, error)
	Unsubscribe(c <-chan interface{}, action string) error
	Terminate()
}

// MemoryBus is an in-memory Bus without persistence.
// Use #NewMemoryBus to create new instance.
type MemoryBus struct {
	log logger.Logger

	terminateLock *sync.RWMutex
	isTerminating bool
	subsMapLock   *sync.RWMutex
	subsLock      map[string]*sync.RWMutex

	subscriptions map[string][]*subscription
}

type subscription struct {
	channel chan interface{}
	isOpen  bool
	lock    *sync.RWMutex
}

// NewMemoryBus creates new instance of MemoryBus.
func NewMemoryBus(log logger.Logger) (*MemoryBus, error) {
	if log == nil {
		return nil, errors.New("Log cannot be nil")
	}

	return &MemoryBus{
		log: log,

		terminateLock: &sync.RWMutex{},
		isTerminating: false,

		subscriptions: make(map[string][]*subscription),
		subsMapLock:   &sync.RWMutex{},
	}, nil
}

// Publish publishes provided message on Bus.
// Message must be of model.Cmd or model.Event type.
func (b *MemoryBus) Publish(msg interface{}) error {
	if msg == nil {
		return errors.New("got nil message")
	}
	var action string
	var msgID string

	switch v := msg.(type) {
	case model.Cmd:
		action = v.Action().String()
		msgID = v.ID()
	case model.Event:
		action = v.Action().String()
		msgID = v.ID()
	default:
		return errors.New("received message of unknown type")
	}

	logPrefix := fmt.Sprintf("[Publish]: [Action: %s]:", action)

	b.terminateLock.RLock()
	if b.isTerminating {
		b.terminateLock.RUnlock()
		return errors.New("bus is terminating")
	}
	b.terminateLock.RUnlock()

	b.ensureActionChan(action)
	logPrefix = fmt.Sprintf("%s [%s]:", logPrefix, msgID)
	b.log.Debugf("%s Received message", logPrefix)

	b.log.Tracef("%s Acquiring locks", logPrefix)
	b.subsMapLock.RLock()
	subs := b.subscriptions[action]
	b.subsMapLock.RUnlock()
	b.log.Tracef("%s Releasing locks", logPrefix)

	b.log.Tracef("%s Subscribers count: %d", logPrefix, len(subs))
	if len(subs) == 0 {
		b.log.Warn("No subscribers found for action")
	}

	for _, sub := range subs {
		sub.lock.RLock()
		if sub.isOpen {
			b.log.Tracef("%s Publishing event", logPrefix)
			sub.channel <- msg
			b.log.Tracef("%s Published event", logPrefix)
		}
		sub.lock.RUnlock()
	}

	return nil
}

// Subscribe returns a receive-channel which'll
// receive data when data is published to
// specified action.
func (b *MemoryBus) Subscribe(action string) (<-chan interface{}, error) {
	if action == "" {
		return nil, errors.New("action is blank")
	}
	b.terminateLock.RLock()
	if b.isTerminating {
		b.terminateLock.RUnlock()
		return nil, errors.New("bus is terminating")
	}
	b.terminateLock.RUnlock()

	logPrefix := fmt.Sprintf("[Subscribe]: [Action: %s]:", action)

	b.terminateLock.RLock()
	if b.isTerminating {
		b.log.Tracef("%s Bus terminating, ignoring subscription-request", logPrefix)
		b.terminateLock.RUnlock()
		return nil, nil
	}
	b.terminateLock.RUnlock()

	b.ensureActionChan(action)
	sub := &subscription{
		channel: make(chan interface{}, 2),
		isOpen:  true,
		lock:    &sync.RWMutex{},
	}

	b.log.Debugf("%s Adding subscription", logPrefix)
	b.subsMapLock.Lock()
	b.subscriptions[action] = append(b.subscriptions[action], sub)
	b.subsMapLock.Unlock()
	b.log.Tracef("%s Subscription added", logPrefix)

	return sub.channel, nil
}

func (b *MemoryBus) ensureActionChan(action string) {
	b.subsMapLock.Lock()
	if b.subscriptions[action] == nil {
		b.subscriptions[action] = make([]*subscription, 0)
	}
	b.subsMapLock.Unlock()
}

// Unsubscribe removes provided subscription.
func (b *MemoryBus) Unsubscribe(c <-chan interface{}, action string) error {
	if action == "" {
		return errors.New("action is blank")
	}

	logPrefix := fmt.Sprintf("[Unsubscribe]: [Action: %s]:", action)

	b.log.Tracef("%s Unsubscribing events-topic", logPrefix)

	drainID, drainCloseSig := b.drain(c)
	defer close(drainCloseSig)
	b.log.Tracef("%s [DrainID: %s]: Started drain-routine", logPrefix, drainID)

	b.subsMapLock.Lock()
	defer b.subsMapLock.Unlock()

	subs := b.subscriptions[action]
	if len(subs) == 0 {
		return errors.New("no matching subscription found")
	}

	b.log.Tracef("%s Searching matching subscription", logPrefix)
	for i, sub := range subs {
		if c == sub.channel {
			b.log.Tracef("%s Found matching subscription", logPrefix)
			subs[i] = subs[0]

			sub.lock.Lock()
			if sub.isOpen {
				close(sub.channel)
				sub.isOpen = false
			}
			sub.lock.Unlock()

			b.subscriptions[action] = subs[1:]
			b.log.Tracef("%s Unsubscribed from events-topic", logPrefix)
			return nil
		}
	}
	b.log.Tracef("%s No matching subscriptions found", logPrefix)

	return errors.New("no matching subscription found")
}

// Terminate closes all subscriptions and terminates MemoryBus.
func (b *MemoryBus) Terminate() {
	logPrefix := fmt.Sprintf("[Terminate]:")

	b.log.Infof("%s Terminating event-bus", logPrefix)

	b.terminateLock.Lock()
	if b.isTerminating {
		b.terminateLock.Unlock()
		return
	}
	b.isTerminating = true
	b.terminateLock.Unlock()

	b.log.Tracef("%s Acquiring lock", logPrefix)
	b.subsMapLock.Lock()
	for action, subs := range b.subscriptions {
		b.log.Tracef("%s Closing events-topic: %s", logPrefix, action)

		for _, sub := range subs {
			subLogPrefix := fmt.Sprintf("%s [Action: %s]:", logPrefix, action)

			drainID, drainCloseSig := b.drain(sub.channel)
			b.log.Tracef("%s [DrainID: %s]: Started drain-routine", subLogPrefix, drainID)

			sub.lock.Lock()
			if sub.isOpen {
				close(sub.channel)
				sub.isOpen = false
			}
			sub.lock.Unlock()
			close(drainCloseSig)
		}

		b.log.Tracef("%s Closed events-topic: %s", logPrefix, action)
		b.subscriptions[action] = make([]*subscription, 0)
	}

	b.log.Tracef("%s Releasing lock", logPrefix)
	b.subsMapLock.Unlock()
	b.log.Debugf("%s Event-Bus terminated", logPrefix)
}

// drain ensures a channel/subscription doesnt block
// while it is being unsubscribed to or when the Bus
// is terminating.
func (b *MemoryBus) drain(c <-chan interface{}) (string, chan<- struct{}) {
	drainCloseSig := make(chan struct{})
	drainID, _ := uuid.NewRandom()

	go func(
		closeSig <-chan struct{},
		c <-chan interface{},
		drainID string,
	) {
		for {
			select {
			case <-closeSig:
				b.log.Tracef("[%s] Closing drain-routine", drainID)
				return
			default:
				<-c
			}
		}
	}(drainCloseSig, c, drainID.String())

	return drainID.String(), drainCloseSig
}
