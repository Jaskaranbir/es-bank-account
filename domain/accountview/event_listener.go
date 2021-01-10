package accountview

import (
	"context"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

type eventListener struct {
	log logger.Logger

	bus                  eventutil.Bus
	accountDeposited     model.EventAction
	AccountWithdrawn     model.EventAction
	duplicateTxn         model.EventAction
	accountLimitExceeded model.EventAction
	eventSubs            map[model.EventAction]<-chan interface{}

	resultView *txnResultView
}

// EventListenerCfg is config for event-listener.
type EventListenerCfg struct {
	Log logger.Logger `validate:"nonnil"`

	Bus                  eventutil.Bus     `validate:"nonnil"`
	AccountDeposited     model.EventAction `validate:"nonzero"`
	AccountWithdrawn     model.EventAction `validate:"nonzero"`
	DuplicateTxn         model.EventAction `validate:"nonzero"`
	AccountLimitExceeded model.EventAction `validate:"nonzero"`

	ResultViewCfg *TxnResultViewCfg `validate:"nonnil"`
}

// InitEventListener validates event-listener
// config and runs event-listener.
func InitEventListener(ctx context.Context, cfg *EventListenerCfg) error {
	err := validator.Validate(cfg)
	if err != nil {
		return errors.Wrap(err, "error validating config")
	}
	if ctx == nil {
		return errors.New("context is nil")
	}

	actions := []model.EventAction{
		cfg.AccountDeposited,
		cfg.AccountWithdrawn,
		cfg.AccountLimitExceeded,
		cfg.DuplicateTxn,
	}
	eventSubs := make(map[model.EventAction]<-chan interface{})
	for _, action := range actions {
		eventSubs[action], err = cfg.Bus.Subscribe(action.String())
		if err != nil {
			return errors.Wrapf(err, "error subscribing to event-bus for action: %s", action)
		}
	}

	resultView, err := newTxnResultView(cfg.ResultViewCfg)
	if err != nil {
		return errors.Wrap(err, "error creating transaction-result view")
	}

	// Create and run listener
	cfg.Log.Infof("Starting event-listener")
	listener := &eventListener{
		log: cfg.Log,

		bus:                  cfg.Bus,
		accountDeposited:     cfg.AccountDeposited,
		AccountWithdrawn:     cfg.AccountWithdrawn,
		duplicateTxn:         cfg.DuplicateTxn,
		accountLimitExceeded: cfg.AccountLimitExceeded,
		eventSubs:            eventSubs,

		resultView: resultView,
	}

	err = listener.start(ctx)
	return errors.Wrap(err, "listener-routine exited with error")
}

func (el *eventListener) start(ctx context.Context) error {
	defer el.unsubscribe()

	for {
		select {
		case <-ctx.Done():
			el.log.Debug("Received context-done signal")
			err := el.unsubscribe()
			if err != nil {
				err = errors.Wrap(err, "error disposing instance")
			}
			return err

		case <-el.eventSubs[el.accountDeposited]:
			err := el.resultView.hydrate()
			if err != nil {
				return errors.Wrap(err, "error hydrating transaction-result view")
			}
		case <-el.eventSubs[el.AccountWithdrawn]:
			err := el.resultView.hydrate()
			if err != nil {
				return errors.Wrap(err, "error hydrating transaction-result view")
			}
		case <-el.eventSubs[el.accountLimitExceeded]:
			err := el.resultView.hydrate()
			if err != nil {
				return errors.Wrap(err, "error hydrating transaction-result view")
			}
		case <-el.eventSubs[el.duplicateTxn]:
			err := el.resultView.hydrate()
			if err != nil {
				return errors.Wrap(err, "error hydrating transaction-result view")
			}
		}
	}
}

func (el *eventListener) unsubscribe() error {
	for action, channel := range el.eventSubs {
		// Already unsubscribed
		if channel == nil {
			continue
		}

		el.log.Debugf("Unsubscribing from action: %s", action)
		err := el.bus.Unsubscribe(channel, action.String())
		if err != nil {
			return errors.Wrapf(err, "error unsubscribing from event-bus for action: %s", action)
		}
		el.eventSubs[action] = nil
		el.log.Tracef("Unsubscribed from action: %s", action)
	}
	return nil
}
