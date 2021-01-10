package account

import (
	"context"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

type cmdListener struct {
	log logger.Logger

	bus           eventutil.Bus
	processTxnCmd model.CmdAction
	cmdSubs       map[model.CmdAction]<-chan interface{}

	accountCfg *AggregateCfg
}

// CmdListenerCfg is config for command-listener.
type CmdListenerCfg struct {
	Log logger.Logger `validate:"nonnil"`

	Bus           eventutil.Bus   `validate:"nonnil"`
	ProcessTxnCmd model.CmdAction `validate:"nonzero"`

	AccountCfg *AggregateCfg `validate:"nonnil"`
}

// InitCmdListener validates command-listener
// config and runs command-listener.
func InitCmdListener(ctx context.Context, cfg *CmdListenerCfg) error {
	err := validator.Validate(cfg)
	if err != nil {
		return errors.Wrap(err, "error validating config")
	}
	if ctx == nil {
		return errors.New("context is nil")
	}

	// Subscribe to actions from Bus
	cmdSubs := make(map[model.CmdAction]<-chan interface{})
	cmdSubs[cfg.ProcessTxnCmd], err = cfg.Bus.Subscribe(cfg.ProcessTxnCmd.String())
	if err != nil {
		return errors.Wrapf(err, "error subscribing to event-bus for action: %s", cfg.ProcessTxnCmd)
	}

	// Create and run listener
	cfg.Log.Infof("Starting command-listener")
	listener := &cmdListener{
		log: cfg.Log,

		bus:           cfg.Bus,
		processTxnCmd: cfg.ProcessTxnCmd,
		cmdSubs:       cmdSubs,

		accountCfg: cfg.AccountCfg,
	}
	err = listener.start(ctx)
	return errors.Wrap(err, "listener-routine exited with error")
}

func (cl *cmdListener) start(ctx context.Context) error {
	defer cl.unsubscribe()

	for {
		select {
		case <-ctx.Done():
			cl.log.Debug("Received context-done signal")
			err := cl.unsubscribe()
			if err != nil {
				err = errors.Wrap(err, "error disposing instance")
			}
			return err

		case msg := <-cl.cmdSubs[cl.processTxnCmd]:
			// Validate message
			if msg == nil {
				continue
			}
			cmd, castSuccess := msg.(model.Cmd)
			if !castSuccess {
				cl.log.Warnf("error casting message to command")
				continue
			}
			if cmd.Data() == nil {
				continue
			}

			// Aggregate-operations
			account, err := newAccount(cl.accountCfg)
			if err != nil {
				return errors.Wrap(err, "error creating account-aggregate instance")
			}
			err = account.handleProcessTxnCmd(cmd)
			if err != nil {
				return errors.Wrap(err, "error handling process-transaction command")
			}
		}
	}
}

func (cl *cmdListener) unsubscribe() error {
	for action, channel := range cl.cmdSubs {
		// Already unsubscribed
		if channel == nil {
			continue
		}

		cl.log.Debugf("Unsubscribing from action: %s", action)
		err := cl.bus.Unsubscribe(channel, action.String())
		if err != nil {
			return errors.Wrapf(err, "error unsubscribing from event-bus for action: %s", action)
		}
		cl.cmdSubs[action] = nil
		cl.log.Tracef("Unsubscribed from action: %s", action)
	}
	return nil
}
