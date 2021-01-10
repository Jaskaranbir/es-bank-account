package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// processMgr handles coordinating the overall application-flow,
// such as from reading-data to processing it, and then writing
// result.
type processMgr struct {
	log               logger.Logger
	bus               eventutil.Bus
	txnResultViewRepo accountview.TxnResultViewRepo

	writeData  model.CmdAction
	createTxn  model.CmdAction
	processTxn model.CmdAction

	txnRead         model.EventAction
	txnCreated      model.EventAction
	txnCreateFailed model.EventAction
	reportWritten   model.EventAction

	reportWrittenEventTimeoutSec int

	eventSubs map[model.EventAction]<-chan interface{}
}

// ProcessMgrCfg is config for processMgr.
type ProcessMgrCfg struct {
	Log               logger.Logger                 `validate:"nonnil"`
	Bus               eventutil.Bus                 `validate:"nonnil"`
	TxnResultViewRepo accountview.TxnResultViewRepo `validate:"nonnil"`

	WriteData  model.CmdAction `validate:"nonzero"`
	CreateTxn  model.CmdAction `validate:"nonzero"`
	ProcessTxn model.CmdAction `validate:"nonzero"`

	TxnRead         model.EventAction `validate:"nonzero"`
	TxnCreated      model.EventAction `validate:"nonzero"`
	TxnCreateFailed model.EventAction `validate:"nonzero"`
	ReportWritten   model.EventAction `validate:"nonzero"`

	ReportWrittenEventTimeoutSec int `validate:"min=0"`
}

// InitProcessMgr validates process-manager
// config and runs process-manager.
func InitProcessMgr(ctx context.Context, cfg *ProcessMgrCfg) error {
	err := validator.Validate(cfg)
	if err != nil {
		return errors.Wrap(err, "error validating config")
	}

	// Subscribe to actions from Bus
	actions := []model.EventAction{
		cfg.TxnRead,
		cfg.TxnCreated,
		cfg.TxnCreateFailed,
		cfg.ReportWritten,
	}
	eventSubs := make(map[model.EventAction]<-chan interface{})
	for _, action := range actions {
		eventSubs[action], err = cfg.Bus.Subscribe(action.String())
		if err != nil {
			return errors.Wrapf(err, "error subscribing to event-bus for action: %s", action)
		}
	}

	// Create and run manager
	cfg.Log.Infof("Starting process-manager")
	runner := &processMgr{
		log:               cfg.Log,
		bus:               cfg.Bus,
		txnResultViewRepo: cfg.TxnResultViewRepo,

		writeData:  cfg.WriteData,
		createTxn:  cfg.CreateTxn,
		processTxn: cfg.ProcessTxn,

		txnRead:         cfg.TxnRead,
		txnCreated:      cfg.TxnCreated,
		txnCreateFailed: cfg.TxnCreateFailed,
		reportWritten:   cfg.ReportWritten,

		reportWrittenEventTimeoutSec: cfg.ReportWrittenEventTimeoutSec,

		eventSubs: eventSubs,
	}
	err = runner.start(ctx)
	return errors.Wrap(err, "process-loop returned with error")
}

func (p *processMgr) start(ctx context.Context) error {
	defer p.unsubscribe()

	// Helps collect error from routines
	errChan := make(chan error, 0)
	// Some tasks need to be completed after
	// context-done signal is received.
	// To prevent select-case from executing
	// context-done multiple times and running
	// duplicate tasks, this control-var is used.
	ctxDoneAck := false

	for {
		// Some operations here run in their own routines to
		// prevent deadlock in process-manager (such as when
		// its waiting for a case to complete, but that case
		// in return is waiting on another case to forward an
		// event/command, and so all such cases need to be
		// able to process concurrently).
		select {
		case <-ctx.Done():
			if ctxDoneAck {
				continue
			}
			ctxDoneAck = true
			p.log.Debug("Received context-done signal")
			err := p.writeReportAndStopLoop(errChan)
			if err != nil {
				return errors.Wrap(err, "error processing context-done signal")
			}

		case msg := <-p.eventSubs[p.txnRead]:
			p.pubCreateTxnCmd(errChan, msg)

		case msg := <-p.eventSubs[p.txnCreated]:
			p.pubProcessTxnCmd(errChan, msg)

		case msg := <-p.eventSubs[p.txnCreateFailed]:
			p.logCreateTxnFailure(msg)

		case err := <-errChan:
			return errors.Wrap(err, "received error on error-channel")
		}
	}
}

func (p *processMgr) writeReportAndStopLoop(errChan chan<- error) error {
	// Get data from transaction-result view-repo and
	// send command to writer-service to write it
	txnResults := p.txnResultViewRepo.Serialized()
	writeDataCmd, err := model.NewCmd(&model.CmdCfg{
		Action: p.writeData,
		Data:   []byte(txnResults),
	})
	if err != nil {
		return errors.Wrapf(err, "error creating '%s' command", p.writeData)
	}
	err = p.bus.Publish(writeDataCmd)
	if err != nil {
		return errors.Wrapf(err, "error publishing '%s' command on bus", p.writeData)
	}
	p.log.Debugf("Waiting for response from writer-service")

	// Wait for success-event from data-writer, or time-out with error
	go func() {
		err := func() error {
			select {
			case <-time.After(time.Duration(p.reportWrittenEventTimeoutSec) * time.Second):
				return errors.New("timed-out waiting for response from write-service")
			case msg := <-p.eventSubs[p.reportWritten]:
				event, castSuccess := msg.(model.Event)
				if !castSuccess {
					return fmt.Errorf("error casting message to '%s' Event", p.reportWritten)
				}
				p.log.Tracef("[Event: %s]: [Action: %s]: Received event", event.ID(), event.Action())
			}
			return nil
		}()

		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	return nil
}

func (p *processMgr) pubCreateTxnCmd(errChan chan<- error, msg interface{}) {
	if msg == nil {
		return
	}
	event, castSuccess := msg.(model.Event)
	if !castSuccess {
		p.log.Warnf("error casting message to '%s' Event", p.txnRead)
		return
	}
	// Send create-transaction command
	logPrefix := fmt.Sprintf("[Event: %s]: [Action: %s]:", event.ID(), event.Action())
	p.log.Tracef("%s Received event", logPrefix)

	go func() {
		err := func() error {
			cmd, err := model.NewCmd(&model.CmdCfg{
				CorrelationKey: event.ID(),
				Action:         p.createTxn,
				Data:           event.Data(),
			})
			if err != nil {
				return errors.Wrapf(err, "error creating '%s' command", p.createTxn)
			}
			err = p.bus.Publish(cmd)
			if err != nil {
				return errors.Wrapf(err, "error publishing '%s' command on bus", p.createTxn)
			}
			return nil
		}()

		if err != nil {
			errChan <- err
		}
	}()
}

func (p *processMgr) pubProcessTxnCmd(errChan chan<- error, msg interface{}) {
	if msg == nil {
		return
	}
	event, castSuccess := msg.(model.Event)
	if !castSuccess {
		p.log.Warnf("error casting message to '%s' Event", p.txnCreated)
		return
	}
	// Send process-transaction command
	logPrefix := fmt.Sprintf("[Event: %s]: [Action: %s]:", event.ID(), event.Action())
	p.log.Tracef("%s Received event", logPrefix)

	go func() {
		err := func() error {
			cmd, err := model.NewCmd(&model.CmdCfg{
				CorrelationKey: event.ID(),
				Action:         p.processTxn,
				Data:           event.Data(),
			})
			if err != nil {
				return errors.Wrapf(err, "error creating '%s' command", p.processTxn)
			}
			err = p.bus.Publish(cmd)
			return errors.Wrapf(err, "error publishing '%s' command on bus", p.processTxn)
		}()

		if err != nil {
			errChan <- err
		}
	}()
}

func (p *processMgr) logCreateTxnFailure(msg interface{}) {
	if msg == nil {
		return
	}
	event, castSuccess := msg.(model.Event)
	if !castSuccess {
		p.log.Warnf("error casting message to '%s' Event", p.txnCreated)
		return
	}
	logPrefix := fmt.Sprintf("[Event: %s]: [Action: %s]:", event.ID(), event.Action())
	p.log.Tracef("%s Received event", logPrefix)

	failureData := &txn.CreateTxnFailure{}
	err := json.Unmarshal(event.Data(), failureData)
	if err != nil {
		p.log.Warnf("error unmarshalling event-data for '%s' Event", p.txnCreateFailed)
		return
	}
	p.log.Infof("Failed creating transaction: %+v", failureData)
}

func (p *processMgr) unsubscribe() error {
	for action, channel := range p.eventSubs {
		// Already unsubscribed
		if channel == nil {
			continue
		}

		p.log.Debugf("Unsubscribing from action: %s", action)
		err := p.bus.Unsubscribe(channel, action.String())
		if err != nil {
			return errors.Wrapf(err, "error unsubscribing from event-bus for action: %s", action)
		}
		p.eventSubs[action] = nil
		p.log.Tracef("Unsubscribed from action: %s", action)
	}
	return nil
}
