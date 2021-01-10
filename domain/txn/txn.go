package txn

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// creator handles validating commands and
// creates model.Transaction instances from
// those commands.
// Use #newCreator to create new instance.
type creator struct {
	defaultTimeFmt string

	log       logger.Logger
	eventRepo eventutil.EventRepo

	txnCreated      model.EventAction
	txnCreateFailed model.EventAction
}

// CreateTxnReq represents the request from which
// model.Transaction is created after validation.
type CreateTxnReq struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	LoadAmount string `json:"load_amount"`
	Time       string `json:"time"`
	TimeFmt    string `json:"time_format"`
}

// CreateTxnFailure is failure during transaction-creation.
type CreateTxnFailure struct {
	TxnReq *CreateTxnReq `json:"txn_request"`
	Error  string        `json:"error"`
}

// CreatorCfg is config for txnCreator.
type CreatorCfg struct {
	DefaultTimeFmt string `validate:"nonzero"`

	Log       logger.Logger       `validate:"nonnil"`
	EventRepo eventutil.EventRepo `validate:"nonnil"`

	TxnCreated      model.EventAction `validate:"nonzero"`
	TxnCreateFailed model.EventAction `validate:"nonzero"`
}

// newCreator validates txnCreator-config
// and creates new txn-creator instance.
func newCreator(cfg *CreatorCfg) (*creator, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, err
	}

	return &creator{
		defaultTimeFmt: cfg.DefaultTimeFmt,

		log:             cfg.Log,
		eventRepo:       cfg.EventRepo,
		txnCreated:      cfg.TxnCreated,
		txnCreateFailed: cfg.TxnCreateFailed,
	}, nil
}

func (tc *creator) handleCreateTxnCmd(cmd model.Cmd) error {
	if cmd.Data() == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[CMD-Action: %s]: [CMD: %s]:", cmd.Action(), cmd.ID())
	tc.log.Tracef("%s Creating transaction", logPrefix)

	req := &CreateTxnReq{}
	err := json.Unmarshal(cmd.Data(), req)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling transaction-req from event-data: %s", string(cmd.Data()))
	}

	// Create transaction from transaction-request
	txn, err := tc.createTxn(req)
	if err != nil {
		err = errors.Wrap(err, "error creating transaction from transaction-request")
		txnFail := &CreateTxnFailure{
			TxnReq: req,
			Error:  err.Error(),
		}

		// Publish failure-event
		tc.log.Tracef("%s Publishing fail-result event", logPrefix)
		event, err := model.NewEvent(&model.EventCfg{
			// txn is nil, so cant get aggregate-id
			AggregateID:    "-",
			CorrelationKey: cmd.ID(),
			Action:         tc.txnCreateFailed,
			Data:           txnFail,
		})
		if err != nil {
			return errors.Wrap(err, "error creating event")
		}
		pubErr := tc.eventRepo.InsertAndPublish(event)
		if pubErr == nil {
			tc.log.Tracef("%s Published fail-result event", logPrefix)
		}
		return errors.Wrap(pubErr, "error publishing fail-event")
	}

	// Publish transaction-created event
	tc.log.Tracef("%s Publishing result event", logPrefix)
	event, err := model.NewEvent(&model.EventCfg{
		AggregateID:    txn.ID,
		CorrelationKey: cmd.ID(),
		Action:         tc.txnCreated,
		Data:           txn,
	})
	if err != nil {
		return errors.Wrap(err, "error creating event")
	}
	err = tc.eventRepo.InsertAndPublish(event)
	if err != nil {
		return errors.Wrap(err, "error publishing transaction on event-bus")
	}
	tc.log.Tracef("%s Published result event", logPrefix)
	return nil
}

// createTxn returns a transaction-instance
// using properties from given CreateTxnReq.
// Returns error if TransactionRequest contains
// invalid values.
func (tc *creator) createTxn(txnReq *CreateTxnReq) (*model.Transaction, error) {
	if txnReq == nil {
		return nil, errors.New("TransactionRequest cannot be nil")
	}

	// Assuming ID "0" is valid
	if txnReq.ID == "" {
		return nil, errors.New("TransactionID cannot be empty")
	}

	// Assuming CustomerID "0" is valid
	if txnReq.CustomerID == "" {
		return nil, errors.New("CustomerID cannot be empty")
	}

	// ============== Validate LoadAmount ==============
	// "[1:]" removes the "$" prefix from string
	if txnReq.LoadAmount == "" || txnReq.LoadAmount[1:] == "" {
		return nil, errors.New("LoadAmount cannot be empty")
	}
	loadAmountStr := strings.ReplaceAll(txnReq.LoadAmount, "$", "")
	loadAmount, err := strconv.ParseFloat(loadAmountStr, 64)
	if err != nil {
		return nil, errors.New("invalid value for LoadAmount")
	}

	// ============== Validate Time ==============
	if txnReq.TimeFmt == "" {
		txnReq.TimeFmt = tc.defaultTimeFmt
	}
	parsedTime, err := time.Parse(txnReq.TimeFmt, txnReq.Time)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing time")
	}

	return &model.Transaction{
		ID:         txnReq.ID,
		CustomerID: txnReq.CustomerID,
		LoadAmount: loadAmount,
		Time:       parsedTime,
	}, nil
}
