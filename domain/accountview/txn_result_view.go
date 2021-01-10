package accountview

import (
	"encoding/json"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/domain/account"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// txnResultView handles maintaining a projection of transaction-results.
// Use #newTxnResultView to create new instance.
type txnResultView struct {
	log            logger.Logger
	resultRepo     TxnResultViewRepo
	eventRepo      eventutil.EventRepo
	lastEventIndex int

	accountDeposited     model.EventAction
	accountWithdrawn     model.EventAction
	duplicateTxn         model.EventAction
	accountLimitExceeded model.EventAction
}

// TxnResultViewCfg defines config for txnResultView.
type TxnResultViewCfg struct {
	Log        logger.Logger       `validate:"nonnil"`
	ResultRepo TxnResultViewRepo   `validate:"nonnil"`
	EventRepo  eventutil.EventRepo `validate:"nonnil"`

	AccountDeposited     model.EventAction `validate:"nonzero"`
	AccountWithdrawn     model.EventAction `validate:"nonzero"`
	DuplicateTxn         model.EventAction `validate:"nonzero"`
	AccountLimitExceeded model.EventAction `validate:"nonzero"`
}

func newTxnResultView(cfg *TxnResultViewCfg) (*txnResultView, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error validating config")
	}

	return &txnResultView{
		log:            cfg.Log,
		resultRepo:     cfg.ResultRepo,
		eventRepo:      cfg.EventRepo,
		lastEventIndex: 0,

		accountDeposited:     cfg.AccountDeposited,
		accountWithdrawn:     cfg.AccountWithdrawn,
		duplicateTxn:         cfg.DuplicateTxn,
		accountLimitExceeded: cfg.AccountLimitExceeded,
	}, nil
}

// hydrate updates transaction-result view-repo
// with new events from eventstore.
func (rv *txnResultView) hydrate() error {
	// Fetch new events
	rv.log.Tracef("Fetching events from event-repo")
	events, err := rv.eventRepo.FetchByIndex(rv.resultRepo.Index())
	if err != nil {
		return errors.Wrap(err, "error getting events from event-repo")
	}
	rv.log.Tracef("Fetched %d event(s) from event-repo", len(events))

	// Add events to view-repo
	for _, event := range events {
		rv.log.Tracef("[EventID: %s]: Processing event", event.ID())

		switch event.Action() {
		case rv.accountDeposited, rv.accountWithdrawn:
			txnState := &account.State{}
			err := json.Unmarshal(event.Data(), txnState)
			if err != nil {
				return errors.Wrap(err, "error unmarshalling event-data")
			}
			err = rv.resultRepo.Insert(TxnResultEntry{
				ID:         txnState.TxnID,
				CustomerID: txnState.CustID,
				Accepted:   true,
			})
			if err != nil {
				return errors.Wrap(err, "error inserting event into transaction-view repo")
			}

		case rv.duplicateTxn, rv.accountLimitExceeded:
			txnFailure := &account.TxnFailure{}
			err := json.Unmarshal(event.Data(), txnFailure)
			if err != nil {
				return errors.Wrap(err, "error unmarshalling event-data")
			}
			err = rv.resultRepo.Insert(TxnResultEntry{
				ID:         txnFailure.Txn.ID,
				CustomerID: txnFailure.Txn.CustomerID,
				Accepted:   false,
			})
			if err != nil {
				return errors.Wrap(err, "error inserting event into transaction-view repo")
			}

		default:
			return errors.New("event has invalid action")
		}

		rv.log.Tracef("[EventID: %s]: Processed event", event.ID())
	}

	return nil
}
