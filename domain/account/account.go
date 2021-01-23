package account

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// TxnFailureCause represents a probable
// cause for a transaction's failure.
type TxnFailureCause string

// Various possible causes for transaction-failure.
const (
	DuplicateTxn         TxnFailureCause = "DuplicateTxn"
	DailyLimitsExceeded  TxnFailureCause = "DailyLimitsExceeded"
	WeeklyLimitsExceeded TxnFailureCause = "WeeklyLimitsExceeded"
	InsufficientFunds    TxnFailureCause = "InsufficientFunds"
)

// account represents an account for a specific customer.
// Use #newAccount to create new instance.
type account struct {
	log       logger.Logger
	eventRepo eventutil.EventRepo

	accountDeposited     model.EventAction
	accountWithdrawn     model.EventAction
	duplicateTxn         model.EventAction
	accountLimitExceeded model.EventAction

	dailyLimits  TxnRecord
	weeklyLimits TxnRecord

	custID        string
	dailyTxn      map[int]map[int]TxnRecord
	weeklyTxn     map[int]map[int]TxnRecord
	balance       float64
	txnKeysRecord []string
}

// TxnRecord is aggregated transaction-data
// for a specific time-range (example: a day).
type TxnRecord struct {
	NumTxns     int
	TotalAmount float64
}

// State is result of all transactions till
// a specific transaction (denoted by TxnID in
// record) for an account.
type State struct {
	TxnID   string
	CustID  string
	TxnTime time.Time

	DailyTxn    TxnRecord
	WeeklyTxn   TxnRecord
	TotalAmount float64
}

// TxnFailure contains data/info for transaction-failure.
type TxnFailure struct {
	Txn          model.Transaction
	Error        string
	FailureCause TxnFailureCause
}

// AggregateCfg defines config for Account-aggregate.
type AggregateCfg struct {
	Log       logger.Logger       `validate:"nonnil"`
	EventRepo eventutil.EventRepo `validate:"nonnil"`

	AccountDeposited     model.EventAction `validate:"nonzero"`
	AccountWithdrawn     model.EventAction `validate:"nonzero"`
	DuplicateTxn         model.EventAction `validate:"nonzero"`
	AccountLimitExceeded model.EventAction `validate:"nonzero"`

	DailyTxnsAmountLimit  float64 `validate:"min=0"`
	NumDailyTxnsLimit     int     `validate:"min=0"`
	WeeklyTxnsAmountLimit float64 `validate:"min=0"`
	NumWeeklyTxnsLimit    int     `validate:"min=0"`
}

// newAccount validates Account-Config
// and creates new Account-instance.
func newAccount(cfg *AggregateCfg) (*account, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, errors.New("error validating config")
	}
	if cfg.WeeklyTxnsAmountLimit > 0 &&
		cfg.WeeklyTxnsAmountLimit < cfg.DailyTxnsAmountLimit {
		return nil, errors.New("weekly-amount limit must be greater than daily amount limit")
	}
	if cfg.NumWeeklyTxnsLimit > 0 &&
		cfg.NumWeeklyTxnsLimit < cfg.NumDailyTxnsLimit {
		return nil, errors.New(
			"num of weekly-transactions must be greater than num of daily-transactions",
		)
	}

	return &account{
		log:       cfg.Log,
		eventRepo: cfg.EventRepo,

		accountDeposited:     cfg.AccountDeposited,
		accountWithdrawn:     cfg.AccountWithdrawn,
		duplicateTxn:         cfg.DuplicateTxn,
		accountLimitExceeded: cfg.AccountLimitExceeded,

		dailyLimits: TxnRecord{
			NumTxns:     cfg.NumDailyTxnsLimit,
			TotalAmount: cfg.DailyTxnsAmountLimit,
		},
		weeklyLimits: TxnRecord{
			NumTxns:     cfg.NumWeeklyTxnsLimit,
			TotalAmount: cfg.WeeklyTxnsAmountLimit,
		},

		dailyTxn:      make(map[int]map[int]TxnRecord),
		weeklyTxn:     make(map[int]map[int]TxnRecord),
		txnKeysRecord: make([]string, 0),
	}, nil
}

func (a *account) handleProcessTxnCmd(cmd model.Cmd) error {
	if cmd.Data() == nil {
		a.log.Debugf("[CMD: %s] ignored command with nil data", cmd.ID())
		return nil
	}
	logPrefix := fmt.Sprintf("[CMD-Action: %s]: [CMD: %s]:", cmd.Action(), cmd.ID())

	a.log.Tracef("%s Processing transaction", logPrefix)
	txn := &model.Transaction{}
	err := json.Unmarshal(cmd.Data(), txn)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling event-data")
	}
	logPrefix = fmt.Sprintf("%s [Txn: %s]:", logPrefix, txn.ID)

	a.log.Tracef("%s Loading aggregate", logPrefix)
	err = a.loadAggregate(txn.CustomerID)
	if err != nil {
		return errors.Wrap(err, "error loading aggregate")
	}

	a.ensureYear(txn.Time.UTC().Year())

	// Check duplicate-transaction
	isUnique, err := a.checkDuplicateTxn(cmd.ID(), txn)
	if err != nil {
		return errors.Wrap(err, "errors validating transaction-uniqueness")
	}
	if !isUnique {
		return nil
	}

	// Validate daily-limits
	dailyTxnRecord, err := a.checkDailyLimits(cmd.ID(), txn)
	if err != nil {
		return errors.Wrap(err, "errors validating transaction daily-limits")
	}
	if dailyTxnRecord == nil {
		return nil
	}

	// Validate weekly-limits
	weeklyTxnRecord, err := a.checkWeeklyLimits(cmd.ID(), txn)
	if err != nil {
		return errors.Wrap(err, "errors validating transaction daily-limits")
	}
	if weeklyTxnRecord == nil {
		return nil
	}

	accEvent := a.accountDeposited
	if txn.LoadAmount < 0 {
		accEvent = a.accountWithdrawn
	}

	logPrefix = fmt.Sprintf("%s [EventAction: %s]", logPrefix, accEvent)

	state := &State{
		TxnID:       txn.ID,
		CustID:      txn.CustomerID,
		TxnTime:     txn.Time,
		DailyTxn:    *dailyTxnRecord,
		WeeklyTxn:   *weeklyTxnRecord,
		TotalAmount: a.balance + txn.LoadAmount,
	}
	a.log.Tracef("%s Publishing success-event", logPrefix)
	err = a.publishEvent(cmd.ID(), accEvent, state)
	if err != nil {
		return errors.Wrapf(
			err,
			"error publishing event: %s", accEvent,
		)
	}
	a.log.Tracef("%s Published success-event", logPrefix)
	return nil
}

// checkDuplicateTxn checks if transaction is unique for this account. Also publishes TxnDuplicate event on Bus.
// Return params:
// - bool: Indicates if transaction was unique.
// - error: Critical errors encountered while checking
// 					for transaction uniqueness.
func (a *account) checkDuplicateTxn(cmdID string, txn *model.Transaction) (bool, error) {
	logPrefix := fmt.Sprintf("[CMD: %s]: [Txn: %s]:", cmdID, txn.ID)

	a.log.Tracef("%s Validating transaction-uniqueness", logPrefix)
	for _, txnID := range a.txnKeysRecord {
		if txnID == txn.ID {
			failure := &TxnFailure{
				Txn:          *txn,
				Error:        errors.New("duplicate transaction").Error(),
				FailureCause: DuplicateTxn,
			}
			subLogPrefix := fmt.Sprintf("%s [EventAction: %s]", logPrefix, a.duplicateTxn)

			a.log.Tracef("%s Publishing failure-event", subLogPrefix)
			err := a.publishEvent(cmdID, a.duplicateTxn, failure)
			if err != nil {
				return false, errors.Wrapf(
					err,
					"error publishing event: %s", a.duplicateTxn,
				)
			}
			a.log.Tracef("%s Published failure-event", subLogPrefix)
			return false, nil
		}
	}
	return true, nil
}

// checkDailyLimits checks if transaction passes daily-limits
// for this account. Also publishes AccountLimitExceeded event
// on Bus.
// Return params:
// - TxnRecord: New Daily-Transaction record for this account.
// - error: Critical errors encountered while validating
// 					for daily-limits.
func (a *account) checkDailyLimits(
	cmdID string,
	txn *model.Transaction,
) (*TxnRecord, error) {
	logPrefix := fmt.Sprintf("[CMD: %s]: [Txn: %s]:", cmdID, txn.ID)

	txnUTCTime := txn.Time.UTC()
	txnDay := txnUTCTime.YearDay()
	txnYear := txnUTCTime.Year()

	a.log.Tracef("%s Validating daily limits", logPrefix)
	dailyTxnRecord := a.dailyTxn[txnYear][txnDay]
	dailyTxnRecord.NumTxns++
	dailyTxnRecord.TotalAmount += txn.LoadAmount

	failureCause, err := a.validateLimits(dailyTxnRecord, a.dailyLimits)
	if err != nil {
		if failureCause == "" {
			failureCause = DailyLimitsExceeded
		}
		failure := &TxnFailure{
			Txn:          *txn,
			Error:        errors.Wrap(err, "failed daily-limits validation").Error(),
			FailureCause: failureCause,
		}
		subLogPrefix := fmt.Sprintf("%s [EventAction: %s]", logPrefix, a.accountLimitExceeded)

		a.log.Tracef("%s Publishing failure-event", subLogPrefix)
		err = a.publishEvent(cmdID, a.accountLimitExceeded, failure)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"error publishing event: %s", a.accountLimitExceeded,
			)
		}
		a.log.Tracef("%s Published failure-event", subLogPrefix)
		return nil, nil
	}
	return &dailyTxnRecord, nil
}

// checkWeeklyLimits checks if transaction passes weekly-limits
// for this account. Also publishes AccountLimitExceeded event
// on Bus.
// Return params:
// - TxnRecord: New Weekly-Transaction record for this account.
// - error: Critical errors encountered while validating
// 					for weekly-limits.
func (a *account) checkWeeklyLimits(
	cmdID string,
	txn *model.Transaction,
) (*TxnRecord, error) {
	logPrefix := fmt.Sprintf("[CMD: %s]: [Txn: %s]:", cmdID, txn.ID)

	txnUTCTime := txn.Time.UTC()
	txnYear, txnWeek := txnUTCTime.ISOWeek()

	a.log.Tracef("%s Validating weekly limits", logPrefix)
	weeklyTxnRecord := a.weeklyTxn[txnYear][txnWeek]
	weeklyTxnRecord.NumTxns++
	weeklyTxnRecord.TotalAmount += txn.LoadAmount

	failureCause, err := a.validateLimits(weeklyTxnRecord, a.weeklyLimits)
	if err != nil {
		if failureCause == "" {
			failureCause = WeeklyLimitsExceeded
		}
		failure := &TxnFailure{
			Txn:          *txn,
			Error:        errors.Wrap(err, "failed weekly-limits validation").Error(),
			FailureCause: failureCause,
		}
		subLogPrefix := fmt.Sprintf("%s [EventAction: %s]", logPrefix, a.accountLimitExceeded)

		a.log.Tracef("%s Publishing failure-event", subLogPrefix)
		err = a.publishEvent(cmdID, a.accountLimitExceeded, failure)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"error publishing event: %s", a.accountLimitExceeded,
			)
		}
		a.log.Tracef("%s Published failure-event", subLogPrefix)
		return nil, nil
	}
	return &weeklyTxnRecord, nil
}

func (a *account) publishEvent(correlationKey string, action model.EventAction, data interface{}) error {
	event, err := model.NewEvent(&model.EventCfg{
		AggregateID:    a.custID,
		CorrelationKey: correlationKey,
		Action:         action,
		Data:           data,
	})
	if err != nil {
		return errors.Wrap(err, "error creating event")
	}
	err = a.eventRepo.InsertAndPublish(event)
	return errors.Wrap(err, "error storing event in event-repo")
}

// TxnFailureCause is specified if there's a special/specific error.
// Otherwise TxnFailureCause is blank on generic errors.
func (a *account) validateLimits(
	currValues TxnRecord,
	limits TxnRecord,
) (TxnFailureCause, error) {
	if a.balance+currValues.TotalAmount < 0 {
		return InsufficientFunds, errors.New("balance less than zero")
	}
	if limits.NumTxns > 0 && currValues.NumTxns > limits.NumTxns {
		return "", errors.New("limit exceeded for number of deposits")
	}
	if limits.TotalAmount > 0 && currValues.TotalAmount > limits.TotalAmount {
		return "", fmt.Errorf(
			"limit exceeded for total load-value by: $%.2f",
			(currValues.TotalAmount - a.dailyLimits.TotalAmount),
		)
	}

	return "", nil
}

func (a *account) loadAggregate(custID string) error {
	a.custID = custID
	events, err := a.eventRepo.Fetch(custID)
	if err != nil {
		return errors.Wrap(err, "error fetching events from event-store")
	}

	for _, event := range events {
		err := a.applyEvent(event)
		if err != nil {
			return errors.Wrap(err, "error applying event")
		}
	}

	return nil
}

func (a *account) applyEvent(event model.Event) error {
	state := &State{}
	err := json.Unmarshal(event.Data(), state)
	if err != nil {
		return errors.Wrap(err, "error unmarshalling event-data")
	}

	txnUTCTime := state.TxnTime.UTC()
	txnYear, txnWeek := txnUTCTime.ISOWeek()
	txnDay := txnUTCTime.YearDay()

	a.ensureYear(txnYear)

	a.dailyTxn[txnYear][txnDay] = state.DailyTxn
	a.weeklyTxn[txnYear][txnWeek] = state.WeeklyTxn
	a.txnKeysRecord = append(a.txnKeysRecord, state.TxnID)

	a.balance = state.TotalAmount

	return nil
}

func (a *account) ensureYear(year int) {
	_, found := a.dailyTxn[year]
	if !found {
		a.dailyTxn[year] = make(map[int]TxnRecord)
	}

	_, found = a.weeklyTxn[year]
	if !found {
		a.weeklyTxn[year] = make(map[int]TxnRecord)
	}
}
