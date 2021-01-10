package domain_test

import (
	"bufio"
	"io"

	globalcfg "github.com/Jaskaranbir/es-bank-account/config"
	"github.com/Jaskaranbir/es-bank-account/domain/account"
	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/domain/writer"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
	"github.com/pkg/errors"
)

// ConfigProvider provides configs for various
// domain-entities (such as listeners, aggregates,
// producers, util, etc.) for use in tests.
type ConfigProvider struct{}

// AccountRunCfg provides config for testing
// Account (command-listener and aggregate).
func (cp *ConfigProvider) AccountRunCfg(
	bus eventutil.Bus,
) (*account.CmdListenerCfg, error) {
	accountEventRepo, err := eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
		Bus:            bus,
		EventStore:     eventutil.NewMemoryEventStore(),
		UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating event-repo for account")
	}

	return &account.CmdListenerCfg{
		Log: logger.NewStdLogger("account/CmdListener"),

		Bus:           bus,
		ProcessTxnCmd: model.ProcessTxn,

		AccountCfg: &account.AggregateCfg{
			Log:       logger.NewStdLogger("account/Aggregate"),
			EventRepo: accountEventRepo,

			DailyTxnsAmountLimit:  globalcfg.DailyTxnsAmountLimit,
			NumDailyTxnsLimit:     globalcfg.NumDailyTxnsLimit,
			WeeklyTxnsAmountLimit: globalcfg.WeeklyTxnsAmountLimit,
			NumWeeklyTxnsLimit:    globalcfg.NumWeeklyTxnsLimit,

			AccountDeposited:     model.AccountDeposited,
			AccountWithdrawn:     model.AccountWithdrawn,
			DuplicateTxn:         model.DuplicateTxn,
			AccountLimitExceeded: model.AccountLimitExceeded,
		},
	}, nil
}

// AccountViewRunCfg provides config for testing
// Account-View (event-listener and projection).
func (cp *ConfigProvider) AccountViewRunCfg(
	bus eventutil.Bus,
	accountEventRepo eventutil.EventRepo,
) *accountview.EventListenerCfg {
	txnResultViewRepo := accountview.NewMemoryTxnResultViewRepo()

	return &accountview.EventListenerCfg{
		Log: logger.NewStdLogger("accountView/EventListener"),

		Bus:                  bus,
		AccountDeposited:     model.AccountDeposited,
		AccountWithdrawn:     model.AccountWithdrawn,
		DuplicateTxn:         model.DuplicateTxn,
		AccountLimitExceeded: model.AccountLimitExceeded,

		ResultViewCfg: &accountview.TxnResultViewCfg{
			Log:        logger.NewStdLogger("accountView/TxnResultView"),
			ResultRepo: txnResultViewRepo,
			// To fetch events from Account-aggregate
			EventRepo: accountEventRepo,

			AccountDeposited:     model.AccountDeposited,
			AccountWithdrawn:     model.AccountWithdrawn,
			DuplicateTxn:         model.DuplicateTxn,
			AccountLimitExceeded: model.AccountLimitExceeded,
		},
	}
}

// TxnCreatorRunCfg provides config for testing
// Creator (command-listener and aggregate).
func (cp *ConfigProvider) TxnCreatorRunCfg(bus eventutil.Bus) (*txn.CmdListenerCfg, error) {
	txnCreatorEventRepo, err := eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
		Bus:            bus,
		EventStore:     eventutil.NewMemoryEventStore(),
		UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating event-repo for transaction-creator")
	}

	return &txn.CmdListenerCfg{
		Log: logger.NewStdLogger("txn/CmdListener"),

		Bus:          bus,
		CreateTxnCmd: model.CreateTxn,

		CreatorCfg: &txn.CreatorCfg{
			Log:            logger.NewStdLogger("txn/Aggregate"),
			EventRepo:      txnCreatorEventRepo,
			DefaultTimeFmt: globalcfg.TxnRequestTimeFmt,

			TxnCreated:      model.TxnCreated,
			TxnCreateFailed: model.TxnCreateFailed,
		},
	}, nil
}

// WriterRunCfg provides config for testing
// Creator (command-listener and aggregate).
func (cp *ConfigProvider) WriterRunCfg(
	bus eventutil.Bus,
	w io.Writer,
) (*writer.CmdListenerCfg, error) {
	writerEventRepo, err := eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
		Bus:            bus,
		EventStore:     eventutil.NewMemoryEventStore(),
		UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating event-repo for writer")
	}

	return &writer.CmdListenerCfg{
		Log: logger.NewStdLogger("writer/CmdListener"),

		Bus:       bus,
		WriteData: model.WriteData,

		WriterCfg: &writer.AggregateCfg{
			Log:    logger.NewStdLogger("writer/Aggregate"),
			Writer: bufio.NewWriter(w),

			EventRepo:   writerEventRepo,
			DataWritten: model.DataWritten,
		},
	}, nil
}
