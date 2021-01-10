package main

import (
	"bufio"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"

	globalcfg "github.com/Jaskaranbir/es-bank-account/config"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"

	"github.com/Jaskaranbir/es-bank-account/domain"
	"github.com/Jaskaranbir/es-bank-account/domain/account"
	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/reader"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/domain/writer"
)

func main() {
	bus, err := eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
	if err != nil {
		err = errors.Wrap(err, "error creating memory-bus")
		log.Fatalln(err)
	}

	// ================== Account ==================
	accountCfg, err := accountRunCfg(bus)
	if err != nil {
		err = errors.Wrap(err, "error creating account-config")
		log.Fatalln(err)
	}

	// ================== TxnResultView ==================
	accountViewCfg := accountViewRunCfg(bus, accountCfg.AccountCfg.EventRepo)

	// ================== TxnCreator ==================
	txnCreatorCfg, err := txnCreatorRunCfg(bus)
	if err != nil {
		err = errors.Wrap(err, "error creating transaction-creator config")
		log.Fatalln(err)
	}

	// ================== Process-Manager ==================
	processMgrCfg := processMgrRunCfg(bus, accountViewCfg.ResultViewCfg.ResultRepo)

	// ================== Reader ==================
	inputFile, err := os.Open(globalcfg.InputFilePath)
	if err != nil {
		err = errors.Wrap(err, "error opening input-file")
		log.Fatalln(err)
	}
	defer inputFile.Close()
	readerCfg := &reader.Cfg{
		Log:      logger.NewStdLogger("reader"),
		Bus:      bus,
		Reader:   inputFile,
		DataRead: model.TxnRead,
	}

	// ================== Writer ==================
	outputFile, err := os.Create(globalcfg.OutputFilePath)
	if err != nil {
		err = errors.Wrap(err, "error creating output-file")
		log.Fatalln(err)
	}
	defer outputFile.Close()
	writerCfg, err := writerRunCfg(bus, outputFile)
	if err != nil {
		err = errors.Wrap(err, "error creating writer-config")
		log.Fatalln(err)
	}

	// ================== Runner ==================
	err = domain.RunRoutines(&domain.RoutinesCfg{
		Log:            logger.NewStdLogger("runner"),
		ReaderCfg:      readerCfg,
		TxnCreatorCfg:  txnCreatorCfg,
		AccountCfg:     accountCfg,
		AccountViewCfg: accountViewCfg,
		ProcessMgrCfg:  processMgrCfg,
		WriterCfg:      writerCfg,

		PostReadWaitIntervalSec: 10,
	})
	if err != nil {
		err = errors.Wrap(err, "error running domain-routines")
		log.Fatalln(err)
	}

	err = inputFile.Close()
	if err != nil {
		err = errors.Wrap(err, "error closing input-file")
		log.Fatalln(err)
	}
	err = outputFile.Close()
	if err != nil {
		err = errors.Wrap(err, "error closing output-file")
		log.Fatalln(err)
	}
}

func accountRunCfg(bus eventutil.Bus) (*account.CmdListenerCfg, error) {
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

func accountViewRunCfg(
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

func txnCreatorRunCfg(bus eventutil.Bus) (*txn.CmdListenerCfg, error) {
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

func processMgrRunCfg(
	bus eventutil.Bus,
	txnResultViewRepo accountview.TxnResultViewRepo,
) *domain.ProcessMgrCfg {
	return &domain.ProcessMgrCfg{
		Log:               logger.NewStdLogger("ProcessMgr"),
		Bus:               bus,
		TxnResultViewRepo: txnResultViewRepo,

		WriteData:  model.WriteData,
		CreateTxn:  model.CreateTxn,
		ProcessTxn: model.ProcessTxn,

		TxnRead:         model.TxnRead,
		TxnCreated:      model.TxnCreated,
		TxnCreateFailed: model.TxnCreateFailed,
		ReportWritten:   model.DataWritten,

		ReportWrittenEventTimeoutSec: 3,
	}
}

func writerRunCfg(bus eventutil.Bus, w io.Writer) (*writer.CmdListenerCfg, error) {
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
