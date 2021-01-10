package domain

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/domain/account"
	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/reader"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/domain/writer"
	"github.com/Jaskaranbir/es-bank-account/logger"
)

type routinesRunner struct{}

// RoutinesCfg is config for all routines.
type RoutinesCfg struct {
	Log logger.Logger

	ReaderCfg     *reader.Cfg         `validate:"nonnil"`
	TxnCreatorCfg *txn.CmdListenerCfg `validate:"nonnil"`

	AccountCfg     *account.CmdListenerCfg       `validate:"nonnil"`
	AccountViewCfg *accountview.EventListenerCfg `validate:"nonnil"`

	ProcessMgrCfg *ProcessMgrCfg         `validate:"nonnil"`
	WriterCfg     *writer.CmdListenerCfg `validate:"nonnil"`

	// Since we have no deterministic way to know if
	// all routines finished processing their tasks
	// (check README for more details on why), there
	// needs to be this arbitrary wait.
	// The duration was chosen as a guess and has
	// proven sufficient by a number of tests.
	PostReadWaitIntervalSec int `validate:"min=0"`
}

// RunRoutines runs domain-routines with provided config.
func RunRoutines(cfg *RoutinesCfg) error {
	err := validator.Validate(cfg)
	if err != nil {
		return errors.Wrap(err, "error validating config")
	}

	runner := routinesRunner{}

	// Context to monitor all routines collectively
	mainCtx, mainCancel := context.WithCancel(context.Background())

	// Process-Manager
	processMgrRun, processMgrCancel := runner.runProcessMgr(cfg.Log, mainCancel, cfg.ProcessMgrCfg)
	// TxnCreator
	txnCreatorRun, txnCreatorCancel := runner.runTxnCreator(cfg.Log, mainCancel, cfg.TxnCreatorCfg)
	// Account
	accountRun, accountCancel := runner.runAccount(cfg.Log, mainCancel, cfg.AccountCfg)
	// TxnResultView
	accountViewRun, accountViewCancel := runner.runAccountView(cfg.Log, mainCancel, cfg.AccountViewCfg)
	// Writer
	writerRun, writerCancel := runner.runWriter(cfg.Log, mainCancel, cfg.WriterCfg)
	// Reader
	readerRun, readerCancel, err := runner.runReader(cfg.Log, mainCancel, cfg.ReaderCfg)
	if err != nil {
		mainCancel()
		return errors.Wrap(err, "error running reader")
	}

	// ================== Manage routines ==================
	<-mainCtx.Done()
	time.Sleep(time.Duration(cfg.PostReadWaitIntervalSec) * time.Second)
	// To collect errors from all routines as they close
	routineErrors := make(map[string]error)

	readerCancel()
	cfg.Log.Tracef("Waiting for Reader to return")
	err = readerRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "reader returned with error")
		routineErrors["reader"] = err
	}

	txnCreatorCancel()
	cfg.Log.Tracef("Waiting for TxnCreator to return")
	err = txnCreatorRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "transaction-creator returned with error")
		routineErrors["txnCreator"] = err
	}

	accountCancel()
	cfg.Log.Tracef("Waiting for Account to return")
	err = accountRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "account returned with error")
		routineErrors["account"] = err
	}

	processMgrCancel()
	cfg.Log.Tracef("Waiting for ProcessMgr to return")
	err = processMgrRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "process-manager returned with error")
		routineErrors["processMgr"] = err
	}

	accountViewCancel()
	cfg.Log.Tracef("Waiting for TxnResultView to return")
	err = accountViewRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "transaction-result-view returned with error")
		routineErrors["txnResultView"] = err
	}

	writerCancel()
	cfg.Log.Tracef("Waiting for Writer to return")
	err = writerRun.Wait()
	if err != nil {
		err = errors.Wrap(err, "writer returned with error")
		routineErrors["writer"] = err
	}

	// Collect and print all errors
	errStr := ""
	for routine, routineErr := range routineErrors {
		errStr += fmt.Sprintf("[%s]: %s\n", routine, routineErr)
	}
	if errStr != "" {
		errStr = "Some routines returned with errors:\n" + errStr
		// Remove last newline char
		errStr = errStr[:len(errStr)-1]
		return errors.New(errStr)
	}
	return nil
}

func (r *routinesRunner) runAccount(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *account.CmdListenerCfg,
) (*errgroup.Group, context.CancelFunc) {
	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := account.InitCmdListener(ctx, cfg)
		if err != nil {
			err = errors.Wrap(err, "error in account routine")
		}
		stdLog.Infof("Account routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel
}

func (r *routinesRunner) runAccountView(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *accountview.EventListenerCfg,
) (*errgroup.Group, context.CancelFunc) {
	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := accountview.InitEventListener(ctx, cfg)
		if err != nil {
			err = errors.Wrap(err, "error in account-view routine")
		}
		stdLog.Infof("Account-view routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel
}

func (r *routinesRunner) runTxnCreator(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *txn.CmdListenerCfg,
) (*errgroup.Group, context.CancelFunc) {
	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := txn.InitCmdListener(ctx, cfg)
		if err != nil {
			err = errors.Wrap(err, "error in transaction-creator routine")
		}
		stdLog.Infof("Transaction-creator routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel
}

func (r *routinesRunner) runProcessMgr(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *ProcessMgrCfg,
) (*errgroup.Group, context.CancelFunc) {
	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := InitProcessMgr(ctx, cfg)
		if err != nil {
			err = errors.Wrap(err, "error in process-manager")
		}
		stdLog.Infof("Process-manager routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel
}

func (r *routinesRunner) runReader(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *reader.Cfg,
) (*errgroup.Group, context.CancelFunc, error) {
	reader, err := reader.NewReader(cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating reader")
	}

	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := reader.Start(ctx)
		if err != nil {
			err = errors.Wrap(err, "error in reader routine")
		}
		stdLog.Infof("Reader-routine routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel, nil
}

func (r *routinesRunner) runWriter(
	stdLog logger.Logger,
	mainCancel context.CancelFunc,
	cfg *writer.CmdListenerCfg,
) (*errgroup.Group, context.CancelFunc) {
	startupWg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	run, _ := errgroup.WithContext(ctx)

	startupWg.Add(1)
	run.Go(func() error {
		startupWg.Done()
		err := writer.InitCmdListener(ctx, cfg)
		if err != nil {
			err = errors.Wrap(err, "error in writer routine")
		}
		stdLog.Infof("Writer-routine routine returned")
		cancel()
		mainCancel()
		return err
	})
	startupWg.Wait()

	return run, cancel
}
