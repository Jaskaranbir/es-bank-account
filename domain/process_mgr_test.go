package domain

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

var _ = Describe("ProcessMgr", func() {
	const busMsgReceiveTimeoutSec = 3
	const (
		WriteData  model.CmdAction = "writeDataCmd"
		CreateTxn  model.CmdAction = "createTxnCmd"
		ProcessTxn model.CmdAction = "processTxnCmd"
	)
	const (
		TxnRead         model.EventAction = "txnRead"
		TxnCreated      model.EventAction = "txnCreated"
		TxnCreateFailed model.EventAction = "txnCreateFailed"
		ReportWritten   model.EventAction = "reportWritten"
	)

	var bus eventutil.Bus
	var txnResultViewRepo accountview.TxnResultViewRepo

	var processMgrCancel context.CancelFunc
	var processMgrErrGroup *errgroup.Group

	BeforeSuite(func() {
		SetDefaultEventuallyTimeout(busMsgReceiveTimeoutSec * time.Second)
	})

	BeforeEach(func() {
		var err error
		bus, err = eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())
		txnResultViewRepo = accountview.NewMemoryTxnResultViewRepo()

		// Start Process-Manager
		var ctx context.Context
		ctx, processMgrCancel = context.WithCancel(context.Background())
		processMgrErrGroup, _ = errgroup.WithContext(context.Background())
		processMgrErrGroup.Go(func() error {
			err := InitProcessMgr(ctx, &ProcessMgrCfg{
				Log:               logger.NewStdLogger("ProcessMgr"),
				Bus:               bus,
				TxnResultViewRepo: txnResultViewRepo,

				WriteData:  WriteData,
				CreateTxn:  CreateTxn,
				ProcessTxn: ProcessTxn,

				TxnRead:         TxnRead,
				TxnCreated:      TxnCreated,
				TxnCreateFailed: TxnCreateFailed,
				ReportWritten:   ReportWritten,

				ReportWrittenEventTimeoutSec: 2,
			})
			if err != nil {
				err = errors.Wrap(err, "error in process-manager")
			}
			return err
		})
		// Ensure the goroutine above
		// is ready to process messages
		time.Sleep(10 * time.Millisecond)
	})

	AfterEach(func() {
		// Since we are testing partial flows for
		// process-manager, we need to force-terminate
		// the channels so they dont keep blocking.
		bus.Terminate()
		processMgrCancel()
		// Force-terminating channels will cause
		// errors, so this is a known/intentional
		// error-case which can be ignored here.
		_ = processMgrErrGroup.Wait()
	})

	When("transaction-read event received", func() {
		It("publishes create-transaction command", func() {
			createTxnSub, err := bus.Subscribe(CreateTxn.String())
			Expect(err).ToNot(HaveOccurred())

			readData := &txn.CreateTxnReq{
				ID:         "2356",
				CustomerID: "23599",
				LoadAmount: "456.66",
				Time:       time.Now().String(),
			}

			txnReadEvent, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      TxnRead,
				Data:        readData,
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(txnReadEvent)
			Expect(err).ToNot(HaveOccurred())

			cmd := &model.Cmd{}
			Eventually(createTxnSub).Should(Receive(cmd))

			cmdData := &txn.CreateTxnReq{}
			err = json.Unmarshal(cmd.Data(), cmdData)
			Expect(err).ToNot(HaveOccurred())
			Expect(cmd.Action()).To(Equal(CreateTxn))
			Expect(cmdData).To(Equal(readData))
		})
	})

	When("transaction-created event received", func() {
		It("publishes process-transaction command", func() {
			processTxnSub, err := bus.Subscribe(ProcessTxn.String())
			Expect(err).ToNot(HaveOccurred())

			dummyTxn := &model.Transaction{
				ID:         "2356",
				CustomerID: "23599",
				LoadAmount: 456.66,
				Time:       time.Now(),
			}
			txnCreatedEvent, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      TxnCreated,
				Data:        dummyTxn,
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(txnCreatedEvent)
			Expect(err).ToNot(HaveOccurred())

			cmd := &model.Cmd{}
			Eventually(processTxnSub).Should(Receive(cmd))

			cmdData := &model.Transaction{}
			err = json.Unmarshal(cmd.Data(), cmdData)
			Expect(err).ToNot(HaveOccurred())
			Expect(cmd.Action()).To(Equal(ProcessTxn))

			Expect(cmdData.Time.UnixNano()).To(Equal(dummyTxn.Time.UnixNano()))
			// Since time-zone metadata can be different
			cmdData.Time = dummyTxn.Time
			Expect(cmdData).To(Equal(dummyTxn))
		})
	})

	When("context completed", func() {
		var resultEntries []accountview.TxnResultEntry

		JustAfterEach(func() {
			resultEntries = []accountview.TxnResultEntry{
				{
					ID:         "34235",
					CustomerID: "29752",
					Accepted:   true,
				},
				{
					ID:         "39257",
					CustomerID: "82619",
					Accepted:   false,
				},
				{
					ID:         "9238",
					CustomerID: "29752",
					Accepted:   true,
				},
			}

			for _, resultEntry := range resultEntries {
				err := txnResultViewRepo.Insert(resultEntry)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		It("publishes write-data command", func() {
			writeDataCmdSub, err := bus.Subscribe(WriteData.String())
			Expect(err).ToNot(HaveOccurred())

			processMgrCancel()

			cmd := &model.Cmd{}
			Eventually(writeDataCmdSub).Should(Receive(cmd))

			cmdData := string(cmd.Data())
			Expect(cmdData).To(Equal(txnResultViewRepo.Serialized()))
		})

		It("exits successfully on data-written event", func(done Done) {
			writeDataCmdSub, err := bus.Subscribe(WriteData.String())
			Expect(err).ToNot(HaveOccurred())

			processMgrCancel()
			Eventually(writeDataCmdSub).Should(Receive())

			dataWrittenEvent, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      ReportWritten,
				Data:        []byte(txnResultViewRepo.Serialized()),
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(dataWrittenEvent)
			Expect(err).ToNot(HaveOccurred())

			err = processMgrErrGroup.Wait()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, busMsgReceiveTimeoutSec)

		It("errors with time-out when data-written event is not received", func(done Done) {
			writeDataCmdSub, err := bus.Subscribe(WriteData.String())
			Expect(err).ToNot(HaveOccurred())

			processMgrCancel()
			Eventually(writeDataCmdSub).Should(Receive())

			err = processMgrErrGroup.Wait()
			Expect(err).To(HaveOccurred())
			close(done)
		}, busMsgReceiveTimeoutSec)
	})
})
