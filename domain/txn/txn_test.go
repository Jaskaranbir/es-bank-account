package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

func TestCreator(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")

	RegisterFailHandler(Fail)
	RunSpecs(t, "Creator Suite")
}

var _ = Describe("Creator", func() {
	const busMsgReceiveTimeoutSec = 3
	const txnReqTimeFmt = "2006-01-02T15:04:05Z"

	const (
		CreateTxn model.CmdAction = "createTxn"
	)
	const (
		TxnCreated      model.EventAction = "txnCreated"
		TxnCreateFailed model.EventAction = "txnCreateFailed"
	)

	// Waits for an event, checks if its expected,
	// and returns event-data on success-event.
	var expectEvent = func(
		successChan <-chan interface{},
		failChan <-chan interface{},
		action model.EventAction,
	) (*model.Transaction, error) {
		// Assigned on receiving success-event
		var createdTxn *model.Transaction

		grp, _ := errgroup.WithContext(context.Background())
		grp.Go(func() error {
			select {
			case <-time.After(busMsgReceiveTimeoutSec * time.Second):
				return errors.New("timed-out waiting for transaction-created event")

			case msg := <-successChan:
				if action != TxnCreated {
					return errors.New("expected fail-event but received success-event instead")
				}
				event, castSuccess := msg.(model.Event)
				if !castSuccess {
					return fmt.Errorf("error casting message to '%s' event", TxnCreated)
				}
				createdTxn = &model.Transaction{}
				err := json.Unmarshal(event.Data(), createdTxn)
				if err != nil {
					return errors.Wrapf(err, "error unmarshalling '%s' event-data", TxnCreated)
				}

			case msg := <-failChan:
				event, castSuccess := msg.(model.Event)
				if !castSuccess {
					return fmt.Errorf("error casting message to '%s' event", TxnCreateFailed)
				}
				txnFailure := &CreateTxnFailure{}
				err := json.Unmarshal(event.Data(), txnFailure)
				if err != nil {
					return errors.Wrapf(err, "error unmarshalling '%s' event-data", TxnCreateFailed)
				}
				if action != TxnCreateFailed {
					return fmt.Errorf("Received fail-event: %+v", txnFailure)
				}
			}

			return nil
		})

		err := grp.Wait()
		return createdTxn, err
	}

	var txnCreator *creator
	var bus eventutil.Bus
	var successSub <-chan interface{}
	var failSub <-chan interface{}

	BeforeEach(func() {
		var err error
		bus, err = eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())

		successSub, err = bus.Subscribe(TxnCreated.String())
		Expect(err).ToNot(HaveOccurred())
		failSub, err = bus.Subscribe(TxnCreateFailed.String())
		Expect(err).ToNot(HaveOccurred())

		txnCreatorEventRepo, err := eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
			Bus:            bus,
			EventStore:     eventutil.NewMemoryEventStore(),
			UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
		})

		txnCreator, err = newCreator(&CreatorCfg{
			DefaultTimeFmt: txnReqTimeFmt,
			Log:            logger.NewStdLogger("TxnCreator"),
			EventRepo:      txnCreatorEventRepo,

			TxnCreated:      TxnCreated,
			TxnCreateFailed: TxnCreateFailed,
		})
	})

	AfterEach(func() {
		bus.Terminate()
	})

	When("create-transaction command is received", func() {
		Describe("transaction-creation if command-data is valid", func() {
			Specify("account-deposit", func() {
				req := &CreateTxnReq{
					ID:         "43583",
					CustomerID: "37648",
					LoadAmount: "$4528.20",
					Time:       time.Now().Format(txnReqTimeFmt),
				}
				cmd, err := model.NewCmd(&model.CmdCfg{
					Action: CreateTxn,
					Data:   req,
				})
				Expect(err).ToNot(HaveOccurred())

				err = txnCreator.handleCreateTxnCmd(cmd)
				Expect(err).ToNot(HaveOccurred())

				createdTxn, err := expectEvent(successSub, failSub, TxnCreated)
				Expect(err).ToNot(HaveOccurred())
				Expect(createdTxn.ID).To(Equal(req.ID))
				Expect(createdTxn.CustomerID).To(Equal(req.CustomerID))
			})

			Specify("account-withdrawal", func() {
				req := &CreateTxnReq{
					ID:         "43583",
					CustomerID: "37648",
					LoadAmount: "-$4528.20",
					Time:       time.Now().Format(txnReqTimeFmt),
				}
				cmd, err := model.NewCmd(&model.CmdCfg{
					Action: CreateTxn,
					Data:   req,
				})
				Expect(err).ToNot(HaveOccurred())

				err = txnCreator.handleCreateTxnCmd(cmd)
				Expect(err).ToNot(HaveOccurred())

				createdTxn, err := expectEvent(successSub, failSub, TxnCreated)
				Expect(err).ToNot(HaveOccurred())
				Expect(createdTxn.ID).To(Equal(req.ID))
				Expect(createdTxn.CustomerID).To(Equal(req.CustomerID))
			})
		})

		It("errors on empty transaction-request", func() {
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   &CreateTxnReq{},
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors on empty ID in transaction-request", func() {
			req := &CreateTxnReq{
				CustomerID: "37648",
				LoadAmount: "$4528.20",
				Time:       time.Now().Format(txnReqTimeFmt),
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors on empty customer-ID in transaction-request", func() {
			req := &CreateTxnReq{
				ID:         "37648",
				LoadAmount: "$4528.20",
				Time:       time.Now().Format(txnReqTimeFmt),
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors on empty load-amount in transaction-request", func() {
			req := &CreateTxnReq{
				ID:         "43583",
				CustomerID: "37648",
				LoadAmount: "",
				Time:       time.Now().Format(txnReqTimeFmt),
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors on invalid load-amount in transaction-request", func() {
			req := &CreateTxnReq{
				ID:         "43583",
				CustomerID: "37648",
				LoadAmount: "$asd",
				Time:       time.Now().Format(txnReqTimeFmt),
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors on invalid time-format in transaction-request", func() {
			req := &CreateTxnReq{
				ID:         "43583",
				CustomerID: "37648",
				LoadAmount: "$835.78",
				Time:       "2000-01-01T04:05",
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())
			_, err = expectEvent(successSub, failSub, TxnCreateFailed)
			Expect(err).ToNot(HaveOccurred())
		})

		It("uses custom time-format if specified", func() {
			req := &CreateTxnReq{
				ID:         "43583",
				CustomerID: "37648",
				LoadAmount: "$835.78",
				Time:       "2000-01-01T04:05",
				TimeFmt:    "2006-01-02T15:04",
			}
			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: CreateTxn,
				Data:   req,
			})
			Expect(err).ToNot(HaveOccurred())

			err = txnCreator.handleCreateTxnCmd(cmd)
			Expect(err).ToNot(HaveOccurred())

			createdTxn, err := expectEvent(successSub, failSub, TxnCreated)
			Expect(err).ToNot(HaveOccurred())
			Expect(createdTxn.ID).To(Equal(req.ID))
			Expect(createdTxn.CustomerID).To(Equal(req.CustomerID))
		})
	})
})
