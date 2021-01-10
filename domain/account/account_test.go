package account

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

func TestAccount(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")

	RegisterFailHandler(Fail)
	RunSpecs(t, "Account Suite")
}

var _ = Describe("Account", func() {
	const busMsgReceiveTimeoutSec = 2
	const (
		ProcessTxnCmd model.CmdAction = "ProcessTxn"
	)
	const (
		AccountDepositedEvent     model.EventAction = "AccountDeposited"
		AccountWithdrawnEvent     model.EventAction = "AccountWithdrawn"
		DuplicateTxnEvent         model.EventAction = "DuplicateTxn"
		AccountLimitExceededEvent model.EventAction = "AccountLimitExceeded"
	)
	const (
		DailyTxnsAmountLimit  = 5000
		NumDailyTxnsLimit     = 3
		WeeklyTxnsAmountLimit = 20000
		NumWeeklyTxnsLimit    = 5
	)

	const txnTimeFmt = "2006-01-02T15:04:05Z"
	type mockCmdCfg struct {
		// Optional, gets generated if absent
		txnID      string
		customerID string
		// time should match txnTimeFmt
		time       string
		loadAmount float64
	}

	var bus eventutil.Bus
	var eventRepo eventutil.EventRepo
	var acc *account

	var mockCmd = func(cfgs ...mockCmdCfg) error {
		for _, cfg := range cfgs {
			txnUTCTime1, err := time.Parse(txnTimeFmt, cfg.time)
			if err != nil {
				return errors.Wrapf(err, "error parsing time into string: %+v", cfg)
			}

			if cfg.txnID == "" {
				txnID, err := uuid.NewRandom()
				if err != nil {
					return errors.Wrapf(err, "error generating transaction-id: %+v", cfg)
				}
				cfg.txnID = txnID.String()
			}

			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: ProcessTxnCmd,
				Data: &model.Transaction{
					ID:         cfg.txnID,
					CustomerID: cfg.customerID,
					LoadAmount: cfg.loadAmount,
					Time:       txnUTCTime1,
				},
			})
			if err != nil {
				return errors.Wrapf(err, "error creating command: %+v", cfg)
			}

			err = acc.handleProcessTxnCmd(cmd)
			if err != nil {
				return errors.Wrapf(err, "error in account command-handler: %+v", cfg)
			}
		}
		return nil
	}

	BeforeSuite(func() {
		SetDefaultEventuallyTimeout(busMsgReceiveTimeoutSec * time.Second)
	})

	BeforeEach(func() {
		var err error

		bus, err = eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())

		eventRepo, err = eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
			Bus:            bus,
			EventStore:     eventutil.NewMemoryEventStore(),
			UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
		})
		Expect(err).ToNot(HaveOccurred())

		acc, err = newAccount(&AggregateCfg{
			Log:       logger.NewStdLogger("Account"),
			EventRepo: eventRepo,

			AccountDeposited:     AccountDepositedEvent,
			AccountWithdrawn:     AccountWithdrawnEvent,
			DuplicateTxn:         DuplicateTxnEvent,
			AccountLimitExceeded: AccountLimitExceededEvent,

			DailyTxnsAmountLimit:  DailyTxnsAmountLimit,
			NumDailyTxnsLimit:     NumDailyTxnsLimit,
			WeeklyTxnsAmountLimit: WeeklyTxnsAmountLimit,
			NumWeeklyTxnsLimit:    NumWeeklyTxnsLimit,
		})
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		bus.Terminate()
	})

	When("creating new account-aggregate instance and weekly limits are specified", func() {
		It("errors if weekly-amount limit is less than daily-amount limit", func() {
			_, err := newAccount(&AggregateCfg{
				Log:       logger.NewStdLogger("Account"),
				EventRepo: eventRepo,

				AccountDeposited:     AccountDepositedEvent,
				AccountWithdrawn:     AccountWithdrawnEvent,
				DuplicateTxn:         DuplicateTxnEvent,
				AccountLimitExceeded: AccountLimitExceededEvent,

				DailyTxnsAmountLimit:  100,
				NumDailyTxnsLimit:     NumDailyTxnsLimit,
				WeeklyTxnsAmountLimit: 99,
				NumWeeklyTxnsLimit:    NumWeeklyTxnsLimit,
			})
			Expect(err).To(HaveOccurred())
		})

		It("errors if num of weekly-txns are greater than num of daily-txns", func() {
			_, err := newAccount(&AggregateCfg{
				Log:       logger.NewStdLogger("Account"),
				EventRepo: eventRepo,

				AccountDeposited:     AccountDepositedEvent,
				AccountWithdrawn:     AccountWithdrawnEvent,
				DuplicateTxn:         DuplicateTxnEvent,
				AccountLimitExceeded: AccountLimitExceededEvent,

				DailyTxnsAmountLimit:  DailyTxnsAmountLimit,
				NumDailyTxnsLimit:     4,
				WeeklyTxnsAmountLimit: WeeklyTxnsAmountLimit,
				NumWeeklyTxnsLimit:    3,
			})
			Expect(err).To(HaveOccurred())
		})
	})

	When("handling process-transaction command", func() {
		It("processes valid transactions", func() {
			accDepositedSub, err := bus.Subscribe(AccountDepositedEvent.String())
			Expect(err).ToNot(HaveOccurred())
			accWithdrawnSub, err := bus.Subscribe(string(AccountWithdrawnEvent.String()))
			Expect(err).ToNot(HaveOccurred())

			// Start listening for command early on to
			// prevent channel-deadlock (since multiple
			// channels are involved).
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				event := &model.Event{}
				// We expect 4 deposit and 1 withdrawal events
				// (as per sample-data used below)
				for i := 0; i < 4; i++ {
					Eventually(accDepositedSub).Should(Receive(event))
					Expect(event.Action()).To(Equal(AccountDepositedEvent))
				}
				Eventually(accWithdrawnSub).Should(Receive(event))
				Expect(event.Action()).To(Equal(AccountWithdrawnEvent))
			}()

			custID := "1"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "11",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-05T00:00:01Z",
				},
				mockCmdCfg{
					txnID:      "12",
					customerID: custID,
					loadAmount: 1000,
					time:       "2000-01-05T08:04:06Z",
				},
				mockCmdCfg{
					txnID:      "13",
					customerID: custID,
					loadAmount: -100,
					time:       "2000-01-06T15:04:06Z",
				},
				// Limit exceeds here
				mockCmdCfg{
					txnID:      "14",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-09T23:59:59Z",
				},
				// Next day
				mockCmdCfg{
					txnID:      "15",
					customerID: custID,
					loadAmount: 200,
					time:       "2000-01-14T23:59:59Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			wg.Wait()
		})

		It("declines duplicate transactions", func() {
			duplicateTxnSub, err := bus.Subscribe(DuplicateTxnEvent.String())
			Expect(err).ToNot(HaveOccurred())

			custID := "1"
			txnDate := "2000-01-05"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "10",
					customerID: custID,
					loadAmount: 100,
					time:       txnDate + "T03:04:06Z",
				},
				mockCmdCfg{
					txnID:      "10",
					customerID: custID,
					loadAmount: 100,
					time:       txnDate + "T04:04:06Z",
				},
				mockCmdCfg{
					txnID:      "20",
					customerID: custID,
					loadAmount: 150,
					time:       "2000-01-07T04:04:06Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			event := &model.Event{}
			Eventually(duplicateTxnSub).Should(Receive(event))
			txnFailure := &TxnFailure{}
			err = json.Unmarshal(event.Data(), txnFailure)
			Expect(err).ToNot(HaveOccurred())

			Expect(txnFailure.FailureCause).To(Equal(DuplicateTxn))
			Expect(txnFailure.Txn.ID).To(Equal("10"))
		})

		It("declines transaction when daily limit for num of transactions exceeds", func() {
			limitExceededSub, err := bus.Subscribe(AccountLimitExceededEvent.String())
			Expect(err).ToNot(HaveOccurred())

			custID := "1"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "11",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-05T00:00:01Z",
				},
				mockCmdCfg{
					txnID:      "12",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-05T08:04:06Z",
				},
				mockCmdCfg{
					txnID:      "13",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-05T15:04:06Z",
				},
				// Limit exceeds here
				mockCmdCfg{
					txnID:      "14",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-05T23:59:59Z",
				},
				// Next day
				mockCmdCfg{
					txnID:      "15",
					customerID: custID,
					loadAmount: 200,
					time:       "2000-01-06T23:59:59Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			event := &model.Event{}
			Eventually(limitExceededSub).Should(Receive(event))
			txnFailure := &TxnFailure{}
			err = json.Unmarshal(event.Data(), txnFailure)
			Expect(err).ToNot(HaveOccurred())

			Expect(txnFailure.FailureCause).To(Equal(DailyLimitsExceeded))
			Expect(txnFailure.Txn.ID).To(Equal("14"))
		})

		It("declines transaction when daily limit for amount exceeds", func() {
			limitExceededSub, err := bus.Subscribe(AccountLimitExceededEvent.String())
			Expect(err).ToNot(HaveOccurred())

			custID := "1"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "11",
					customerID: custID,
					loadAmount: 1000,
					time:       "2000-01-05T00:00:00Z",
				},
				mockCmdCfg{
					txnID:      "12",
					customerID: custID,
					loadAmount: 2500,
					time:       "2000-01-05T08:04:06Z",
				},
				mockCmdCfg{
					txnID:      "13",
					customerID: custID,
					loadAmount: 1500,
					time:       "2000-01-05T23:04:06Z",
				},

				// Daily amount-limit exceeds here
				mockCmdCfg{
					txnID:      "14",
					customerID: custID,
					loadAmount: 1000,
					time:       "2000-01-05T23:59:59Z",
				},
				// Next day
				mockCmdCfg{
					txnID:      "15",
					customerID: custID,
					loadAmount: 2500,
					time:       "2000-01-06T00:00:01Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			event := &model.Event{}
			Eventually(limitExceededSub).Should(Receive(event))
			txnFailure := &TxnFailure{}
			err = json.Unmarshal(event.Data(), txnFailure)
			Expect(err).ToNot(HaveOccurred())

			Expect(txnFailure.FailureCause).To(Equal(DailyLimitsExceeded))
			Expect(txnFailure.Txn.ID).To(Equal("14"))
		})

		It("declines transaction when weekly limit for num of transactions exceeds", func() {
			limitExceededSub, err := bus.Subscribe(AccountLimitExceededEvent.String())
			Expect(err).ToNot(HaveOccurred())

			custID := "1"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "11",
					customerID: custID,
					loadAmount: 100,
					time:       "2000-01-03T03:04:06Z",
				},
				mockCmdCfg{
					txnID:      "12",
					customerID: custID,
					loadAmount: 180,
					time:       "2000-01-05T04:40:06Z",
				},
				mockCmdCfg{
					txnID:      "13",
					customerID: custID,
					loadAmount: 200,
					time:       "2000-01-06T05:04:06Z",
				},
				mockCmdCfg{
					txnID:      "14",
					customerID: custID,
					loadAmount: 1000,
					time:       "2000-01-07T06:04:06Z",
				},
				mockCmdCfg{
					txnID:      "15",
					customerID: custID,
					loadAmount: 1400,
					time:       "2000-01-09T06:04:06Z",
				},
				// Limit exceeds here
				mockCmdCfg{
					txnID:      "16",
					customerID: custID,
					loadAmount: 1400,
					time:       "2000-01-09T06:04:06Z",
				},
				// Monday next week
				mockCmdCfg{
					txnID:      "17",
					customerID: custID,
					loadAmount: 500,
					time:       "2000-01-10T06:04:06Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			event := &model.Event{}
			Eventually(limitExceededSub).Should(Receive(event))
			txnFailure := &TxnFailure{}
			err = json.Unmarshal(event.Data(), txnFailure)
			Expect(err).ToNot(HaveOccurred())

			Expect(txnFailure.FailureCause).To(Equal(WeeklyLimitsExceeded))
			Expect(txnFailure.Txn.ID).To(Equal("16"))
		})

		It("declines transaction when weekly limit for amount exceeds", func() {
			limitExceededSub, err := bus.Subscribe(AccountLimitExceededEvent.String())
			Expect(err).ToNot(HaveOccurred())

			custID := "1"
			// Add/process sample transactions
			err = mockCmd(
				mockCmdCfg{
					txnID:      "11",
					customerID: custID,
					loadAmount: 2500,
					time:       "2000-01-03T00:00:01Z",
				},
				mockCmdCfg{
					txnID:      "12",
					customerID: custID,
					loadAmount: 2500,
					time:       "2000-01-05T03:04:06Z",
				},
				mockCmdCfg{
					txnID:      "13",
					customerID: custID,
					loadAmount: 5000,
					time:       "2000-01-06T04:04:06Z",
				},
				mockCmdCfg{
					txnID:      "14",
					customerID: custID,
					loadAmount: 5000,
					time:       "2000-01-07T04:04:06Z",
				},
				// Weekly amount-limit exceeds here
				mockCmdCfg{
					txnID:      "15",
					customerID: custID,
					loadAmount: 5001,
					time:       "2000-01-09T23:59:59Z",
				},
				// Monday next week
				mockCmdCfg{
					txnID:      "16",
					customerID: custID,
					loadAmount: 1,
					time:       "2000-01-10T00:00:00Z",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			event := &model.Event{}
			Eventually(limitExceededSub).Should(Receive(event))
			txnFailure := &TxnFailure{}
			err = json.Unmarshal(event.Data(), txnFailure)
			Expect(err).ToNot(HaveOccurred())

			Expect(txnFailure.FailureCause).To(Equal(DailyLimitsExceeded))
			Expect(txnFailure.Txn.ID).To(Equal("15"))
		})
	})

	When("daily and weekly limits are unspecified", func() {
		JustBeforeEach(func() {
			var err error

			acc, err = newAccount(&AggregateCfg{
				Log:       logger.NewStdLogger("Account"),
				EventRepo: eventRepo,

				AccountDeposited:     AccountDepositedEvent,
				AccountWithdrawn:     AccountWithdrawnEvent,
				DuplicateTxn:         DuplicateTxnEvent,
				AccountLimitExceeded: AccountLimitExceededEvent,

				DailyTxnsAmountLimit:  0,
				NumDailyTxnsLimit:     0,
				WeeklyTxnsAmountLimit: 0,
				NumWeeklyTxnsLimit:    0,
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("allows unlimited daily and weekly transactions", func() {
			accDepositedSub, err := bus.Subscribe(AccountDepositedEvent.String())
			Expect(err).ToNot(HaveOccurred())

			expectedNumTxns := 100

			custID := "1"
			mockCmds := make([]mockCmdCfg, 0)
			// All transactions occur on same day,
			// so we test daily/weekly at same time
			for i := 0; i < expectedNumTxns; i++ {
				mockCmds = append(mockCmds, mockCmdCfg{
					txnID:      strconv.Itoa(i),
					customerID: custID,
					loadAmount: float64(10000 * (i + 1)),
					time:       "2000-01-05T00:00:01Z",
				})
			}

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				for i := 0; i < expectedNumTxns; i++ {
					event := &model.Event{}
					Eventually(accDepositedSub).Should(Receive(event))
					Expect(event.Action()).To(Equal(AccountDepositedEvent))
				}
			}()

			// Add/process sample transactions
			err = mockCmd(mockCmds...)
			Expect(err).ToNot(HaveOccurred())
			wg.Wait()
		})
	})
})
