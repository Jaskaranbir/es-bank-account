package accountview

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jaskaranbir/es-bank-account/domain/account"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestTxnResultView(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")

	RegisterFailHandler(Fail)
	RunSpecs(t, "TxnResultView Suite")
}

var _ = Describe("TxnResultView", func() {
	const (
		AccountDeposited     model.EventAction = "AccountDeposited"
		AccountWithdrawn     model.EventAction = "AccountWithdrawn"
		DuplicateTxn         model.EventAction = "DuplicateTxn"
		AccountLimitExceeded model.EventAction = "AccountLimitExceeded"
	)

	var bus eventutil.Bus
	var resultViewCfg *TxnResultViewCfg
	var resultView *txnResultView

	var hydrateAndMarshal = func(
		resultData interface{},
		action model.EventAction,
	) (string, error) {
		// ================ Create/insert event ================
		aggID, err := uuid.NewRandom()
		if err != nil {
			return "", errors.New("error generating aggregate-id")
		}
		event, err := model.NewEvent(&model.EventCfg{
			AggregateID: aggID.String(),
			Action:      action,
			Data:        resultData,
		})
		if err != nil {
			return "", errors.Wrap(err, "error creating event")
		}
		err = resultViewCfg.EventRepo.InsertAndPublish(event)
		if err != nil {
			return "", errors.Wrap(err, "error inserting event into event-repo")
		}

		err = resultView.hydrate()
		if err != nil {
			return "", errors.Wrap(err, "error hydrating result-view repo")
		}

		// ================ Marshal result-data ================
		var data TxnResultEntry
		switch v := resultData.(type) {
		case *account.State:
			data = TxnResultEntry{
				ID:         v.TxnID,
				CustomerID: v.CustID,
				Accepted:   true,
			}
		case *account.TxnFailure:
			data = TxnResultEntry{
				ID:         v.Txn.ID,
				CustomerID: v.Txn.CustomerID,
				Accepted:   false,
			}
		default:
			return "", errors.New("received result-data of unknown type")
		}

		viewEntryBytes, err := json.Marshal(data)
		if err != nil {
			return "", errors.Wrap(err, "error json-marshalling view-entry")
		}
		return string(viewEntryBytes), nil
	}

	BeforeEach(func() {
		var err error
		bus, err = eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())

		resultViewRepo := NewMemoryTxnResultViewRepo()
		eventRepo, err := eventutil.NewLoggedEventRepo(&eventutil.LoggedEventRepoCfg{
			Bus:            bus,
			EventStore:     eventutil.NewMemoryEventStore(),
			UnpublishedLog: eventutil.NewMemoryUnpublishedLog(),
		})
		Expect(err).ToNot(HaveOccurred())

		resultViewCfg = &TxnResultViewCfg{
			Log:        logger.NewStdLogger("TxnResultView"),
			ResultRepo: resultViewRepo,
			EventRepo:  eventRepo,

			AccountDeposited:     AccountDeposited,
			AccountWithdrawn:     AccountWithdrawn,
			DuplicateTxn:         DuplicateTxn,
			AccountLimitExceeded: AccountLimitExceeded,
		}
		resultView, err = newTxnResultView(resultViewCfg)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		bus.Terminate()
	})

	Context("hydrating view-repository", func() {
		// Here we generate some mock events, hydrate
		// transaction-result view-repo, and check
		// if all events have their respective views.
		It("hydrates repo with missing events", func() {
			resultRepo := resultViewCfg.ResultRepo

			accState1 := &account.State{
				TxnID:   "43673",
				CustID:  "38964",
				TxnTime: time.Now(),
				DailyTxn: account.TxnRecord{
					NumTxns:     1,
					TotalAmount: 100,
				},
				WeeklyTxn: account.TxnRecord{
					NumTxns:     1,
					TotalAmount: 100,
				},
			}
			serResultView, err := hydrateAndMarshal(accState1, AccountDeposited)
			Expect(err).ToNot(HaveOccurred())
			Expect(resultRepo.Serialized()).To(Equal(serResultView))

			accState2 := &account.State{
				TxnID:   "94648",
				CustID:  "29904",
				TxnTime: time.Now(),
				DailyTxn: account.TxnRecord{
					NumTxns:     4,
					TotalAmount: 600,
				},
				WeeklyTxn: account.TxnRecord{
					NumTxns:     6,
					TotalAmount: 800,
				},
			}
			newSerResultView, err := hydrateAndMarshal(accState2, AccountDeposited)
			Expect(err).ToNot(HaveOccurred())
			serResultView = fmt.Sprintf("%s\n%s", serResultView, newSerResultView)
			Expect(resultRepo.Serialized()).To(Equal(serResultView))

			txnFailure1 := &account.TxnFailure{
				Txn: model.Transaction{
					ID:         "46232",
					CustomerID: "45222",
					LoadAmount: 456.75,
					Time:       time.Now(),
				},
				Error:        "dummy-error",
				FailureCause: account.DuplicateTxn,
			}
			newSerResultView, err = hydrateAndMarshal(txnFailure1, DuplicateTxn)
			Expect(err).ToNot(HaveOccurred())
			serResultView = fmt.Sprintf("%s\n%s", serResultView, newSerResultView)
			Expect(resultRepo.Serialized()).To(Equal(serResultView))

			txnFailure2 := &account.TxnFailure{
				Txn: model.Transaction{
					ID:         "461236",
					CustomerID: "6739",
					LoadAmount: 6376.34,
					Time:       time.Now(),
				},
				Error:        "another-dummy-error",
				FailureCause: account.DailyLimitsExceeded,
			}
			newSerResultView, err = hydrateAndMarshal(txnFailure2, DuplicateTxn)
			Expect(err).ToNot(HaveOccurred())
			serResultView = fmt.Sprintf("%s\n%s", serResultView, newSerResultView)
			Expect(resultRepo.Serialized()).To(Equal(serResultView))
		})
	})
})
