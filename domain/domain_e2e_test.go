package domain

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Jaskaranbir/es-bank-account/domain/accountview"
	"github.com/Jaskaranbir/es-bank-account/domain/reader"
	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/Jaskaranbir/es-bank-account/domain_test"
	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var _ = Describe("Domain E2E", func() {
	var bus eventutil.Bus

	var ioReader *domain_test.MockReader
	var testData []txn.CreateTxnReq
	var ioWriter *domain_test.MockWriter

	var routinesGrp *errgroup.Group

	// Setup entities/configs and run them.
	BeforeEach(func() {
		var err error
		testData = []txn.CreateTxnReq{
			{
				ID:         "15887",
				CustomerID: "528",
				LoadAmount: "$3318.47",
				Time:       "2000-01-01T00:00:00Z",
			},
			{
				ID:         "16987",
				CustomerID: "898",
				LoadAmount: "-$33.47",
				Time:       "2000-01-02T00:00:00Z",
			},
			{
				ID:         "15887",
				CustomerID: "528",
				LoadAmount: "$3318.47",
				Time:       "2000-01-01T00:00:00Z",
			},
			{
				ID:         "14087",
				CustomerID: "197",
				LoadAmount: "$99",
				Time:       "2000-05-01T00:00:00Z",
			},
		}
		ioReader, err = domain_test.NewMockReader(testData)
		Expect(err).ToNot(HaveOccurred())
		ioWriter = domain_test.NewMockWriter()

		bus, err = eventutil.NewMemoryBus(logger.NewStdLogger("EventBus"))
		if err != nil {
			err = errors.Wrap(err, "error creating memory-bus")
			log.Fatalln(err)
		}
		cfgProvider := domain_test.ConfigProvider{}

		// ================== Account ==================
		accountCfg, err := cfgProvider.AccountRunCfg(bus)
		if err != nil {
			err = errors.Wrap(err, "error creating account-config")
			log.Fatalln(err)
		}

		// ================== TxnResultView ==================
		accountViewCfg := cfgProvider.AccountViewRunCfg(bus, accountCfg.AccountCfg.EventRepo)

		// ================== TxnCreator ==================
		txnCreatorCfg, err := cfgProvider.TxnCreatorRunCfg(bus)
		if err != nil {
			err = errors.Wrap(err, "error creating transaction-creator config")
			log.Fatalln(err)
		}

		// ================== Process-Manager ==================
		processMgrCfg := &ProcessMgrCfg{
			Log:               logger.NewStdLogger("ProcessMgr"),
			Bus:               bus,
			TxnResultViewRepo: accountViewCfg.ResultViewCfg.ResultRepo,

			WriteData:  model.WriteData,
			CreateTxn:  model.CreateTxn,
			ProcessTxn: model.ProcessTxn,

			TxnRead:         model.TxnRead,
			TxnCreated:      model.TxnCreated,
			TxnCreateFailed: model.TxnCreateFailed,
			ReportWritten:   model.DataWritten,

			ReportWrittenEventTimeoutSec: 3,
		}

		// ================== Reader ==================
		readerCfg := &reader.Cfg{
			Log:      logger.NewStdLogger("reader"),
			Bus:      bus,
			Reader:   ioReader,
			DataRead: model.TxnRead,
		}

		// ================== Writer ==================
		writerCfg, err := cfgProvider.WriterRunCfg(bus, ioWriter)
		if err != nil {
			err = errors.Wrap(err, "error creating writer-config")
			log.Fatalln(err)
		}

		// ================== Runner ==================
		routinesGrp = &errgroup.Group{}
		routinesGrp.Go(func() error {
			err = RunRoutines(&RoutinesCfg{
				Log:            logger.NewStdLogger("runner"),
				ReaderCfg:      readerCfg,
				TxnCreatorCfg:  txnCreatorCfg,
				AccountCfg:     accountCfg,
				AccountViewCfg: accountViewCfg,
				ProcessMgrCfg:  processMgrCfg,
				WriterCfg:      writerCfg,

				PostReadWaitIntervalSec: 3,
			})
			return errors.Wrap(err, "error running domain-routines")
		})
	})

	JustBeforeEach(func() {
		// Allow all routines to concurrently
		// process data and write result
		time.Sleep(5 * time.Second)
	})

	AfterEach(func() {
		err := routinesGrp.Wait()
		Expect(err).ToNot(HaveOccurred())
		bus.Terminate()
	})

	Specify("I/O validation", func() {
		expectedResults := make(map[string]bool)
		for _, testReq := range testData {
			txnKey := fmt.Sprintf("%s_%s", testReq.ID, testReq.CustomerID)
			_, keyExists := expectedResults[txnKey]
			expectedResults[txnKey] = !keyExists
		}

		actualResults := make(map[string]bool)
		resultStr := string(ioWriter.Content())
		results := strings.Split(resultStr, "\n")
		for _, result := range results {
			txnResult := &accountview.TxnResultEntry{}
			err := json.Unmarshal([]byte(result), txnResult)
			Expect(err).ToNot(HaveOccurred())

			txnKey := fmt.Sprintf("%s_%s", txnResult.ID, txnResult.CustomerID)
			_, keyExists := actualResults[txnKey]
			actualResults[txnKey] = !keyExists
		}

		Expect(expectedResults).To(Equal(actualResults))
	})
})
