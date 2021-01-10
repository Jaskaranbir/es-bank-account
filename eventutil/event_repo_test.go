package eventutil

import (
	"bytes"
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

var _ = Describe("LoggedEventRepo", func() {
	const testEvent model.EventAction = "testEvent"
	var eventRepo *LoggedEventRepo
	var bus Bus

	type testEventData struct {
		aggID string
		time  time.Time
		data  []byte
	}

	var validateEvents = func(events []model.Event, testEvents []testEventData) error {
		if len(events) != len(testEvents) {
			return errors.New("length mismatch")
		}

		for i := 0; i < len(events); i++ {
			event := events[i]
			testData := testEvents[i]

			dataEqual := bytes.Equal(event.Data(), testData.data)
			if !dataEqual {
				return fmt.Errorf("data mismatch")
			}
			if event.Time().UnixNano() != testData.time.UnixNano() {
				return errors.New("time mismatch")
			}
			if event.AggregateID() != testData.aggID {
				return errors.New("id mismatch")
			}
		}
		return nil
	}

	var (
		// We dont use slice/map here to have
		// more individual control and significance
		// of these IDs, since each ID should have
		// very particular events associated
		agg1Events []testEventData
		agg2Events []testEventData
	)

	BeforeEach(func() {
		var err error
		bus, err = NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())

		eventRepo, err = NewLoggedEventRepo(&LoggedEventRepoCfg{
			Bus:            bus,
			EventStore:     NewMemoryEventStore(),
			UnpublishedLog: NewMemoryUnpublishedLog(),
		})

		// Generates data for an aggregate with
		// specified number of events with random
		// data
		genDummyEventData := func(numEvents int) []testEventData {
			testEvents := make([]testEventData, numEvents)
			aggID, err := uuid.NewRandom()
			Expect(err).ToNot(HaveOccurred())

			for i := 0; i < numEvents; i++ {
				data, err := uuid.NewRandom()
				Expect(err).ToNot(HaveOccurred())

				testEvents[i] = testEventData{
					aggID: aggID.String(),
					time:  time.Now().UTC(),
					data:  []byte(data.String()),
				}
			}
			return testEvents
		}

		agg1Events = genDummyEventData(3)
		agg2Events = genDummyEventData(2)
	})

	AfterEach(func() {
		bus.Terminate()
	})

	It("inserts event into event-store and publishes on bus", func() {
		sub, err := bus.Subscribe(testEvent.String())
		Expect(err).ToNot(HaveOccurred())
		event, err := model.NewEvent(&model.EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())

		grp, _ := errgroup.WithContext(context.Background())
		grp.Go(func() error {
			select {
			case msg := <-sub:
				msgEvent, castSuccess := msg.(model.Event)
				if !castSuccess {
					return errors.New("failed casting message")
				}
				if msgEvent.ID() != event.ID() {
					return errors.New("event mismatched")
				}
			case <-time.After(1 * time.Second):
				return errors.New("timed-out waiting for event on bus")
			}
			return nil
		})

		err = eventRepo.InsertAndPublish(event)
		Expect(err).ToNot(HaveOccurred())
		err = grp.Wait()
		Expect(err).ToNot(HaveOccurred())

		repoEvents, err := eventRepo.Fetch("1")
		Expect(err).ToNot(HaveOccurred())
		Expect(repoEvents).To(HaveLen(1))
		Expect(repoEvents[0]).To(Equal(event))
	})

	It("fetches events by aggregateID", func() {
		// ============ Insert Dummy Events ============
		testDataArr := append(agg1Events, agg2Events...)

		for _, eventData := range testDataArr {
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: eventData.aggID,
				Time:        eventData.time,
				Action:      testEvent,
				Data:        eventData.data,
			})
			Expect(err).ToNot(HaveOccurred())
			err = eventRepo.InsertAndPublish(event)
			Expect(err).ToNot(HaveOccurred())
		}

		// ============ Validation/Tests ============
		agg1 := agg1Events[0].aggID
		agg2 := agg2Events[0].aggID

		agg1StoreEvents, err := eventRepo.Fetch(agg1)
		Expect(err).ToNot(HaveOccurred())
		err = validateEvents(agg1StoreEvents, agg1Events)
		Expect(err).ToNot(HaveOccurred())

		agg2StoreEvents, err := eventRepo.Fetch(agg2)
		Expect(err).ToNot(HaveOccurred())
		err = validateEvents(agg2StoreEvents, agg2Events)
		Expect(err).ToNot(HaveOccurred())
	})

	When("fetching events by index", func() {
		It("fetches all events when index is 0", func() {
			testDataArr := append(agg1Events, agg2Events...)

			for _, eventData := range testDataArr {
				event, err := model.NewEvent(&model.EventCfg{
					AggregateID: eventData.aggID,
					Time:        eventData.time,
					Action:      testEvent,
					Data:        eventData.data,
				})
				Expect(err).ToNot(HaveOccurred())
				err = eventRepo.InsertAndPublish(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := 0
			events, err := eventRepo.FetchByIndex(index)
			Expect(err).ToNot(HaveOccurred())
			err = validateEvents(events, testDataArr)
			Expect(err).ToNot(HaveOccurred())
		})

		It("fetches missing events based on index", func() {
			testDataArr := append(agg1Events, agg2Events...)

			for _, eventData := range testDataArr {
				event, err := model.NewEvent(&model.EventCfg{
					AggregateID: eventData.aggID,
					Time:        eventData.time,
					Action:      testEvent,
					Data:        eventData.data,
				})
				Expect(err).ToNot(HaveOccurred())
				err = eventRepo.InsertAndPublish(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := 2
			events, err := eventRepo.FetchByIndex(index)
			Expect(err).ToNot(HaveOccurred())
			err = validateEvents(events, testDataArr[index:])
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns no events on last index", func() {
			testDataArr := append(agg1Events, agg2Events...)

			for _, eventData := range testDataArr {
				event, err := model.NewEvent(&model.EventCfg{
					AggregateID: eventData.aggID,
					Time:        eventData.time,
					Action:      testEvent,
					Data:        eventData.data,
				})
				Expect(err).ToNot(HaveOccurred())
				err = eventRepo.InsertAndPublish(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := len(testDataArr)
			events, err := eventRepo.FetchByIndex(index)
			Expect(err).ToNot(HaveOccurred())
			err = validateEvents(events, testDataArr[index:])
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
