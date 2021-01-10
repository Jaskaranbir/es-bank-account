package eventutil

import (
	"bytes"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/Jaskaranbir/es-bank-account/model"
)

var _ = Describe("MemoryEventStore", func() {
	const testEvent model.EventAction = "testEvent"
	var store *MemoryEventStore

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
		store = NewMemoryEventStore()

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

	When("storing events", func() {
		It("stores valid events", func() {
			testDataArr := append(agg1Events, agg2Events...)

			for _, eventData := range testDataArr {
				event, err := model.NewEvent(&model.EventCfg{
					AggregateID: eventData.aggID,
					Time:        eventData.time,
					Action:      testEvent,
					Data:        eventData.data,
				})
				Expect(err).ToNot(HaveOccurred())
				err = store.Insert(event)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		It("ignores duplicate events", func() {
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())

			err = store.Insert(event)
			Expect(err).ToNot(HaveOccurred())
			err = store.Insert(event)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors when missing aggregate-id in event", func() {
			event, err := model.NewEvent(&model.EventCfg{
				Action: testEvent,
				Data:   []byte("test-data"),
			})
			Expect(err).To(HaveOccurred())

			err = store.Insert(event)
			Expect(err).To(HaveOccurred())
		})
	})

	It("fetches events by aggregate-id", func() {
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
			err = store.Insert(event)
			Expect(err).ToNot(HaveOccurred())
		}

		// ============ Validation/Tests ============
		agg1 := agg1Events[0].aggID
		agg2 := agg2Events[0].aggID

		agg1StoreEvents, err := store.Fetch(agg1)
		Expect(err).ToNot(HaveOccurred())
		err = validateEvents(agg1StoreEvents, agg1Events)
		Expect(err).ToNot(HaveOccurred())

		agg2StoreEvents, err := store.Fetch(agg2)
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
				err = store.Insert(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := 0
			events, err := store.FetchByIndex(index)
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
				err = store.Insert(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := 2
			events, err := store.FetchByIndex(index)
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
				err = store.Insert(event)
				Expect(err).ToNot(HaveOccurred())
			}

			index := len(testDataArr)
			events, err := store.FetchByIndex(index)
			Expect(err).ToNot(HaveOccurred())
			err = validateEvents(events, testDataArr[index:])
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
