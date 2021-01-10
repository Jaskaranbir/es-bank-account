package eventutil

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Jaskaranbir/es-bank-account/model"
)

var _ = Describe("MemoryUnpublishedLog", func() {
	const testEvent model.EventAction = "testEvent"
	var unpubLog *MemoryUnpublishedLog

	BeforeEach(func() {
		unpubLog = NewMemoryUnpublishedLog()
	})

	Specify("insert events", func() {
		event1, err := model.NewEvent(&model.EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())
		event2, err := model.NewEvent(&model.EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())

		err = unpubLog.Insert(event1)
		Expect(err).ToNot(HaveOccurred())
		err = unpubLog.Insert(event2)
		Expect(err).ToNot(HaveOccurred())
	})

	Specify("fetch events", func() {
		event1, err := model.NewEvent(&model.EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())
		event2, err := model.NewEvent(&model.EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())

		err = unpubLog.Insert(event1)
		Expect(err).ToNot(HaveOccurred())
		err = unpubLog.Insert(event2)
		Expect(err).ToNot(HaveOccurred())

		events, err := unpubLog.Events()
		Expect(err).ToNot(HaveOccurred())
		Expect(events).To(HaveLen(2))

		Expect(events[0]).To(Equal(event1))
		Expect(events[1]).To(Equal(event2))
	})

	When("popping events", func() {
		It("removes specified event from log", func() {
			event1, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			event2, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())

			err = unpubLog.Insert(event1)
			Expect(err).ToNot(HaveOccurred())
			err = unpubLog.Insert(event2)
			Expect(err).ToNot(HaveOccurred())

			err = unpubLog.Pop(event1)
			Expect(err).ToNot(HaveOccurred())
			events, err := unpubLog.Events()
			Expect(err).ToNot(HaveOccurred())
			Expect(events).To(HaveLen(1))
			Expect(events[0]).To(Equal(event2))

			err = unpubLog.Pop(event2)
			Expect(err).ToNot(HaveOccurred())
			events, err = unpubLog.Events()
			Expect(err).ToNot(HaveOccurred())
			Expect(events).To(HaveLen(0))
		})

		It("errors when event is not found in log", func() {
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			err = unpubLog.Pop(event)
			Expect(err).To(HaveOccurred())
		})
	})
})
