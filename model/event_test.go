package model

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewEvent", func() {
	const testEvent EventAction = "testEvent"

	Context("setting time", func() {
		It("should set time if not already set", func() {
			event, err := NewEvent(&EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(event.Time().IsZero()).To(BeFalse())
		})

		It("should use existing time if already set", func() {
			t := time.Now()
			event, err := NewEvent(&EventCfg{
				AggregateID: "1",
				Time:        t,
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())

			Expect(event.Time().IsZero()).To(BeFalse())
			Expect(event.Time().UnixNano()).To(Equal(t.UnixNano()))
		})
	})

	It("should generate event-id", func() {
		event, err := NewEvent(&EventCfg{
			AggregateID: "1",
			Action:      testEvent,
			Data:        []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(event.ID()).ToNot(BeEmpty())
	})

	Context("setting data", func() {
		It("sets data as is if data-type is bytes", func() {
			data := "test-data"
			event, err := NewEvent(&EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(string(event.Data())).To(Equal(data))

			int64Data := int64(33)
			int64Bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(int64Bytes, uint64(int64Data))
			event, err = NewEvent(&EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        int64Bytes,
			})
			Expect(err).ToNot(HaveOccurred())
			eventData := int64(binary.LittleEndian.Uint64(event.Data()))
			Expect(eventData).To(Equal(int64Data))
		})

		It("json-marshals data if data-type is not bytes", func() {
			type dataStruct struct {
				Field1 string
				Field2 int
			}
			data := dataStruct{
				Field1: "test",
				Field2: 140,
			}
			event, err := NewEvent(&EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        data,
			})
			Expect(err).ToNot(HaveOccurred())

			unmarshData := &dataStruct{}
			err = json.Unmarshal(event.Data(), unmarshData)
			Expect(err).ToNot(HaveOccurred())
			Expect(*unmarshData).To(Equal(data))
		})

		It("sets correlation-key correctly", func() {
			key, err := uuid.NewRandom()
			Expect(err).ToNot(HaveOccurred())

			event, err := NewEvent(&EventCfg{
				AggregateID:    "1",
				CorrelationKey: key.String(),
				Action:         testEvent,
				Data:           []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(event.CorrelationKey()).To(Equal(key.String()))
		})
	})
})
