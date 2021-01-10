package eventutil

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

var _ = Describe("MemoryBus", func() {
	const testEvent model.EventAction = "testEvent"
	var bus *MemoryBus

	BeforeSuite(func() {
		SetDefaultEventuallyTimeout(1 * time.Second)
	})

	BeforeEach(func() {
		var err error
		bus, err = NewMemoryBus(logger.NewStdLogger("EventBus"))
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		bus.Terminate()
	})

	When("validating message before publishing", func() {
		It("allows an event as message", func() {
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(event)
			Expect(err).ToNot(HaveOccurred())
		})

		It("allows an command as message", func() {
			const testCmd model.CmdAction = "testCmd"

			cmd, err := model.NewCmd(&model.CmdCfg{
				Action: testCmd,
				Data:   []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(cmd)
			Expect(err).ToNot(HaveOccurred())
		})

		It("errors when message isn't command or event ", func() {
			err := bus.Publish("invalid-message")
			Expect(err).To(HaveOccurred())

			err = bus.Publish(nil)
			Expect(err).To(HaveOccurred())
		})
	})

	When("subscribing to message-action", func() {
		It("subscribes to message-action", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				event, err := model.NewEvent(&model.EventCfg{
					AggregateID: "1",
					Action:      testEvent,
					Data:        []byte("test-data"),
				})
				Expect(err).ToNot(HaveOccurred())
				err = bus.Publish(event)
				Expect(err).ToNot(HaveOccurred())
			}()

			Eventually(sub).Should(Receive())
		})

		It("returns error if message-action is blank", func() {
			_, err := bus.Subscribe("")
			Expect(err).To(HaveOccurred())
		})
	})

	When("unsubscribing from message-action", func() {
		It("unsubscribes from message-action", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			bus.Unsubscribe(sub, testEvent.String())
			Eventually(sub).Should(BeClosed())
		})

		It("errors when unsubscribing with invalid channel", func() {
			_, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			invalidSub := make(chan interface{})
			err = bus.Unsubscribe(invalidSub, testEvent.String())
			Expect(err).To(HaveOccurred())
		})

		It("errors when message-action is blank", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			err = bus.Unsubscribe(sub, "")
			Expect(err).To(HaveOccurred())
		})

		It("errors when subscription doesnt exist", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			err = bus.Unsubscribe(sub, "invalid-action")
			Expect(err).To(HaveOccurred())
		})

		It("errors when unsubscribing same subscription multiple times", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			err = bus.Unsubscribe(sub, testEvent.String())
			Expect(err).ToNot(HaveOccurred())
			err = bus.Unsubscribe(sub, testEvent.String())
			Expect(err).To(HaveOccurred())
		})
	})

	When("terminating bus", func() {
		It("should close subscription", func() {
			sub, err := bus.Subscribe(testEvent.String())
			Expect(err).ToNot(HaveOccurred())

			bus.Terminate()
			Eventually(sub).Should(BeClosed())
		})

		It("errors when subscribing", func() {
			bus.Terminate()
			_, err := bus.Subscribe(testEvent.String())
			Expect(err).To(HaveOccurred())
		})

		It("errors when publishing", func() {
			bus.Terminate()
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: "1",
				Action:      testEvent,
				Data:        []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			err = bus.Publish(event)
			Expect(err).To(HaveOccurred())
		})
	})
})
