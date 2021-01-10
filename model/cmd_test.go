package model

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewCmd", func() {
	const testCmd CmdAction = "testCmd"

	Context("setting time", func() {
		It("should set time if not already set", func() {
			cmd, err := NewCmd(&CmdCfg{
				Action: testCmd,
				Data:   []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(cmd.Time().IsZero()).To(BeFalse())
		})

		It("should use existing time if already set", func() {
			t := time.Now()
			cmd, err := NewCmd(&CmdCfg{
				Time:   t,
				Action: testCmd,
				Data:   []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())

			Expect(cmd.Time().IsZero()).To(BeFalse())
			Expect(cmd.Time().UnixNano()).To(Equal(t.UnixNano()))
		})
	})

	It("should generate command-id", func() {
		cmd, err := NewCmd(&CmdCfg{
			Action: testCmd,
			Data:   []byte("test-data"),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(cmd.ID()).ToNot(BeEmpty())
	})

	Context("setting data", func() {
		It("sets data as is if data-type is bytes", func() {
			data := "test-data"
			cmd, err := NewCmd(&CmdCfg{
				Action: testCmd,
				Data:   []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(string(cmd.Data())).To(Equal(data))

			int64Data := int64(33)
			int64Bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(int64Bytes, uint64(int64Data))
			cmd, err = NewCmd(&CmdCfg{
				Action: testCmd,
				Data:   int64Bytes,
			})
			Expect(err).ToNot(HaveOccurred())
			cmdData := int64(binary.LittleEndian.Uint64(cmd.Data()))
			Expect(cmdData).To(Equal(int64Data))
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
			cmd, err := NewCmd(&CmdCfg{
				Action: testCmd,
				Data:   data,
			})
			Expect(err).ToNot(HaveOccurred())

			unmarshData := &dataStruct{}
			err = json.Unmarshal(cmd.Data(), unmarshData)
			Expect(err).ToNot(HaveOccurred())
			Expect(*unmarshData).To(Equal(data))
		})

		It("sets correlation-key correctly", func() {
			key, err := uuid.NewRandom()
			Expect(err).ToNot(HaveOccurred())

			cmd, err := NewCmd(&CmdCfg{
				CorrelationKey: key.String(),
				Action:         testCmd,
				Data:           []byte("test-data"),
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(cmd.CorrelationKey()).To(Equal(key.String()))
		})
	})
})
