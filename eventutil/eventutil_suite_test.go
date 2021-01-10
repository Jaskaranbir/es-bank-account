package eventutil

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEventUtil(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")
	os.Setenv("EVENTBUS_LOG_LEVEL", "error")

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventUtil Suite")
}
