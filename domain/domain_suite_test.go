package domain

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")
	os.Setenv("EVENTBUS_LOG_LEVEL", "warn")

	RegisterFailHandler(Fail)
	RunSpecs(t, "Domain Suite")
}
