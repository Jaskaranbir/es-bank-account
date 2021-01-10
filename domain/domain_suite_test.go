package domain

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	os.Setenv("LOG_LEVEL", "warn")
	// E2E tests should not generate any warnings
	// since all components required are running
	os.Setenv("EVENTBUS_LOG_LEVEL", "warn")

	RegisterFailHandler(Fail)
	RunSpecs(t, "Domain Suite")
}
