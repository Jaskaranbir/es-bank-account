package config

import "os"

// TxnRequestTimeFmt is default time-format of transaction-times.
const TxnRequestTimeFmt = "2006-01-02T15:04:05Z"

// Account/transaction limits for each customer.
// Set to 0 to disable, values must be positive.
const (
	DailyTxnsAmountLimit  = 5000
	NumDailyTxnsLimit     = 3
	WeeklyTxnsAmountLimit = 20000
	NumWeeklyTxnsLimit    = 0
)

// Files to read/write data from/to respectively.
// Paths are relative to project-root (main.go).
const (
	InputFilePath  = "input.txt"
	OutputFilePath = "output.txt"
)

// ProcessMgrIdleTimeoutSec IdleTimeout for process-manager.
// Check process-manager docs for info on idle-timeout.
const ProcessMgrIdleTimeoutSec = 5

var defaultEnv = map[string]string{
	"LOG_LEVEL":          "debug",
	"EVENTBUS_LOG_LEVEL": "info",
}

func init() {
	for envVar, envVal := range defaultEnv {
		if os.Getenv(envVar) == "" {
			os.Setenv(envVar, envVal)
		}
	}
}
