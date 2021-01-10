package accountview

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// TxnResultViewRepo handles storing/retrieving transaction-results.
type TxnResultViewRepo interface {
	Insert(result TxnResultEntry) error
	Serialized() string
	Index() int
}

// MemoryTxnResultViewRepo is an in-memory TxnResultViewRepo.
// Use #NewMemoryTxnResultViewRepo to create new instance.
type MemoryTxnResultViewRepo struct {
	lock            *sync.RWMutex
	serializedIndex []byte
	index           int
}

// TxnResultEntry reprents a record in TransactionResultViewRepo.
type TxnResultEntry struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Accepted   bool   `json:"accepted"`
}

// NewMemoryTxnResultViewRepo creates a new instance of MemoryTxnResultViewRepo.
func NewMemoryTxnResultViewRepo() *MemoryTxnResultViewRepo {
	return &MemoryTxnResultViewRepo{
		lock:            &sync.RWMutex{},
		serializedIndex: make([]byte, 0),
		index:           0,
	}
}

// Insert inserts a record into MemoryTxnResultViewRepo.
func (rv *MemoryTxnResultViewRepo) Insert(result TxnResultEntry) error {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return errors.Wrap(err, "error marshalling result to json")
	}

	rv.lock.Lock()
	defer rv.lock.Unlock()

	// Change here if valid JSON is required
	// instead of custom serialized-string.
	rv.serializedIndex = append(rv.serializedIndex, resultBytes...)
	rv.serializedIndex = append(rv.serializedIndex, []byte("\n")...)
	rv.index++
	return nil
}

// Serialized returns all results in a pre-defined serialized-format.
func (rv *MemoryTxnResultViewRepo) Serialized() string {
	rv.lock.RLock()
	defer rv.lock.RUnlock()

	results := string(rv.serializedIndex)
	if len(results) > 0 && strings.HasSuffix(results, "\n") {
		// Remove last newline char
		results = results[:len(results)-1]
	}
	return results
}

// Index returns event-repo index of last event processed by repo.
func (rv *MemoryTxnResultViewRepo) Index() int {
	rv.lock.RLock()
	defer rv.lock.RUnlock()

	return rv.index
}
