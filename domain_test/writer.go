package domain_test

import (
	"bytes"
	"sync"
)

// MockWriter implements io.Writer and
// allows inspecting the data-written.
// Use #NewMockWriter to create new instance.
type MockWriter struct {
	content []byte
	lock    *sync.RWMutex
}

// NewMockWriter creates new
// instance of MockWriter.
func NewMockWriter() *MockWriter {
	return &MockWriter{
		content: make([]byte, 0),
		lock:    &sync.RWMutex{},
	}
}

func (w *MockWriter) Write(p []byte) (n int, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.content = append(w.content, p...)
	return len(p), nil
}

// Content returns copy of content
// that was written via #Write.
func (w *MockWriter) Content() []byte {
	w.lock.RLock()
	defer w.lock.RUnlock()

	t := bytes.Trim(w.content, "\n\n")
	content := make([]byte, len(t))
	copy(content, t)
	return content
}
