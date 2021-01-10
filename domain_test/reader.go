package domain_test

import (
	"encoding/json"
	"io"

	"github.com/Jaskaranbir/es-bank-account/domain/txn"
	"github.com/pkg/errors"
)

// MockReader implements io.Reader interface to
// allow in-memory reading of transaction-requests.
// Use #NewMockReader to create new instance.
type MockReader struct {
	data []byte
}

// NewMockReader creates new instance of MockReader.
func NewMockReader(reqs []txn.CreateTxnReq) (*MockReader, error) {
	data := make([]byte, 0)

	for i, req := range reqs {
		reqBytes, err := json.Marshal(req)
		if err != nil {
			return nil, errors.Wrapf(err, "error json-marshalling request: %+v", req)
		}

		data = append(data, reqBytes...)
		if i < len(reqs)-1 {
			data = append(data, []byte("\n")...)
		}
	}

	return &MockReader{
		data: data,
	}, nil
}

func (r MockReader) eof() bool {
	return len(r.data) == 0
}

func (r *MockReader) Read(p []byte) (int, error) {
	if r.eof() {
		return 0, io.EOF
	}

	n := 0

	if cap(p) > 0 {
		for ; n < cap(p) && !r.eof(); n++ {
			// Read single byte
			p[n] = r.data[0]
			r.data = r.data[1:]
		}
	}

	return n, nil
}
