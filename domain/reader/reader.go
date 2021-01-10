package reader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// Reader reads the data line-by-line basis from provided io.Reader,
// and publishes this data to provided topic on bus.
// Use #NewReader to create new instance.
type Reader struct {
	log     logger.Logger
	scanner *bufio.Scanner

	bus      eventutil.Bus
	dataRead model.EventAction
}

// Cfg defines config for Reader.
type Cfg struct {
	Log    logger.Logger `validate:"nonnil"`
	Reader io.Reader     `validate:"nonnil"`

	Bus      eventutil.Bus     `validate:"nonnil"`
	DataRead model.EventAction `validate:"nonzero"`
}

// NewReader validates Reader-Config
// and creates new Reader-instance.
func NewReader(cfg *Cfg) (*Reader, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, err
	}

	return &Reader{
		log:     cfg.Log,
		scanner: bufio.NewScanner(cfg.Reader),

		bus:      cfg.Bus,
		dataRead: cfg.DataRead,
	}, nil
}

// Start runs the loop which reads lines from provided
// io.Reader and listens for context-signal.
func (r *Reader) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}

	r.log.Infof("Started reading")

	for r.scanner.Scan() {
		select {
		case <-ctx.Done():
			r.log.Debug("Received context-done signal")
			err := r.scanner.Err()
			if err != nil {
				return errors.Wrap(err, "errored reading data")
			}
			return nil

		default:
			data := r.scanner.Text()
			trimmedData := strings.ReplaceAll(data, "\n", "")
			if trimmedData == "" {
				continue
			}

			aggID, err := uuid.NewRandom()
			if err != nil {
				return errors.Wrap(err, "error generating aggregate-id")
			}
			event, err := model.NewEvent(&model.EventCfg{
				AggregateID: aggID.String(),
				Action:      r.dataRead,
				Data:        []byte(data),
			})
			if err != nil {
				return errors.Wrap(err, "error creating event")
			}
			logPrefix := fmt.Sprintf("[Event: %s]:", event.ID())

			r.log.Tracef("%s Publishing newly read data", logPrefix)
			err = r.bus.Publish(event)
			if err != nil {
				return errors.Wrap(err, "error publishing to bus")
			}
			r.log.Tracef("%s Published newly read data", logPrefix)
		}
	}

	r.log.Debug("Finished reading data")
	err := r.scanner.Err()
	if err != nil {
		return errors.Wrap(err, "error reading file-lines")
	}
	return nil
}
