package writer

import (
	"bufio"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gopkg.in/validator.v2"

	"github.com/Jaskaranbir/es-bank-account/eventutil"
	"github.com/Jaskaranbir/es-bank-account/logger"
	"github.com/Jaskaranbir/es-bank-account/model"
)

// writer writes data to specific
// buffered-writer interface.
// Use #newWriter to create new instance.
type writer struct {
	log        logger.Logger
	buffWriter *bufio.Writer

	eventRepo   eventutil.EventRepo
	dataWritten model.EventAction
}

// AggregateCfg defines config for Writer-aggregate.
type AggregateCfg struct {
	Log    logger.Logger `validate:"nonnil"`
	Writer io.Writer     `validate:"nonnil"`

	EventRepo   eventutil.EventRepo `validate:"nonnil"`
	DataWritten model.EventAction   `validate:"nonzero"`
}

func newWriter(cfg *AggregateCfg) (*writer, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return nil, err
	}

	// Check if passed writer is bufio-writer,
	// else create bufio-writer
	buffWriter, castSuccess := cfg.Writer.(*bufio.Writer)
	if !castSuccess {
		buffWriter = bufio.NewWriter(cfg.Writer)
	}

	return &writer{
		log:        cfg.Log,
		buffWriter: buffWriter,

		eventRepo:   cfg.EventRepo,
		dataWritten: cfg.DataWritten,
	}, nil
}

func (w *writer) handleWriteDataCmd(cmd model.Cmd) error {
	if cmd.Data() == nil {
		w.log.Debugf("[CMD: %s] ignored command with nil data", cmd.ID())
		return nil
	}

	err := w.write(cmd.ID(), string(cmd.Data()))
	return errors.Wrap(err, "error writing data")
}

func (w *writer) write(cmdID string, data string) error {
	logPrefix := fmt.Sprintf("[CMD: %s]:", cmdID)

	// Write data to buffered-writer
	w.log.Tracef("%s Writing result to output-file", logPrefix)
	_, err := fmt.Fprintln(w.buffWriter, data)
	if err != nil {
		return errors.Wrap(err, "error writing to output-file")
	}
	err = w.buffWriter.Flush()
	if err != nil {
		return errors.Wrap(err, "error flushing bufferred-writer")
	}
	w.log.Tracef("%s Wrote result to output-file", logPrefix)

	id, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "error generating aggregate-id")
	}
	// Send result-event
	event, err := model.NewEvent(&model.EventCfg{
		AggregateID:    id.String(),
		CorrelationKey: cmdID,
		Action:         w.dataWritten,
		Data:           []byte(data),
	})
	if err != nil {
		return errors.Wrap(err, "error creating event")
	}
	logPrefix = fmt.Sprintf("%s [Event: %s]:", logPrefix, event.ID())

	w.log.Tracef("%s Publishing data-written event", logPrefix)
	err = w.eventRepo.InsertAndPublish(event)
	if err != nil {
		return errors.Wrap(err, "error inserting event to event-repo")
	}
	w.log.Tracef("%s Published data-written event", logPrefix)

	return nil
}
