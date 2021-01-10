package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gopkg.in/validator.v2"
)

// CmdAction represents a Command-action.
type CmdAction string

// String returns string-representation of a CmdAction.
func (ca CmdAction) String() string {
	return string(ca)
}

// Domain commands
const (
	CreateTxn    CmdAction = "CreateTxn"
	ProcessTxn   CmdAction = "ProcessTxn"
	CreateReport CmdAction = "CreateReport"
	WriteData    CmdAction = "WriteData"
)

// Cmd represents a Command.
// Use #NewCmd to create new instance.
type Cmd struct {
	id             string
	correlationKey string

	time   time.Time
	action CmdAction
	data   []byte
}

// CmdCfg is config for Cmd.
type CmdCfg struct {
	CorrelationKey string

	Time   time.Time
	Action CmdAction `validate:"nonzero"`
	Data   interface{}
}

// NewCmd validates provided
// config and creates a new Cmd.
// Uses current UTC-time if time is not set.
func NewCmd(cfg *CmdCfg) (Cmd, error) {
	err := validator.Validate(cfg)
	if err != nil {
		return Cmd{}, errors.Wrap(err, "error validating config")
	}
	if cfg.Time.IsZero() {
		cfg.Time = time.Now().UTC()
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return Cmd{}, errors.Wrap(err, "error generating event-id")
	}

	var dataBytes []byte
	switch v := cfg.Data.(type) {
	case []byte:
		dataBytes = v
	default:
		dataBytes, err = json.Marshal(cfg.Data)
		if err != nil {
			return Cmd{}, errors.Wrap(err, "error json-marshalling data")
		}
	}

	return Cmd{
		id:             id.String(),
		correlationKey: cfg.CorrelationKey,

		time:   cfg.Time,
		action: cfg.Action,
		data:   dataBytes,
	}, nil
}

// ID returns Command-ID.
func (c Cmd) ID() string {
	return c.id
}

// CorrelationKey returns Command-CorrelationKey.
func (c Cmd) CorrelationKey() string {
	return c.correlationKey
}

// Time return Command-Time.
func (c Cmd) Time() time.Time {
	return c.time
}

// Action return Command-Action.
func (c Cmd) Action() CmdAction {
	return c.action
}

// Data return Command-Data.
func (c Cmd) Data() []byte {
	return c.data
}
