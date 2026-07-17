package control

import (
	"errors"
	"fmt"
	"time"

	"github.com/pgsty/go-patroni/model"
)

type Outcome string

const (
	Succeeded Outcome = "SUCCEEDED"
	Failed    Outcome = "FAILED"
	Unknown   Outcome = "UNKNOWN"
)

type SendState string

const (
	SendNotSent   SendState = "NOT_SENT"
	SendMaybeSent SendState = "MAYBE_SENT"
	SendAccepted  SendState = "ACCEPTED"
)

type Verification string

const (
	Unverified        Verification = "UNVERIFIED"
	VerifiedSucceeded Verification = "VERIFIED_SUCCEEDED"
	VerifiedFailed    Verification = "VERIFIED_FAILED"
)

type EvidenceSource string

const (
	EvidenceLocal    EvidenceSource = "LOCAL"
	EvidenceDCS      EvidenceSource = "DCS"
	EvidencePatroni  EvidenceSource = "PATRONI"
	EvidencePostgres EvidenceSource = "POSTGRES"
	EvidenceControl  EvidenceSource = "CONTROL"
)

type Evidence struct {
	Source     EvidenceSource `json:"source" yaml:"source"`
	ObservedAt time.Time      `json:"observedAt" yaml:"observedAt"`
	Summary    string         `json:"summary" yaml:"summary"`
	Revision   string         `json:"revision,omitempty" yaml:"revision,omitempty"`
	Path       string         `json:"path,omitempty" yaml:"path,omitempty"`
	SendState  SendState      `json:"sendState,omitempty" yaml:"sendState,omitempty"`
}

func (source EvidenceSource) valid() bool {
	switch source {
	case EvidenceLocal, EvidenceDCS, EvidencePatroni, EvidencePostgres, EvidenceControl:
		return true
	default:
		return false
	}
}

func (state SendState) validOrEmpty() bool {
	return state == "" || state == SendNotSent || state == SendMaybeSent || state == SendAccepted
}

func (e Evidence) Validate() error {
	if !e.Source.valid() {
		return fmt.Errorf("evidence source %q is invalid", e.Source)
	}
	if e.ObservedAt.IsZero() {
		return errors.New("evidence observed time is required")
	}
	if e.Summary == "" {
		return errors.New("evidence summary is required")
	}
	if !e.SendState.validOrEmpty() {
		return fmt.Errorf("evidence send state %q is invalid", e.SendState)
	}
	return nil
}

type Path string

const (
	PathLocal     Path = "local"
	PathDCS       Path = "dcs"
	PathREST      Path = "rest"
	PathPostgres  Path = "postgres"
	PathRESTToDCS Path = "rest->dcs"
)

func (p Path) valid() bool {
	switch p {
	case PathLocal, PathDCS, PathREST, PathPostgres, PathRESTToDCS:
		return true
	default:
		return false
	}
}

// Result is the adapter-neutral operation result returned by control.Service.
type Result[T any] struct {
	OperationID string       `json:"operationId" yaml:"operationId"`
	Outcome     Outcome      `json:"outcome" yaml:"outcome"`
	Target      model.Target `json:"target" yaml:"target"`
	Path        Path         `json:"path" yaml:"path"`
	Data        T            `json:"data" yaml:"data"`
	Evidence    []Evidence   `json:"evidence" yaml:"evidence"`
	Error       *Error       `json:"error,omitempty" yaml:"error,omitempty"`
}

func (r Result[T]) Validate() error {
	if r.OperationID == "" {
		return errors.New("result operation ID is required")
	}
	if err := r.Target.Validate(r.Target.Scope != ""); err != nil {
		return fmt.Errorf("result target: %w", err)
	}
	if !r.Path.valid() {
		return fmt.Errorf("result path %q is invalid", r.Path)
	}
	if len(r.Evidence) == 0 {
		return errors.New("result requires authoritative evidence")
	}
	for _, evidence := range r.Evidence {
		if err := evidence.Validate(); err != nil {
			return fmt.Errorf("result evidence: %w", err)
		}
	}
	switch r.Outcome {
	case Succeeded:
		if r.Error != nil {
			return errors.New("succeeded result must not contain an error")
		}
	case Failed:
		if r.Error == nil {
			return errors.New("failed result requires an error")
		}
		if r.Error.Category == CategoryUnknown {
			return errors.New("failed result cannot contain UNKNOWN error category")
		}
	case Unknown:
		if r.Error == nil || r.Error.Category != CategoryUnknown {
			return errors.New("unknown result requires UNKNOWN error category")
		}
	default:
		return fmt.Errorf("result outcome %q is invalid", r.Outcome)
	}
	if r.Error != nil {
		if err := r.Error.Validate(); err != nil {
			return fmt.Errorf("result error: %w", err)
		}
	}
	return nil
}

// ClassifyWrite applies the SDK's no-false-failure rule. Verification is
// authoritative; without it, any maybe-sent or accepted write is UNKNOWN.
func ClassifyWrite(send SendState, verification Verification, operationError *Error) Outcome {
	switch verification {
	case VerifiedSucceeded:
		return Succeeded
	case VerifiedFailed:
		return Failed
	}
	if send == SendMaybeSent || send == SendAccepted {
		return Unknown
	}
	if send == SendNotSent && operationError != nil {
		return Failed
	}
	return Unknown
}
