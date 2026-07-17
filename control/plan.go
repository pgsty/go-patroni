package control

import (
	"errors"
	"fmt"

	"github.com/pgsty/go-patroni/model"
)

type Risk string

const (
	RiskRead          Risk = "READ"
	RiskAdminWrite    Risk = "ADMIN_WRITE"
	RiskConfiguration Risk = "CONFIGURATION"
	RiskAvailability  Risk = "AVAILABILITY"
	RiskDestructive   Risk = "DESTRUCTIVE"
	RiskSQL           Risk = "SQL"
)

func (r Risk) valid() bool {
	switch r {
	case RiskRead, RiskAdminWrite, RiskConfiguration, RiskAvailability, RiskDestructive, RiskSQL:
		return true
	default:
		return false
	}
}

type RetrySafety string

const (
	ReadOnly        RetrySafety = "READ_ONLY"
	SafeBeforeSend  RetrySafety = "SAFE_BEFORE_SEND"
	UnsafeAfterSend RetrySafety = "UNSAFE_AFTER_SEND"
)

func (r RetrySafety) valid() bool {
	return r == ReadOnly || r == SafeBeforeSend || r == UnsafeAfterSend
}

type Precondition struct {
	Field    string         `json:"field" yaml:"field"`
	Expected string         `json:"expected" yaml:"expected"`
	Source   EvidenceSource `json:"source" yaml:"source"`
}

// Plan contains only display-safe intent and preconditions. Transport payloads
// and credentials are deliberately excluded.
type Plan struct {
	OperationID   string         `json:"operationId" yaml:"operationId"`
	Operation     string         `json:"operation" yaml:"operation"`
	Target        model.Target   `json:"target" yaml:"target"`
	Targets       []model.Target `json:"targets,omitempty" yaml:"targets,omitempty"`
	Path          Path           `json:"path" yaml:"path"`
	Risk          Risk           `json:"risk" yaml:"risk"`
	RetrySafety   RetrySafety    `json:"retrySafety" yaml:"retrySafety"`
	Summary       string         `json:"summary" yaml:"summary"`
	Preconditions []Precondition `json:"preconditions" yaml:"preconditions"`
}

func (p Plan) Validate() error {
	if p.OperationID == "" || p.Operation == "" || p.Summary == "" {
		return errors.New("plan operation ID, operation, and summary are required")
	}
	if !p.Path.valid() {
		return fmt.Errorf("plan path %q is invalid", p.Path)
	}
	if !p.Risk.valid() {
		return fmt.Errorf("plan risk %q is invalid", p.Risk)
	}
	if !p.RetrySafety.valid() {
		return fmt.Errorf("plan retry safety %q is invalid", p.RetrySafety)
	}
	requireMember := p.Operation == "restart" || p.Operation == "reload"
	if err := p.Target.Validate(true); err != nil {
		return fmt.Errorf("plan target: %w", err)
	}
	if requireMember && p.Target.Member == "" && len(p.Targets) == 0 {
		return fmt.Errorf("plan operation %s requires a member target", p.Operation)
	}
	seenTargets := make(map[string]struct{}, len(p.Targets))
	for _, target := range p.Targets {
		target = target.Normalize()
		if err := target.Validate(true); err != nil {
			return fmt.Errorf("plan member target: %w", err)
		}
		if target.Member == "" {
			return errors.New("plan member target requires a member")
		}
		planTarget := p.Target.Normalize()
		crossGroupBatch := (p.Operation == "reload" || p.Operation == "restart" || p.Operation == "reinit" || p.Operation == "flush" ||
			p.Operation == "demote-cluster" || p.Operation == "promote-cluster") && planTarget.Group == nil && target.Group != nil &&
			target.Context == planTarget.Context && target.Namespace == planTarget.Namespace && target.Scope == planTarget.Scope
		if target.ClusterID() != planTarget.ClusterID() && !crossGroupBatch {
			return errors.New("plan member target belongs to another cluster")
		}
		if _, exists := seenTargets[target.MemberID()]; exists {
			return errors.New("plan member targets contain a duplicate")
		}
		seenTargets[target.MemberID()] = struct{}{}
	}
	for _, precondition := range p.Preconditions {
		if precondition.Field == "" || precondition.Source == "" {
			return errors.New("plan precondition field and source are required")
		}
	}
	return nil
}
