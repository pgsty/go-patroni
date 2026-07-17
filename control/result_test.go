package control_test

import (
	"testing"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestWriteOutcomeClassification(t *testing.T) {
	definite := control.NewError(control.CategoryUnreachable, "restart", model.Target{Scope: "alpha"}.Normalize(), true, "request was not sent", nil)
	ambiguous := control.NewError(control.CategoryUnknown, "restart", model.Target{Scope: "alpha"}.Normalize(), false, "request may have been sent", nil)
	tests := []struct {
		name         string
		send         control.SendState
		verification control.Verification
		err          *control.Error
		want         control.Outcome
	}{
		{"verified success overrides transport loss", control.SendMaybeSent, control.VerifiedSucceeded, ambiguous, control.Succeeded},
		{"verified failure", control.SendAccepted, control.VerifiedFailed, ambiguous, control.Failed},
		{"not sent definite error", control.SendNotSent, control.Unverified, definite, control.Failed},
		{"maybe sent is unknown", control.SendMaybeSent, control.Unverified, ambiguous, control.Unknown},
		{"accepted is unknown", control.SendAccepted, control.Unverified, ambiguous, control.Unknown},
		{"missing evidence is unknown", control.SendNotSent, control.Unverified, nil, control.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := control.ClassifyWrite(test.send, test.verification, test.err); got != test.want {
				t.Fatalf("ClassifyWrite()=%s want %s", got, test.want)
			}
		})
	}
}

func TestResultValidationRequiresConsistentOutcome(t *testing.T) {
	target := model.Target{Scope: "alpha"}.Normalize()
	evidence := []control.Evidence{{Source: control.EvidenceDCS, ObservedAt: time.Unix(2, 0).UTC(), Summary: "fresh snapshot"}}
	valid := control.Result[string]{
		OperationID: "op-1", Outcome: control.Succeeded, Target: target, Path: control.PathDCS,
		Data: "ok", Evidence: evidence,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	unknownWithoutError := valid
	unknownWithoutError.Outcome = control.Unknown
	if err := unknownWithoutError.Validate(); err == nil {
		t.Fatal("UNKNOWN result without UNKNOWN error accepted")
	}
	unknownWithoutError.Error = control.NewError(control.CategoryUnknown, "restart", target, false, "verify before retry", nil)
	if err := unknownWithoutError.Validate(); err != nil {
		t.Fatalf("consistent UNKNOWN result rejected: %v", err)
	}
	failedWithUnknown := unknownWithoutError
	failedWithUnknown.Outcome = control.Failed
	if err := failedWithUnknown.Validate(); err == nil {
		t.Fatal("FAILED result with UNKNOWN error accepted")
	}
	badEvidence := valid
	badEvidence.Evidence = []control.Evidence{{Source: control.EvidenceDCS, Summary: "missing timestamp"}}
	if err := badEvidence.Validate(); err == nil {
		t.Fatal("result with incomplete evidence accepted")
	}
}

func TestPlanValidation(t *testing.T) {
	plan := control.Plan{
		OperationID: "op-1",
		Operation:   "restart",
		Target:      model.Target{Scope: "alpha", Member: "node-1"}.Normalize(),
		Path:        control.PathREST,
		Risk:        control.RiskAvailability,
		RetrySafety: control.UnsafeAfterSend,
		Summary:     "restart one selected member",
		Preconditions: []control.Precondition{
			{Field: "member.role", Expected: "replica", Source: control.EvidenceDCS},
		},
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
	plan.Target.Member = ""
	if err := plan.Validate(); err == nil {
		t.Fatal("member operation plan without complete target accepted")
	}
}
