package control_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestErrorCategoryExitContract(t *testing.T) {
	tests := []struct {
		category control.Category
		exit     int
	}{
		{control.CategoryFailed, 1},
		{control.CategoryUsage, 2},
		{control.CategoryConfig, 2},
		{control.CategoryUnsupported, 3},
		{control.CategoryAuth, 4},
		{control.CategoryTLS, 4},
		{control.CategoryNotFound, 5},
		{control.CategoryConflict, 6},
		{control.CategoryUnreachable, 7},
		{control.CategoryUnknown, 8},
		{control.CategoryInternal, 9},
	}
	for _, test := range tests {
		t.Run(string(test.category), func(t *testing.T) {
			if got := control.ExitCode(test.category); got != test.exit {
				t.Fatalf("ExitCode(%s)=%d want %d", test.category, got, test.exit)
			}
		})
	}
	if got := control.ExitCode("NOT_A_CATEGORY"); got != 9 {
		t.Fatalf("unknown category must fail as internal, got %d", got)
	}
}

func TestErrorWrapsInternalCauseButDoesNotMarshalIt(t *testing.T) {
	cause := errors.New("transport contained __SENSITIVE_CAUSE__")
	target := (model.Target{Scope: "alpha", Member: "node-1"}).Normalize()
	err := control.NewError(
		control.CategoryUnknown,
		"restart",
		target,
		false,
		"restart may have been accepted; verify member state before retrying",
		cause,
		control.Evidence{Source: control.EvidencePatroni, ObservedAt: time.Unix(1, 0).UTC(), Summary: "response lost", SendState: control.SendMaybeSent},
	)
	if !errors.Is(err, cause) {
		t.Fatal("typed error does not unwrap its internal cause")
	}
	if err.Error() != "restart may have been accepted; verify member state before retrying" {
		t.Fatalf("unsafe or unstable Error string: %q", err.Error())
	}
	data, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(data), "__SENSITIVE_CAUSE__") || strings.Contains(string(data), "transport contained") {
		t.Fatalf("internal cause leaked into public JSON: %s", data)
	}
	if !strings.Contains(string(data), `"category":"UNKNOWN"`) || !strings.Contains(string(data), `"operation":"restart"`) {
		t.Fatalf("public error contract missing fields: %s", data)
	}
}

func TestErrorValidation(t *testing.T) {
	validTarget := (model.Target{Scope: "alpha"}).Normalize()
	for name, err := range map[string]*control.Error{
		"category":  control.NewError("BAD", "list", validTarget, false, "message", nil),
		"operation": control.NewError(control.CategoryConfig, "", validTarget, false, "message", nil),
		"message":   control.NewError(control.CategoryConfig, "load", validTarget, false, "", nil),
	} {
		t.Run(name, func(t *testing.T) {
			if validationErr := err.Validate(); validationErr == nil {
				t.Fatal("invalid control error accepted")
			}
		})
	}
}
