package control_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestUnknownResultGolden(t *testing.T) {
	target := (model.Target{Context: "default", Namespace: "/service", Scope: "alpha", Member: "node-1"}).Normalize()
	evidence := control.Evidence{
		Source: control.EvidencePatroni, ObservedAt: time.Date(2026, 7, 13, 8, 0, 0, 123, time.UTC),
		Summary: "request body sent; response unavailable", Path: "/restart", SendState: control.SendMaybeSent,
	}
	result := control.Result[map[string]any]{
		OperationID: "op-fixture", Outcome: control.Unknown, Target: target, Path: control.PathREST,
		Data: map[string]any{}, Evidence: []control.Evidence{evidence},
		Error: control.NewError(control.CategoryUnknown, "restart", target, false,
			"restart outcome is unknown; verify the member before retrying", nil, evidence),
	}
	actual, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	_, current, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(current), "testdata", "unknown-result.golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (use UPDATE_GOLDEN=1 for an intentional update): %v", err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("UNKNOWN result differs from golden; use UPDATE_GOLDEN=1 only after contract review")
	}
}
