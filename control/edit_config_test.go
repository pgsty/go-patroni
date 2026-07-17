package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

const configSecretMarker = "__BOAR_TEST_ONLY_DCS_CONFIG_PASSWORD__"

type configWriteCall struct {
	Target           model.Target
	Value            []byte
	ExpectedRevision *int64
}

type fakeConfigCAS struct {
	result dcs.WriteResult
	err    error
	calls  []configWriteCall
	hook   func(configWriteCall)
}

func (store *fakeConfigCAS) CompareAndSwapConfig(ctx context.Context, target model.Target, value []byte, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	call := configWriteCall{Target: target, Value: append([]byte(nil), value...)}
	if expected != nil {
		revision := *expected
		call.ExpectedRevision = &revision
	}
	store.calls = append(store.calls, call)
	if store.hook != nil {
		store.hook(call)
	}
	return store.result, store.err
}

func configFixtureSnapshot(t *testing.T, revision int64, configuration map[string]any) dcs.Snapshot {
	t.Helper()
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: "alpha"}).Normalize()
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	return dcs.BuildSnapshot(target, "/service/alpha", revision, []dcs.Entry{
		{RelativePath: "initialize", ModRevision: 2, Value: []byte("741852963")},
		{RelativePath: "config", ModRevision: revision, Value: encoded},
	})
}

func configFixtureWithoutConfig() dcs.Snapshot {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: "alpha"}).Normalize()
	return dcs.BuildSnapshot(target, "/service/alpha", 7, []dcs.Entry{{RelativePath: "initialize", ModRevision: 2, Value: []byte("741852963")}})
}

func newConfigService(t *testing.T, reader *fakeSnapshotReader, writer *fakeConfigCAS) *Service {
	t.Helper()
	service, err := NewService(ServiceOptions{
		Snapshots: reader,
		Config:    writer,
		Wait:      func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		Clock:     func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			return "edit-config-operation"
		},
		VerificationAttempts: 2,
		ProductVersion:       "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func baseDynamicConfig() map[string]any {
	return map[string]any{
		"ttl":       json.Number("30"),
		"remove_me": "old",
		"list":      []any{"old"},
		"temporary": map[string]any{"inner": "remove"},
		"postgresql": map[string]any{
			"parameters":    map[string]any{"work_mem": "4MB", "old": "remove"},
			"use_pg_rewind": true,
		},
	}
}

func editConfigRequest() EditConfigRequest {
	return EditConfigRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"},
		Apply: map[string]any{
			"ttl":       "30",
			"remove_me": nil,
			"list":      []any{"new"},
			"postgresql": map[string]any{
				"parameters":    map[string]any{"work_mem": "5MB"},
				"use_pg_rewind": nil,
			},
			"standby_cluster": map[string]any{"primary_conninfo": "password=" + configSecretMarker},
		},
		Settings: []ConfigSetting{
			{Path: "postgresql.parameters.work_mem.sub", Value: "x"},
			{Path: "postgresql.parameters.old", Value: nil},
			{Path: "temporary.inner", Value: nil},
			{Path: "a.b", Value: "c"},
		},
		Citus: true,
	}
}

func TestPreviewEditConfigMatchesPatroniPatchSetAndReplaceSemantics(t *testing.T) {
	request := editConfigRequest()
	preview, err := PreviewEditConfig(baseDynamicConfig(), request)
	if err != nil {
		t.Fatal(err)
	}
	parameters := preview.After["postgresql"].(map[string]any)["parameters"].(map[string]any)
	if _, exists := preview.After["remove_me"]; exists || preview.After["ttl"] != json.Number("30") ||
		!reflect.DeepEqual(preview.After["list"], []any{"new"}) || parameters["work_mem"] != "5MB" ||
		parameters["work_mem.sub"] != "x" || parameters["old"] != nil || preview.After["temporary"] != nil ||
		preview.After["a"].(map[string]any)["b"] != "c" {
		t.Fatalf("source-compatible patch/set projection mismatch: %#v", preview.After)
	}
	if preview.Noop || !sort.StringsAreSorted(preview.ChangedPaths) || !containsString(preview.ChangedPaths, "postgresql.parameters.work_mem.sub") {
		t.Fatalf("preview change paths mismatch: %#v", preview)
	}

	empty := map[string]any{}
	replaced, err := PreviewEditConfig(baseDynamicConfig(), EditConfigRequest{
		Target:      request.Target,
		Replacement: empty,
		Apply:       map[string]any{"ttl": 31},
		Settings:    []ConfigSetting{{Path: "postgresql.parameters.work_mem", Value: "8MB"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replaced.After) != 2 || replaced.After["ttl"] != 31 || replaced.After["remove_me"] != nil {
		t.Fatalf("replace -> apply -> set ordering mismatch: %#v", replaced.After)
	}
}

func TestPrepareEditConfigFreezesSecretSafeCASPlan(t *testing.T) {
	initial := configFixtureSnapshot(t, 30, baseDynamicConfig())
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
	service := newConfigService(t, reader, &fakeConfigCAS{})
	request := editConfigRequest()

	prepared := service.PrepareEditConfig(context.Background(), request)
	if prepared.Outcome != Succeeded || prepared.Data.Operation != "edit-config" || prepared.Data.Path != PathDCS ||
		prepared.Data.Risk != RiskConfiguration || prepared.Data.RetrySafety != UnsafeAfterSend {
		t.Fatalf("edit-config plan contract mismatch: %#v", prepared)
	}
	if revision, ok := expectedPrecondition(prepared.Data, "config.modRevision"); !ok || revision != "30" {
		t.Fatalf("config CAS revision missing: %#v", prepared.Data.Preconditions)
	}
	for _, rendered := range []string{request.String(), request.GoString(), fmt.Sprintf("%#v", prepared.Data)} {
		if strings.Contains(rendered, configSecretMarker) {
			t.Fatalf("edit-config secret leaked through default formatting")
		}
	}
	encodedRequest, _ := json.Marshal(request)
	encodedPlan, _ := json.Marshal(prepared.Data)
	if strings.Contains(string(encodedRequest), configSecretMarker) || strings.Contains(string(encodedPlan), configSecretMarker) {
		t.Fatal("edit-config secret leaked through JSON")
	}

	missing := newConfigService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": configFixtureWithoutConfig()}}, &fakeConfigCAS{})
	missingResult := missing.PrepareEditConfig(context.Background(), request)
	if missingResult.Outcome != Failed || missingResult.Error == nil || missingResult.Error.Category != CategoryNotFound {
		t.Fatalf("missing config key mismatch: %#v", missingResult)
	}
}

func TestExecuteEditConfigClassifiesCASAndAuthoritativeReadback(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	request := EditConfigRequest{Target: target, Apply: map[string]any{"ttl": 31}}
	initial := configFixtureSnapshot(t, 30, map[string]any{"ttl": 30, "loop_wait": 10})
	desired := configFixtureSnapshot(t, 31, map[string]any{"ttl": 31, "loop_wait": 10})

	t.Run("applied CAS requires matching readback", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, desired}}
		writer := &fakeConfigCAS{result: dcs.WriteResult{Applied: true, Revision: 31}}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Succeeded || result.Data.DCSSendState != SendAccepted || result.Data.Verification != VerifiedSucceeded ||
			len(writer.calls) != 1 || writer.calls[0].ExpectedRevision == nil || *writer.calls[0].ExpectedRevision != 30 ||
			string(writer.calls[0].Value) != `{"loop_wait":10,"ttl":31}` {
			t.Fatalf("applied config CAS mismatch: result=%#v calls=%#v", result, writer.calls)
		}
	})

	t.Run("definite CAS conflict is failed", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		writer := &fakeConfigCAS{err: &dcs.ConflictError{Key: "/service/alpha/config", ExpectedRevision: 30, ObservedRevision: 31}}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(writer.calls) != 1 {
			t.Fatalf("config CAS conflict mismatch: %#v", result)
		}
	})

	t.Run("maybe-sent is succeeded only from desired readback", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, desired}}
		writer := &fakeConfigCAS{err: dcs.NewWriteError(dcs.ErrorTransport, "config-cas", "/service/alpha/config", dcs.DeliveryMaybeSent, errors.New("connection closed"))}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Succeeded || result.Data.DCSSendState != SendMaybeSent || result.Data.Verification != VerifiedSucceeded {
			t.Fatalf("readback-resolved config CAS mismatch: %#v", result)
		}
	})

	t.Run("unresolved maybe-sent remains unknown and is not retried", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		writer := &fakeConfigCAS{err: dcs.NewWriteError(dcs.ErrorTransport, "config-cas", "/service/alpha/config", dcs.DeliveryMaybeSent, errors.New("connection closed"))}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown || len(writer.calls) != 1 {
			t.Fatalf("unresolved config CAS mismatch: %#v calls=%d", result, len(writer.calls))
		}
	})

	t.Run("definitely not-sent is failed", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		writer := &fakeConfigCAS{err: dcs.NewWriteError(dcs.ErrorTransport, "config-cas", "/service/alpha/config", dcs.DeliveryNotSent, errors.New("dial failed"))}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnreachable || result.Data.DCSSendState != SendNotSent || len(writer.calls) != 1 {
			t.Fatalf("not-sent config CAS mismatch: %#v", result)
		}
	})

	t.Run("accepted write without matching readback is unknown", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		writer := &fakeConfigCAS{result: dcs.WriteResult{Applied: true, Revision: 31}}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Unknown || result.Data.DCSSendState != SendAccepted || result.Data.Verification != Unverified || len(writer.calls) != 1 {
			t.Fatalf("accepted-unverified config CAS mismatch: %#v", result)
		}
	})
}

func TestExecuteEditConfigRejectsConcurrencyTamperingAndCancellation(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	request := EditConfigRequest{Target: target, Apply: map[string]any{"ttl": 31}}
	initial := configFixtureSnapshot(t, 30, map[string]any{"ttl": 30})
	desired := configFixtureSnapshot(t, 31, map[string]any{"ttl": 31})
	different := configFixtureSnapshot(t, 31, map[string]any{"ttl": 30, "loop_wait": 5})

	t.Run("different concurrent config aborts before write", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, different}}
		writer := &fakeConfigCAS{}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(writer.calls) != 0 {
			t.Fatalf("pre-write config concurrency mismatch: %#v calls=%d", result, len(writer.calls))
		}
	})

	t.Run("concurrent desired state is verified no-op", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, desired}}
		writer := &fakeConfigCAS{}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(context.Background(), request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Succeeded || !result.Data.Noop || result.Data.Verification != VerifiedSucceeded || len(writer.calls) != 0 {
			t.Fatalf("concurrent desired config mismatch: %#v", result)
		}
	})

	t.Run("request and plan tampering are rejected", func(t *testing.T) {
		for _, mutate := range []func(*EditConfigRequest, *Plan){
			func(request *EditConfigRequest, _ *Plan) { request.Apply["ttl"] = 99 },
			func(_ *EditConfigRequest, plan *Plan) {
				for index := range plan.Preconditions {
					if plan.Preconditions[index].Field == "config.desired" {
						plan.Preconditions[index].Expected = "tampered"
					}
				}
			},
			func(_ *EditConfigRequest, plan *Plan) {
				for index := range plan.Preconditions {
					if plan.Preconditions[index].Field == "config.changedPaths" {
						plan.Preconditions[index].Expected = `["hidden"]`
					}
				}
			},
		} {
			reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
			writer := &fakeConfigCAS{}
			service := newConfigService(t, reader, writer)
			candidate := EditConfigRequest{Target: request.Target, Apply: map[string]any{"ttl": 31}}
			prepared := service.PrepareEditConfig(context.Background(), candidate)
			plan := prepared.Data
			mutate(&candidate, &plan)
			result := service.ExecuteEditConfig(context.Background(), candidate, plan)
			if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(writer.calls) != 0 {
				t.Fatalf("edit-config tampering mismatch: %#v", result)
			}
		}
	})

	t.Run("cancellation before CAS is definitely not sent", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		writer := &fakeConfigCAS{}
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := service.ExecuteEditConfig(ctx, request, prepared.Data)
		if result.Outcome != Failed || !errors.Is(result.Error, context.Canceled) || len(writer.calls) != 0 {
			t.Fatalf("pre-CAS cancellation mismatch: %#v", result)
		}
	})

	t.Run("cancellation after maybe-sent preserves unknown and stops", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		writer := &fakeConfigCAS{err: dcs.NewWriteError(dcs.ErrorTransport, "config-cas", "/service/alpha/config", dcs.DeliveryMaybeSent, errors.New("response lost"))}
		writer.hook = func(configWriteCall) { cancel() }
		service := newConfigService(t, reader, writer)
		prepared := service.PrepareEditConfig(context.Background(), request)
		result := service.ExecuteEditConfig(ctx, request, prepared.Data)
		requireValidEditConfigResult(t, result)
		if result.Outcome != Unknown || !errors.Is(result.Error, context.Canceled) || len(writer.calls) != 1 {
			t.Fatalf("post-send cancellation mismatch: %#v", result)
		}
	})
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func requireValidEditConfigResult(t *testing.T, result Result[ConfigEditData]) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid edit-config result: %v", err)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("invalid edit-config data: %v", err)
	}
}
