package control

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

type fakeClusterRemover struct {
	result dcs.RemoveResult
	err    error
	calls  []model.Target
	hook   func(model.Target)
}

func (store *fakeClusterRemover) DeleteCluster(ctx context.Context, target model.Target) (dcs.RemoveResult, error) {
	if ctx == nil {
		return dcs.RemoveResult{}, errors.New("nil context")
	}
	store.calls = append(store.calls, target)
	if store.hook != nil {
		store.hook(target)
	}
	return store.result, store.err
}

func newRemoveService(t *testing.T, reader *fakeSnapshotReader, remover *fakeClusterRemover) *Service {
	t.Helper()
	service, err := NewService(ServiceOptions{
		Snapshots: reader,
		Remover:   remover,
		Wait:      func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		Clock:     func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			return "remove-operation"
		},
		VerificationAttempts: 2,
		ProductVersion:       "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func emptyRemoveSnapshot(snapshot dcs.Snapshot, revision int64) dcs.Snapshot {
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, revision, nil)
}

func removeSnapshotWithExtraKey(snapshot dcs.Snapshot, relative string, revision int64) dcs.Snapshot {
	entries := append([]dcs.Entry(nil), snapshot.Entries...)
	entries = append(entries, dcs.Entry{RelativePath: relative, ModRevision: revision, Value: []byte(`{"future":true}`)})
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, revision, entries)
}

func removeConfirmation() RemoveConfirmation {
	return RemoveConfirmation{ClusterName: "alpha", Acknowledgement: RemoveAcknowledgement, Leader: "node-a"}
}

func TestPrepareRemoveFreezesExactDestructiveConfirmation(t *testing.T) {
	initial := readFixtureSnapshot()
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
	service := newRemoveService(t, reader, &fakeClusterRemover{})
	request := RemoveRequest{Target: model.Target{Context: "lab", Scope: "alpha"}}

	prepared := service.PrepareRemove(context.Background(), request)
	if prepared.Outcome != Succeeded || prepared.Data.Operation != "remove" || prepared.Data.Path != PathDCS ||
		prepared.Data.Risk != RiskDestructive || prepared.Data.RetrySafety != UnsafeAfterSend {
		t.Fatalf("remove Plan contract mismatch: %#v", prepared)
	}
	if leader, ok := expectedPrecondition(prepared.Data, "remove.leader"); !ok || leader != "node-a" {
		t.Fatalf("remove leader confirmation missing: %#v", prepared.Data.Preconditions)
	}
	if phrase, ok := expectedPrecondition(prepared.Data, "remove.acknowledgement"); !ok || phrase != RemoveAcknowledgement {
		t.Fatalf("remove acknowledgement missing: %#v", prepared.Data.Preconditions)
	}
	keys, err := removePlanKeys(prepared.Data)
	if err != nil || len(keys) != len(initial.Entries) || !sort.StringsAreSorted(keys) {
		t.Fatalf("remove key inventory mismatch: keys=%v err=%v", keys, err)
	}

	citus := service.PrepareRemove(context.Background(), RemoveRequest{Target: request.Target, Citus: true})
	if citus.Outcome != Failed || citus.Error == nil || citus.Error.Category != CategoryUsage {
		t.Fatalf("Citus remove without group mismatch: %#v", citus)
	}
	missing := newRemoveService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": emptyRemoveSnapshot(initial, 20)}}, &fakeClusterRemover{})
	missingResult := missing.PrepareRemove(context.Background(), request)
	if missingResult.Outcome != Failed || missingResult.Error == nil || missingResult.Error.Category != CategoryNotFound {
		t.Fatalf("missing remove target mismatch: %#v", missingResult)
	}
}

func TestExecuteRemoveRequiresPatronictlConfirmations(t *testing.T) {
	initial := readFixtureSnapshot()
	request := RemoveRequest{Target: model.Target{Context: "lab", Scope: "alpha"}}
	for name, confirmation := range map[string]RemoveConfirmation{
		"cluster":         {ClusterName: "beta", Acknowledgement: RemoveAcknowledgement, Leader: "node-a"},
		"acknowledgement": {ClusterName: "alpha", Acknowledgement: "yes", Leader: "node-a"},
		"leader":          {ClusterName: "alpha", Acknowledgement: RemoveAcknowledgement, Leader: "node-b"},
	} {
		t.Run(name, func(t *testing.T) {
			reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
			remover := &fakeClusterRemover{}
			service := newRemoveService(t, reader, remover)
			prepared := service.PrepareRemove(context.Background(), request)
			result := service.ExecuteRemove(context.Background(), request, confirmation, prepared.Data)
			if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(remover.calls) != 0 {
				t.Fatalf("remove confirmation mismatch: %#v", result)
			}
		})
	}
}

func TestExecuteRemoveClassifiesDeleteAndAuthoritativeReadback(t *testing.T) {
	initial := readFixtureSnapshot()
	empty := emptyRemoveSnapshot(initial, 40)
	request := RemoveRequest{Target: model.Target{Context: "lab", Scope: "alpha"}}

	t.Run("accepted delete requires empty-prefix readback", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, empty}}
		remover := &fakeClusterRemover{result: dcs.RemoveResult{Deleted: int64(len(initial.Entries)), Revision: 40}}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Succeeded || result.Data.DCSSendState != SendAccepted || result.Data.Verification != VerifiedSucceeded ||
			result.Data.Deleted != int64(len(initial.Entries)) || len(remover.calls) != 1 {
			t.Fatalf("accepted remove mismatch: %#v", result)
		}
	})

	t.Run("definitely not-sent delete is failed", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		remover := &fakeClusterRemover{err: dcs.NewWriteError(dcs.ErrorTransport, "remove-cluster", "", dcs.DeliveryNotSent, errors.New("dial failed"))}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnreachable || result.Data.DCSSendState != SendNotSent || len(remover.calls) != 1 {
			t.Fatalf("not-sent remove mismatch: %#v", result)
		}
	})

	t.Run("maybe-sent delete resolves only from empty prefix", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, empty}}
		remover := &fakeClusterRemover{err: dcs.NewWriteError(dcs.ErrorTransport, "remove-cluster", "", dcs.DeliveryMaybeSent, errors.New("response lost"))}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Succeeded || result.Data.DCSSendState != SendMaybeSent || result.Data.Verification != VerifiedSucceeded {
			t.Fatalf("readback-resolved remove mismatch: %#v", result)
		}
	})

	t.Run("remaining prefix after maybe-sent is unknown and not retried", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, initial, initial}}
		remover := &fakeClusterRemover{err: dcs.NewWriteError(dcs.ErrorTransport, "remove-cluster", "", dcs.DeliveryMaybeSent, errors.New("response lost"))}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown || len(remover.calls) != 1 || len(result.Data.RemainingKeys) == 0 {
			t.Fatalf("unresolved remove mismatch: %#v", result)
		}
	})
}

func TestExecuteRemoveConcurrencyAndCancellationRemainSafe(t *testing.T) {
	initial := readFixtureSnapshot()
	empty := emptyRemoveSnapshot(initial, 40)
	changed := removeSnapshotWithExtraKey(initial, "future", 41)
	request := RemoveRequest{Target: model.Target{Context: "lab", Scope: "alpha"}}

	t.Run("key inventory drift aborts before delete", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, changed}}
		remover := &fakeClusterRemover{}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(remover.calls) != 0 {
			t.Fatalf("remove concurrency mismatch: %#v", result)
		}
	})

	t.Run("already absent target is verified no-op", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, empty}}
		remover := &fakeClusterRemover{}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Succeeded || !result.Data.Noop || result.Data.DCSSendState != SendNotSent || len(remover.calls) != 0 {
			t.Fatalf("concurrent remove no-op mismatch: %#v", result)
		}
	})

	t.Run("bound key inventory cannot be tampered", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		remover := &fakeClusterRemover{}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		plan := prepared.Data
		for index := range plan.Preconditions {
			if plan.Preconditions[index].Field == "remove.keys" {
				plan.Preconditions[index].Expected = `["config"]`
			}
		}
		result := service.ExecuteRemove(context.Background(), request, removeConfirmation(), plan)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(remover.calls) != 0 {
			t.Fatalf("remove Plan tampering mismatch: %#v", result)
		}
	})

	t.Run("cancellation before delete is definitely not sent", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		remover := &fakeClusterRemover{}
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := service.ExecuteRemove(ctx, request, removeConfirmation(), prepared.Data)
		if result.Outcome != Failed || !errors.Is(result.Error, context.Canceled) || len(remover.calls) != 0 {
			t.Fatalf("pre-delete cancellation mismatch: %#v", result)
		}
	})

	t.Run("cancellation after maybe-sent remains unknown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		remover := &fakeClusterRemover{err: dcs.NewWriteError(dcs.ErrorTransport, "remove-cluster", "", dcs.DeliveryMaybeSent, errors.New("response lost"))}
		remover.hook = func(model.Target) { cancel() }
		service := newRemoveService(t, reader, remover)
		prepared := service.PrepareRemove(context.Background(), request)
		result := service.ExecuteRemove(ctx, request, removeConfirmation(), prepared.Data)
		requireValidRemoveResult(t, result)
		if result.Outcome != Unknown || !errors.Is(result.Error, context.Canceled) || len(remover.calls) != 1 {
			t.Fatalf("post-delete cancellation mismatch: %#v", result)
		}
	})
}

func requireValidRemoveResult(t *testing.T, result Result[RemoveData]) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid remove result: %v", err)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("invalid remove data: %v", err)
	}
}
