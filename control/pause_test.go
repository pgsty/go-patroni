package control

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

func snapshotWithPauseState(t *testing.T, snapshot dcs.Snapshot, clusterPaused bool, members map[string]bool) dcs.Snapshot {
	t.Helper()
	configuration := make(map[string]any, len(snapshot.Cluster.Config)+1)
	for key, value := range snapshot.Cluster.Config {
		configuration[key] = value
	}
	if clusterPaused {
		configuration["pause"] = true
	} else {
		delete(configuration, "pause")
	}
	updated := snapshotWithConfig(t, snapshot, configuration)
	entries := make([]dcs.Entry, len(updated.Entries))
	copy(entries, updated.Entries)
	for index := range entries {
		if !stringsHasPrefix(entries[index].RelativePath, "members/") {
			continue
		}
		name := entries[index].RelativePath[len("members/"):]
		paused, ok := members[name]
		if !ok {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal(entries[index].Value, &value); err != nil {
			t.Fatal(err)
		}
		value["pause"] = paused
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		entries[index].Value = encoded
		entries[index].ModRevision++
	}
	return dcs.BuildSnapshot(updated.Target, updated.Prefix, updated.Revision+1, entries)
}

func stringsHasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func requireValidPauseResult(t *testing.T, result Result[PauseData]) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid pause/resume result: %v", err)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("invalid pause/resume data: %v", err)
	}
}

func TestPreparePauseResumeFreezesLeaderFirstPlan(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	paused := snapshotWithPauseState(t, readFixtureSnapshot(), true, nil)
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": paused}}
	service := newTransitionService(t, reader, &fakePatroniStatusReader{}, &fakeFailoverCAS{})

	alreadyPaused := service.PreparePause(context.Background(), PauseRequest{Target: target})
	if alreadyPaused.Outcome != Failed || alreadyPaused.Error == nil || alreadyPaused.Error.Category != CategoryConflict {
		t.Fatalf("already-paused precondition mismatch: %#v", alreadyPaused)
	}
	resume := service.PrepareResume(context.Background(), PauseRequest{Target: target, Wait: true, Citus: true})
	if resume.Outcome != Succeeded || resume.Data.Operation != "resume" || resume.Data.Path != PathREST ||
		!reflect.DeepEqual(memberNamesFromTargets(resume.Data.Targets), []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("resume leader-first plan mismatch: %#v", resume)
	}
	if desired, ok := expectedPrecondition(resume.Data, "pause.desired"); !ok || desired != "false" {
		t.Fatalf("desired-state precondition missing: %#v", resume.Data.Preconditions)
	}
	if wait, ok := expectedPrecondition(resume.Data, "request.wait"); !ok || wait != "true" {
		t.Fatalf("wait precondition missing: %#v", resume.Data.Preconditions)
	}

	reader.snapshots["alpha"] = snapshotWithPauseState(t, readFixtureSnapshot(), false, nil)
	alreadyResumed := service.PrepareResume(context.Background(), PauseRequest{Target: target})
	if alreadyResumed.Outcome != Failed || alreadyResumed.Error == nil || alreadyResumed.Error.Category != CategoryConflict {
		t.Fatalf("already-resumed precondition mismatch: %#v", alreadyResumed)
	}
}

func TestPauseResumePayloadAndTerminalResponseSemantics(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}

	t.Run("pause true", func(t *testing.T) {
		initial := snapshotWithPauseState(t, readFixtureSnapshot(), false, nil)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
			"https://node-a:8008": {StatusCode: 200},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Succeeded || !result.Data.Paused || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://node-a:8008"}) ||
			len(transport.patchConfigRequests) != 1 || transport.patchConfigRequests[0]["pause"] != true {
			t.Fatalf("pause payload/response mismatch: result=%#v calls=%v requests=%#v", result, transport.patchConfigCalls, transport.patchConfigRequests)
		}
	})

	t.Run("resume null", func(t *testing.T) {
		initial := snapshotWithPauseState(t, readFixtureSnapshot(), true, nil)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
			"https://node-a:8008": {StatusCode: 200},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PrepareResume(context.Background(), request)
		result := service.ExecuteResume(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		value, present := transport.patchConfigRequests[0]["pause"]
		if result.Outcome != Succeeded || result.Data.Paused || !present || value != nil {
			t.Fatalf("resume payload/response mismatch: result=%#v request=%#v", result, transport.patchConfigRequests)
		}
	})

	t.Run("first HTTP response is terminal", func(t *testing.T) {
		initial := snapshotWithPauseState(t, readFixtureSnapshot(), false, nil)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
			"https://node-a:8008": {StatusCode: 409}, "https://node-b:8008": {StatusCode: 200},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Failed || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://node-a:8008"}) || result.Data.Results[0].HTTPStatus != 409 {
			t.Fatalf("terminal HTTP response mismatch: result=%#v calls=%v", result, transport.patchConfigCalls)
		}
	})
}

func TestPauseResumeProbingAndAmbiguousEvidence(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	initial := snapshotWithPauseState(t, readFixtureSnapshot(), false, nil)

	t.Run("not-sent exception advances to next member", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		transport := &fakePatroniStatusReader{
			patchConfigErrors:    map[string]error{"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryNotSent}},
			patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-b:8008": {StatusCode: 200}},
		}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Succeeded || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://node-a:8008", "https://node-b:8008"}) {
			t.Fatalf("member probing mismatch: result=%#v calls=%v", result, transport.patchConfigCalls)
		}
	})

	t.Run("maybe-sent plus terminal rejection resolves from DCS", func(t *testing.T) {
		desired := snapshotWithPauseState(t, initial, true, nil)
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, desired}}
		transport := &fakePatroniStatusReader{
			patchConfigErrors:    map[string]error{"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryMaybeSent}},
			patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-b:8008": {StatusCode: 500}},
		}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Succeeded || result.Data.RESTSendState != SendMaybeSent || result.Data.Verification != VerifiedSucceeded {
			t.Fatalf("DCS-resolved ambiguous pause mismatch: %#v", result)
		}
	})

	t.Run("all definite misses fail", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		notSent := &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryNotSent}
		transport := &fakePatroniStatusReader{patchConfigErrors: map[string]error{
			"https://node-a:8008": notSent, "https://node-b:8008": notSent, "https://node-c:8008": notSent,
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnreachable {
			t.Fatalf("all-not-sent pause mismatch: %#v", result)
		}
	})
}

func TestPauseResumeWaitConvergenceAndConcurrency(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	initial := snapshotWithPauseState(t, readFixtureSnapshot(), false, nil)

	t.Run("wait observes all original REST members", func(t *testing.T) {
		converged := snapshotWithPauseState(t, initial, true, map[string]bool{"node-a": true, "node-b": true, "node-c": true})
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, converged}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": {StatusCode: 200}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target, Wait: true}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Succeeded || len(result.Data.PendingMembers) != 0 || result.Data.DCSRevision != converged.Revision {
			t.Fatalf("pause convergence mismatch: %#v", result)
		}
	})

	t.Run("accepted write without full convergence is unknown", func(t *testing.T) {
		pending := snapshotWithPauseState(t, initial, true, map[string]bool{"node-a": true, "node-b": false, "node-c": true})
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, initial, pending, pending}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": {StatusCode: 200}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target, Wait: true}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Unknown || !reflect.DeepEqual(result.Data.PendingMembers, []string{"node-b"}) {
			t.Fatalf("unconverged pause classification mismatch: %#v", result)
		}
	})

	t.Run("desired state reached before execute is a no-op", func(t *testing.T) {
		desired := snapshotWithPauseState(t, initial, true, nil)
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{initial, desired}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": {StatusCode: 200}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(context.Background(), request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Succeeded || !result.Data.Noop || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("concurrent desired-state no-op mismatch: %#v calls=%v", result, transport.patchConfigCalls)
		}
	})

	t.Run("cancellation after maybe-sent stops later writes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": initial}}
		transport := &fakePatroniStatusReader{patchConfigErrors: map[string]error{
			"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryMaybeSent},
		}}
		transport.patchConfigHook = func(baseURL string) {
			if baseURL == "https://node-a:8008" {
				cancel()
			}
		}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PauseRequest{Target: target}
		prepared := service.PreparePause(context.Background(), request)
		result := service.ExecutePause(ctx, request, prepared.Data)
		requireValidPauseResult(t, result)
		if result.Outcome != Unknown || !errors.Is(result.Error, context.Canceled) || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://node-a:8008"}) {
			t.Fatalf("pause cancellation mismatch: result=%#v calls=%v", result, transport.patchConfigCalls)
		}
	})
}
