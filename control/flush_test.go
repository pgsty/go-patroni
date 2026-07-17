package control

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni"
)

const flushSchedule = "2026-07-14T03:04:05Z"

func scheduledSwitchoverSnapshot(t *testing.T, snapshot dcs.Snapshot) dcs.Snapshot {
	t.Helper()
	return snapshotWithFailoverValue(t, snapshot,
		`{"leader":"node-a","member":"node-b","scheduled_at":"`+flushSchedule+`"}`, 31)
}

func requireValidFlushResult(t *testing.T, result Result[FlushData]) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid flush result envelope: %v", err)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("invalid flush data: %v", err)
	}
}

func TestPrepareFlushFreezesPatronictlSelection(t *testing.T) {
	base := readFixtureSnapshot()
	restart := snapshotWithScheduledRestart(t, base, "node-b", flushSchedule)
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": restart}}
	service := newTransitionService(t, reader, &fakePatroniStatusReader{}, &fakeFailoverCAS{})
	target := model.Target{Context: "lab", Scope: "alpha"}

	restartRequest := FlushRequest{Target: target, Event: FlushRestart, Members: []string{"node-c", "node-b"}, Role: RoleReplica}
	restartPlan := service.PrepareFlush(context.Background(), restartRequest)
	if restartPlan.Outcome != Succeeded || restartPlan.Data.Operation != "flush" || restartPlan.Data.Path != PathREST ||
		!reflect.DeepEqual(memberNamesFromTargets(restartPlan.Data.Targets), []string{"node-b", "node-c"}) {
		t.Fatalf("restart flush plan mismatch: %#v", restartPlan)
	}
	if scheduled, ok := expectedPrecondition(restartPlan.Data, "member.node-b.scheduledRestart"); !ok || scheduled != "true" {
		t.Fatalf("scheduled restart precondition missing: %#v", restartPlan.Data.Preconditions)
	}
	if scheduled, ok := expectedPrecondition(restartPlan.Data, "member.node-c.scheduledRestart"); !ok || scheduled != "false" {
		t.Fatalf("no-op restart precondition missing: %#v", restartPlan.Data.Preconditions)
	}
	tampered := restartPlan.Data
	tampered.Targets = nil
	tamperedResult := service.ExecuteFlush(context.Background(), restartRequest, tampered)
	if tamperedResult.Outcome != Failed || tamperedResult.Error == nil || tamperedResult.Error.Category != CategoryUsage {
		t.Fatalf("removed flush targets were accepted: %#v", tamperedResult)
	}

	missingGroup := service.PrepareFlush(context.Background(), FlushRequest{Target: target, Event: FlushRestart, Citus: true})
	if missingGroup.Outcome != Failed || missingGroup.Error == nil || missingGroup.Error.Category != CategoryConfig {
		t.Fatalf("group-less Citus flush without a discoverer mismatch: %#v", missingGroup)
	}

	switchover := scheduledSwitchoverSnapshot(t, base)
	reader.snapshots["alpha"] = switchover
	switchoverRequest := FlushRequest{
		Target: target, Event: FlushSwitchover, Members: []string{"ignored-member"}, Role: RoleReplica,
	}
	switchoverPlan := service.PrepareFlush(context.Background(), switchoverRequest)
	if switchoverPlan.Outcome != Succeeded || switchoverPlan.Data.Path != PathRESTToDCS ||
		!reflect.DeepEqual(memberNamesFromTargets(switchoverPlan.Data.Targets), []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("switchover leader-first plan mismatch: %#v", switchoverPlan)
	}
	if revision, ok := expectedPrecondition(switchoverPlan.Data, "failover.modRevision"); !ok || revision != "31" {
		t.Fatalf("failover revision precondition mismatch: %#v", switchoverPlan.Data.Preconditions)
	}

	reader.snapshots["alpha"] = base
	noEvent := service.PrepareFlush(context.Background(), FlushRequest{Target: target, Event: FlushSwitchover})
	noEventResult := service.ExecuteFlush(context.Background(), FlushRequest{Target: target, Event: FlushSwitchover}, noEvent.Data)
	requireValidFlushResult(t, noEventResult)
	if noEventResult.Outcome != Succeeded || !noEventResult.Data.Noop || noEventResult.Data.RESTSendState != SendNotSent {
		t.Fatalf("no-event switchover flush mismatch: %#v", noEventResult)
	}
}

func TestFlushRestartExpandsAndRevalidatesGroupLessCitusScope(t *testing.T) {
	coordinator := snapshotWithScheduledRestart(t, discoveredGroupFixtureSnapshot("citus", 0, 71, "coordinator"), "coordinator", flushSchedule)
	worker := snapshotWithScheduledRestart(t, discoveredGroupFixtureSnapshot("citus", 1, 72, "worker"), "worker", flushSchedule)
	items := []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}
	discoverer := &fakeDiscoverer{clusters: items}
	transport := &fakePatroniStatusReader{deleteRestartResponses: map[string]patroni.Response[string]{
		"https://coordinator:8008": {StatusCode: 204},
		"https://worker:8008":      {StatusCode: 204},
	}}
	service, err := NewService(ServiceOptions{
		Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}, Discovery: discoverer, Patroni: transport,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-flush-operation" },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := FlushRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Event: FlushRestart, Role: RoleAny, Force: true, Citus: true,
	}
	prepared := service.PrepareFlush(context.Background(), request)
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 2 || prepared.Data.Targets[0].Group == nil ||
		*prepared.Data.Targets[0].Group != 0 || prepared.Data.Targets[1].Group == nil || *prepared.Data.Targets[1].Group != 1 {
		t.Fatalf("Citus restart flush plan mismatch: %#v", prepared)
	}
	if groups, ok := expectedPrecondition(prepared.Data, "citus.groups"); !ok || groups != "0,1" {
		t.Fatalf("Citus restart flush group inventory was not frozen: %#v", prepared.Data.Preconditions)
	}
	result := service.ExecuteFlush(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(result.Data.Results) != 2 ||
		!reflect.DeepEqual(transport.deleteRestartCalls, []string{"https://coordinator:8008", "https://worker:8008"}) {
		t.Fatalf("Citus all-group restart flush mismatch: result=%#v calls=%v", result, transport.deleteRestartCalls)
	}

	transport.deleteRestartCalls = nil
	discoverer.clusters = items[:1]
	changed := service.ExecuteFlush(context.Background(), request, prepared.Data)
	if changed.Outcome != Failed || changed.Error == nil || changed.Error.Category != CategoryConflict || len(transport.deleteRestartCalls) != 0 {
		t.Fatalf("Citus restart flush group drift was not rejected before send: result=%#v calls=%v", changed, transport.deleteRestartCalls)
	}
}

func TestFlushSwitchoverUsesCitusCoordinatorGroup(t *testing.T) {
	coordinatorBase := discoveredGroupFixtureSnapshot("citus", 0, 81, "coordinator")
	coordinator := snapshotWithFailoverValue(t, coordinatorBase,
		`{"leader":"coordinator","member":"","scheduled_at":"`+flushSchedule+`"}`, 79)
	workerBase := discoveredGroupFixtureSnapshot("citus", 1, 82, "worker")
	worker := snapshotWithFailoverValue(t, workerBase,
		`{"leader":"worker","member":"","scheduled_at":"`+flushSchedule+`"}`, 80)
	items := []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}
	discoverer := &fakeDiscoverer{clusters: items}
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"citus": coordinator}}
	transport := &fakePatroniStatusReader{deleteSwitchoverResponses: map[string]patroni.Response[string]{
		"https://coordinator:8008": {StatusCode: 200},
		"https://worker:8008":      {StatusCode: 200},
	}}
	writer := &fakeFailoverCAS{}
	service, err := NewService(ServiceOptions{
		Snapshots: reader, Discovery: discoverer, Patroni: transport, Failover: writer,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-switchover-flush-operation" },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := FlushRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Event: FlushSwitchover, Force: true, Citus: true,
	}
	prepared := service.PrepareFlush(context.Background(), request)
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 1 || prepared.Data.Targets[0].Group == nil ||
		*prepared.Data.Targets[0].Group != 0 || prepared.Data.Targets[0].Member != "coordinator" {
		t.Fatalf("group-less Citus switchover flush did not select coordinator group: %#v", prepared)
	}
	result := service.ExecuteFlush(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || !reflect.DeepEqual(transport.deleteSwitchoverCalls, []string{"https://coordinator:8008"}) || writer.deleteCalls != 0 {
		t.Fatalf("Citus coordinator REST switchover flush mismatch: result=%#v calls=%v", result, transport.deleteSwitchoverCalls)
	}

	transport.deleteSwitchoverResponses = nil
	transport.deleteSwitchoverCalls = nil
	transport.deleteSwitchoverErrors = map[string]error{
		"https://coordinator:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
	}
	cleared := coordinatorBase
	writer.deleteResult = dcs.WriteResult{Applied: true, Revision: 83}
	writer.deleteHook = func(call failoverDeleteCall) { reader.snapshots["citus"] = cleared }
	result = service.ExecuteFlush(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || writer.deleteCalls != 1 || len(writer.deleteRecords) != 1 ||
		writer.deleteRecords[0].Target.Group == nil || *writer.deleteRecords[0].Target.Group != 0 {
		t.Fatalf("Citus coordinator DCS switchover fallback mismatch: result=%#v deletes=%#v", result, writer.deleteRecords)
	}
}

func TestFlushRestartClassifiesAndVerifiesOutcomes(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	request := FlushRequest{Target: target, Event: FlushRestart, Members: []string{"node-b"}, Force: true}

	t.Run("success", func(t *testing.T) {
		scheduled := snapshotWithScheduledRestart(t, readFixtureSnapshot(), "node-b", flushSchedule)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		transport := &fakePatroniStatusReader{deleteRestartResponses: map[string]patroni.Response[string]{"https://node-b:8008": {StatusCode: 204}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Succeeded || result.Data.Verification != VerifiedSucceeded || len(result.Data.Results) != 1 ||
			result.Data.Results[0].HTTPStatus != 204 || len(transport.deleteRestartCalls) != 1 {
			t.Fatalf("successful restart flush mismatch: %#v calls=%v", result, transport.deleteRestartCalls)
		}
	})

	t.Run("definite rejection", func(t *testing.T) {
		scheduled := snapshotWithScheduledRestart(t, readFixtureSnapshot(), "node-b", flushSchedule)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		transport := &fakePatroniStatusReader{deleteRestartResponses: map[string]patroni.Response[string]{"https://node-b:8008": {StatusCode: 404}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Failed || result.Data.Results[0].Outcome != Failed || result.Data.Results[0].HTTPStatus != 404 {
			t.Fatalf("definite restart flush rejection mismatch: %#v", result)
		}
	})

	t.Run("ambiguous send resolved by DCS", func(t *testing.T) {
		scheduled := snapshotWithScheduledRestart(t, readFixtureSnapshot(), "node-b", flushSchedule)
		cleared := readFixtureSnapshot()
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{scheduled, scheduled, cleared}}
		transport := &fakePatroniStatusReader{deleteRestartErrors: map[string]error{
			"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/restart", Delivery: patroni.DeliveryMaybeSent},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Succeeded || result.Data.Results[0].SendState != SendMaybeSent || result.Data.Results[0].Verification != VerifiedSucceeded {
			t.Fatalf("DCS-resolved restart flush mismatch: %#v", result)
		}
	})

	t.Run("ambiguous send without DCS proof remains unknown", func(t *testing.T) {
		scheduled := snapshotWithScheduledRestart(t, readFixtureSnapshot(), "node-b", flushSchedule)
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		transport := &fakePatroniStatusReader{deleteRestartErrors: map[string]error{
			"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/restart", Delivery: patroni.DeliveryMaybeSent},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown || result.Data.Results[0].Verification != Unverified {
			t.Fatalf("unverified restart flush mismatch: %#v", result)
		}
	})

	t.Run("new event after confirmation is not deleted", func(t *testing.T) {
		base := readFixtureSnapshot()
		changed := snapshotWithScheduledRestart(t, base, "node-b", flushSchedule)
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{base, changed}}
		transport := &fakePatroniStatusReader{deleteRestartResponses: map[string]patroni.Response[string]{"https://node-b:8008": {StatusCode: 200}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(transport.deleteRestartCalls) != 0 {
			t.Fatalf("concurrent restart scheduling was not stopped: %#v calls=%v", result, transport.deleteRestartCalls)
		}
	})
}

func TestFlushSwitchoverProbesLeaderFirstAndStopsOnTerminalResponse(t *testing.T) {
	snapshot := scheduledSwitchoverSnapshot(t, readFixtureSnapshot())
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": snapshot}}
	transport := &fakePatroniStatusReader{deleteSwitchoverResponses: map[string]patroni.Response[string]{
		"https://node-a:8008": {StatusCode: 500},
		"https://node-b:8008": {StatusCode: 200},
		"https://node-c:8008": {StatusCode: 200},
	}}
	writer := &fakeFailoverCAS{}
	service := newTransitionService(t, reader, transport, writer)
	request := FlushRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Event: FlushSwitchover}
	prepared := service.PrepareFlush(context.Background(), request)
	result := service.ExecuteFlush(context.Background(), request, prepared.Data)
	requireValidFlushResult(t, result)
	if result.Outcome != Succeeded || !reflect.DeepEqual(transport.deleteSwitchoverCalls, []string{"https://node-a:8008", "https://node-b:8008"}) ||
		len(result.Data.Results) != 2 || writer.deleteCalls != 0 {
		t.Fatalf("leader-first terminal success mismatch: result=%#v calls=%v dcs=%d", result, transport.deleteSwitchoverCalls, writer.deleteCalls)
	}

	transport.deleteSwitchoverCalls = nil
	transport.deleteSwitchoverResponses["https://node-a:8008"] = patroni.Response[string]{StatusCode: 404}
	result = service.ExecuteFlush(context.Background(), request, prepared.Data)
	requireValidFlushResult(t, result)
	if result.Outcome != Failed || !reflect.DeepEqual(transport.deleteSwitchoverCalls, []string{"https://node-a:8008"}) || writer.deleteCalls != 0 {
		t.Fatalf("terminal 404 compatibility mismatch: result=%#v calls=%v dcs=%d", result, transport.deleteSwitchoverCalls, writer.deleteCalls)
	}
}

func TestFlushSwitchoverFallsBackToExactDCSDelete(t *testing.T) {
	scheduled := scheduledSwitchoverSnapshot(t, readFixtureSnapshot())
	cleared := readFixtureSnapshot()
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
	transport := &fakePatroniStatusReader{deleteSwitchoverErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryMaybeSent},
		"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
		"https://node-c:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
	}}
	writer := &fakeFailoverCAS{deleteResult: dcs.WriteResult{Applied: true, Revision: 32}}
	writer.deleteHook = func(call failoverDeleteCall) { reader.snapshots["alpha"] = cleared }
	service := newTransitionService(t, reader, transport, writer)
	request := FlushRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Event: FlushSwitchover}
	prepared := service.PrepareFlush(context.Background(), request)
	result := service.ExecuteFlush(context.Background(), request, prepared.Data)
	requireValidFlushResult(t, result)
	if result.Outcome != Succeeded || result.Path != PathRESTToDCS || result.Data.RESTSendState != SendMaybeSent ||
		result.Data.DCSSendState != SendAccepted || result.Data.Verification != VerifiedSucceeded || writer.deleteCalls != 1 ||
		len(writer.deleteRecords) != 1 || writer.deleteRecords[0].ExpectedRevision == nil || *writer.deleteRecords[0].ExpectedRevision != 31 {
		t.Fatalf("switchover DCS fallback mismatch: result=%#v delete=%#v", result, writer.deleteRecords)
	}
}

func TestFlushSwitchoverConflictAndCancellationRemainSafe(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	scheduled := scheduledSwitchoverSnapshot(t, readFixtureSnapshot())

	t.Run("ambiguous REST plus DCS conflict is unknown", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		transport := &fakePatroniStatusReader{deleteSwitchoverErrors: map[string]error{
			"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryMaybeSent},
			"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
			"https://node-c:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
		}}
		writer := &fakeFailoverCAS{deleteError: &dcs.ConflictError{Key: "failover", ExpectedRevision: 31, ObservedRevision: 32}}
		service := newTransitionService(t, reader, transport, writer)
		request := FlushRequest{Target: target, Event: FlushSwitchover}
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown || writer.deleteCalls != 1 {
			t.Fatalf("ambiguous conflict classification mismatch: %#v", result)
		}
	})

	t.Run("definite REST misses plus DCS conflict fail", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		notSent := &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent}
		transport := &fakePatroniStatusReader{deleteSwitchoverErrors: map[string]error{
			"https://node-a:8008": notSent, "https://node-b:8008": notSent, "https://node-c:8008": notSent,
		}}
		writer := &fakeFailoverCAS{deleteError: &dcs.ConflictError{Key: "failover", ExpectedRevision: 31, ObservedRevision: 32}}
		service := newTransitionService(t, reader, transport, writer)
		request := FlushRequest{Target: target, Event: FlushSwitchover}
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(context.Background(), request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || writer.deleteCalls != 1 {
			t.Fatalf("definite conflict classification mismatch: %#v", result)
		}
	})

	t.Run("cancellation stops later REST and DCS writes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": scheduled}}
		transport := &fakePatroniStatusReader{deleteSwitchoverErrors: map[string]error{
			"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/switchover", Delivery: patroni.DeliveryMaybeSent},
		}}
		transport.deleteSwitchoverHook = func(baseURL string) {
			if baseURL == "https://node-a:8008" {
				cancel()
			}
		}
		writer := &fakeFailoverCAS{}
		service := newTransitionService(t, reader, transport, writer)
		request := FlushRequest{Target: target, Event: FlushSwitchover}
		prepared := service.PrepareFlush(context.Background(), request)
		result := service.ExecuteFlush(ctx, request, prepared.Data)
		requireValidFlushResult(t, result)
		if result.Outcome != Unknown || !errors.Is(result.Error, context.Canceled) ||
			!reflect.DeepEqual(transport.deleteSwitchoverCalls, []string{"https://node-a:8008"}) || writer.deleteCalls != 0 {
			t.Fatalf("cancellation safety mismatch: result=%#v calls=%v dcs=%d", result, transport.deleteSwitchoverCalls, writer.deleteCalls)
		}
	})
}
