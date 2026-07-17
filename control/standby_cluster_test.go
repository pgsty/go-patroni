package control

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

func snapshotWithClusterRole(
	t *testing.T,
	snapshot dcs.Snapshot,
	leader string,
	roles map[string]string,
	states map[string]string,
) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, 0, len(snapshot.Entries))
	leaderFound := false
	for _, original := range snapshot.Entries {
		entry := original.Clone()
		if entry.RelativePath == "leader" {
			leaderFound = true
			if leader == "" {
				continue
			}
			entry.Value = []byte(leader)
			entry.ModRevision++
		}
		if strings.HasPrefix(entry.RelativePath, "members/") {
			name := strings.TrimPrefix(entry.RelativePath, "members/")
			var value map[string]any
			if err := json.Unmarshal(entry.Value, &value); err != nil {
				t.Fatal(err)
			}
			if role, ok := roles[name]; ok {
				value["role"] = role
			}
			if state, ok := states[name]; ok {
				value["state"] = state
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			entry.Value = encoded
			entry.ModRevision++
		}
		entries = append(entries, entry)
	}
	if leader != "" && !leaderFound {
		entries = append(entries, dcs.Entry{RelativePath: "leader", ModRevision: snapshot.Revision + 1, Value: []byte(leader)})
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func requireValidStandbyResult(t *testing.T, result Result[ClusterRoleData]) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid standby-cluster result: %v", err)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("invalid standby-cluster data: %v", err)
	}
}

func TestStandbyClusterVerificationPolicyIsBoundedInjectableAndDelayed(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	primary := snapshotWithClusterRole(t, readFixtureSnapshot(), "node-a", map[string]string{"node-a": "primary"}, nil)
	standby := snapshotWithClusterRole(t, primary, "node-a", map[string]string{"node-a": "standby_leader"}, nil)
	promoted := snapshotWithClusterRole(t, standby, "node-a", map[string]string{"node-a": "primary"}, map[string]string{"node-a": "running"})
	reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{standby, standby, standby, standby, promoted}}
	transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
		"https://node-a:8008": {StatusCode: 200},
	}}
	waits := make([]time.Duration, 0, 2)
	service, err := NewService(ServiceOptions{
		Snapshots: reader,
		Patroni:   transport,
		Wait: func(ctx context.Context, duration time.Duration) error {
			waits = append(waits, duration)
			return ctx.Err()
		},
		StandbyVerificationAttempts: 4,
		StandbyVerificationInterval: 17 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := PromoteClusterRequest{Target: target, Force: true}
	prepared := service.PreparePromoteCluster(context.Background(), request)
	result := service.ExecutePromoteCluster(context.Background(), request, prepared.Data)
	requireValidStandbyResult(t, result)
	if result.Outcome != Succeeded || !reflect.DeepEqual(waits, []time.Duration{17 * time.Millisecond, 17 * time.Millisecond}) || len(transport.patchConfigCalls) != 1 {
		t.Fatalf("delayed convergence policy mismatch: result=%#v waits=%v calls=%v", result, waits, transport.patchConfigCalls)
	}

	defaults, err := NewService(ServiceOptions{Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}}})
	if err != nil {
		t.Fatal(err)
	}
	if defaults.standbyVerificationAttempts != 31 || defaults.standbyVerificationInterval != time.Second {
		t.Fatalf("standby defaults=%d/%s, want 31/1s", defaults.standbyVerificationAttempts, defaults.standbyVerificationInterval)
	}
	legacyOverride, err := NewService(ServiceOptions{
		Snapshots:            &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}},
		VerificationAttempts: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if legacyOverride.standbyVerificationAttempts != 9 {
		t.Fatalf("explicit legacy verification override was not preserved: %d", legacyOverride.standbyVerificationAttempts)
	}
}

func TestPrepareStandbyClusterTransitionsFreezeLeaderAndSecretSafePlan(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	primary := snapshotWithClusterRole(t, readFixtureSnapshot(), "node-a", map[string]string{"node-a": "primary"}, nil)
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}}
	service := newTransitionService(t, reader, &fakePatroniStatusReader{}, &fakeFailoverCAS{})

	invalid := service.PrepareDemoteCluster(context.Background(), DemoteClusterRequest{
		Target: target, Standby: StandbyConfig{PrimarySlotName: "slot-only"},
	})
	if invalid.Outcome != Failed || invalid.Error == nil || invalid.Error.Category != CategoryUsage {
		t.Fatalf("demote source-compatible required options mismatch: %#v", invalid)
	}

	withoutLeader := snapshotWithClusterRole(t, primary, "", nil, nil)
	reader.snapshots["alpha"] = withoutLeader
	missing := service.PrepareDemoteCluster(context.Background(), DemoteClusterRequest{
		Target: target, Standby: StandbyConfig{RestoreCommand: "restore-safe-fixture"},
	})
	if missing.Outcome != Failed || missing.Error == nil || missing.Error.Category != CategoryNotFound {
		t.Fatalf("leaderless demote mismatch: %#v", missing)
	}

	const protected = "standby-sensitive-fixture"
	reader.snapshots["alpha"] = primary
	request := DemoteClusterRequest{Target: target, Force: true, Standby: StandbyConfig{
		Host: protected + ".invalid", Port: 5432, RestoreCommand: "fetch " + protected, PrimarySlotName: "slot_" + protected,
	}}
	prepared := service.PrepareDemoteCluster(context.Background(), request)
	if prepared.Outcome != Succeeded || prepared.Data.Operation != "demote-cluster" || prepared.Data.Risk != RiskAvailability ||
		prepared.Data.RetrySafety != UnsafeAfterSend || !reflect.DeepEqual(memberNamesFromTargets(prepared.Data.Targets), []string{"node-a"}) {
		t.Fatalf("demote Plan mismatch: %#v", prepared)
	}
	for _, field := range []string{"cluster.leader", "leader.role", "leader.state", "desired.role", "request.binding", "state.binding"} {
		if _, ok := expectedPrecondition(prepared.Data, field); !ok {
			t.Fatalf("demote Plan lacks %s: %#v", field, prepared.Data.Preconditions)
		}
	}
	encodedPlan, err := json.Marshal(prepared.Data)
	if err != nil {
		t.Fatal(err)
	}
	encodedRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedPlan), protected) || strings.Contains(string(encodedRequest), protected) || strings.Contains(request.String(), protected) {
		t.Fatalf("standby values leaked through public formatting: plan=%s request=%s string=%s", encodedPlan, encodedRequest, request.String())
	}

	standby := snapshotWithClusterRole(t, primary, "node-a", map[string]string{"node-a": "standby_leader"}, nil)
	reader.snapshots["alpha"] = standby
	already := service.PrepareDemoteCluster(context.Background(), DemoteClusterRequest{
		Target: target, Standby: StandbyConfig{Host: "remote.invalid"},
	})
	if already.Outcome != Failed || already.Error == nil || already.Error.Category != CategoryConflict {
		t.Fatalf("already-demoted precondition mismatch: %#v", already)
	}
	promote := service.PreparePromoteCluster(context.Background(), PromoteClusterRequest{Target: target, Force: true})
	if promote.Outcome != Succeeded {
		t.Fatalf("promote Plan mismatch: %#v", promote)
	}
	if desired, ok := expectedPrecondition(promote.Data, "desired.role"); !ok || desired != "primary" {
		t.Fatalf("promote desired role mismatch: %#v", promote.Data.Preconditions)
	}
}

func TestStandbyClusterCitusCommandsSelectCoordinatorGroupZero(t *testing.T) {
	coordinatorPrimary := discoveredGroupFixtureSnapshot("citus", 0, 91, "coordinator")
	coordinatorStandby := snapshotWithClusterRole(t, coordinatorPrimary, "coordinator", map[string]string{"coordinator": "standby_leader"}, nil)
	coordinatorPromoted := snapshotWithClusterRole(t, coordinatorStandby, "coordinator", map[string]string{"coordinator": "primary"}, nil)
	worker := discoveredGroupFixtureSnapshot("citus", 1, 92, "worker")
	discoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinatorPrimary.Target, Revision: coordinatorPrimary.Revision, Snapshot: &coordinatorPrimary},
	}}
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"citus": coordinatorStandby}}
	transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
		"https://coordinator:8008": {StatusCode: 200},
		"https://worker:8008":      {StatusCode: 200},
	}}
	service, err := NewService(ServiceOptions{
		Snapshots: reader, Discovery: discoverer, Patroni: transport,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-standby-operation" },
		StandbyVerificationAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}
	workerGroup := 1
	for operation, result := range map[string]Result[Plan]{
		"demote-cluster": service.PrepareDemoteCluster(context.Background(), DemoteClusterRequest{
			Target:  model.Target{Context: "lab", Namespace: "/service", Scope: "citus", Group: &workerGroup},
			Standby: StandbyConfig{Host: "remote.invalid"}, Force: true, Citus: true,
		}),
		"promote-cluster": service.PreparePromoteCluster(context.Background(), PromoteClusterRequest{
			Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus", Group: &workerGroup}, Force: true, Citus: true,
		}),
	} {
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage {
			t.Fatalf("%s accepted an unavailable Patroni Citus group selector: %#v", operation, result)
		}
	}
	demoteRequest := DemoteClusterRequest{Target: target, Standby: StandbyConfig{Host: "remote.invalid"}, Force: true, Citus: true}
	preparedDemote := service.PrepareDemoteCluster(context.Background(), demoteRequest)
	if preparedDemote.Outcome != Succeeded || len(preparedDemote.Data.Targets) != 1 || preparedDemote.Data.Targets[0].Group == nil ||
		*preparedDemote.Data.Targets[0].Group != 0 || preparedDemote.Data.Targets[0].Member != "coordinator" {
		t.Fatalf("Citus demote did not freeze coordinator group 0: %#v", preparedDemote)
	}
	demoted := service.ExecuteDemoteCluster(context.Background(), demoteRequest, preparedDemote.Data)
	if demoted.Outcome != Succeeded || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://coordinator:8008"}) {
		t.Fatalf("Citus demote crossed coordinator boundary: result=%#v calls=%v", demoted, transport.patchConfigCalls)
	}

	discoverer.clusters[1].Snapshot = &coordinatorStandby
	reader.snapshots["citus"] = coordinatorPromoted
	transport.patchConfigCalls = nil
	transport.patchConfigRequests = nil
	promoteRequest := PromoteClusterRequest{Target: target, Force: true, Citus: true}
	preparedPromote := service.PreparePromoteCluster(context.Background(), promoteRequest)
	promoted := service.ExecutePromoteCluster(context.Background(), promoteRequest, preparedPromote.Data)
	if promoted.Outcome != Succeeded || len(preparedPromote.Data.Targets) != 1 || preparedPromote.Data.Targets[0].Group == nil ||
		*preparedPromote.Data.Targets[0].Group != 0 || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://coordinator:8008"}) {
		t.Fatalf("Citus promote crossed coordinator boundary: prepared=%#v result=%#v calls=%v", preparedPromote, promoted, transport.patchConfigCalls)
	}
}

func TestStandbyClusterTransitionPayloadAndDefiniteOutcomes(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	primary := snapshotWithClusterRole(t, readFixtureSnapshot(), "node-a", map[string]string{"node-a": "primary"}, nil)
	demoted := snapshotWithClusterRole(t, primary, "node-b",
		map[string]string{"node-a": "replica", "node-b": "standby_leader"},
		map[string]string{"node-a": "running", "node-b": "running"})

	t.Run("demote exact truthy payload and convergence", func(t *testing.T) {
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, primary, demoted}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
			"https://node-a:8008": {StatusCode: 200},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := DemoteClusterRequest{Target: target, Force: true, Standby: StandbyConfig{
			Host: "remote.invalid", Port: 5432, RestoreCommand: "restore-command", PrimarySlotName: "remote_slot",
		}}
		prepared := service.PrepareDemoteCluster(context.Background(), request)
		result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
		requireValidStandbyResult(t, result)
		want := patroni.DynamicConfig{"standby_cluster": map[string]any{
			"host": "remote.invalid", "port": 5432, "primary_slot_name": "remote_slot", "restore_command": "restore-command",
		}}
		if result.Outcome != Succeeded || result.Data.Leader != "node-b" || result.Data.PreviousLeader != "node-a" ||
			result.Data.HTTPStatus != 200 || result.Data.RESTSendState != SendAccepted || !reflect.DeepEqual(transport.patchConfigCalls, []string{"https://node-a:8008"}) ||
			len(transport.patchConfigRequests) != 1 || !reflect.DeepEqual(transport.patchConfigRequests[0], want) {
			t.Fatalf("demote wire/convergence mismatch: result=%#v calls=%v payload=%#v", result, transport.patchConfigCalls, transport.patchConfigRequests)
		}
	})

	t.Run("promote exact null payload", func(t *testing.T) {
		standby := snapshotWithClusterRole(t, primary, "node-a", map[string]string{"node-a": "standby_leader"}, nil)
		promoted := snapshotWithClusterRole(t, standby, "node-a", map[string]string{"node-a": "primary"}, map[string]string{"node-a": "running"})
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{standby, standby, promoted}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{
			"https://node-a:8008": {StatusCode: 200},
		}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		request := PromoteClusterRequest{Target: target, Force: true}
		prepared := service.PreparePromoteCluster(context.Background(), request)
		result := service.ExecutePromoteCluster(context.Background(), request, prepared.Data)
		requireValidStandbyResult(t, result)
		value, present := transport.patchConfigRequests[0]["standby_cluster"]
		if result.Outcome != Succeeded || !present || value != nil || len(transport.patchConfigRequests[0]) != 1 {
			t.Fatalf("promote wire mismatch: result=%#v payload=%#v", result, transport.patchConfigRequests)
		}
	})

	for name, testCase := range map[string]struct {
		response patroni.Response[patroni.DynamicConfig]
		err      error
		category Category
	}{
		"HTTP 500": {response: patroni.Response[patroni.DynamicConfig]{StatusCode: 500}, category: CategoryUnreachable},
		"HTTP 202": {response: patroni.Response[patroni.DynamicConfig]{StatusCode: 202}, category: CategoryFailed},
		"not sent": {err: &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryNotSent}, category: CategoryUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, primary}}
			transport := &fakePatroniStatusReader{
				patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": testCase.response},
				patchConfigErrors:    map[string]error{"https://node-a:8008": testCase.err},
			}
			service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
			request := DemoteClusterRequest{Target: target, Force: true, Standby: StandbyConfig{Host: "remote.invalid"}}
			prepared := service.PrepareDemoteCluster(context.Background(), request)
			result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
			requireValidStandbyResult(t, result)
			if result.Outcome != Failed || result.Error == nil || result.Error.Category != testCase.category || len(transport.patchConfigCalls) != 1 {
				t.Fatalf("definite outcome mismatch: %#v calls=%v", result, transport.patchConfigCalls)
			}
		})
	}
}

func TestStandbyClusterTransitionAmbiguousReadback(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	primary := snapshotWithClusterRole(t, readFixtureSnapshot(), "node-a", map[string]string{"node-a": "primary"}, nil)
	demoted := snapshotWithClusterRole(t, primary, "node-b",
		map[string]string{"node-a": "replica", "node-b": "standby_leader"},
		map[string]string{"node-a": "running", "node-b": "running"})
	maybe := &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryMaybeSent}

	for name, testCase := range map[string]struct {
		response     patroni.Response[patroni.DynamicConfig]
		callError    error
		verification dcs.Snapshot
		outcome      Outcome
	}{
		"maybe sent converged":  {callError: maybe, verification: demoted, outcome: Succeeded},
		"maybe sent unresolved": {callError: maybe, verification: primary, outcome: Unknown},
		"accepted unresolved":   {response: patroni.Response[patroni.DynamicConfig]{StatusCode: 200}, verification: primary, outcome: Unknown},
	} {
		t.Run(name, func(t *testing.T) {
			reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, primary, testCase.verification, testCase.verification}}
			transport := &fakePatroniStatusReader{
				patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": testCase.response},
				patchConfigErrors:    map[string]error{"https://node-a:8008": testCase.callError},
			}
			service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
			request := DemoteClusterRequest{Target: target, Force: true, Standby: StandbyConfig{RestoreCommand: "restore-safe"}}
			prepared := service.PrepareDemoteCluster(context.Background(), request)
			result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
			requireValidStandbyResult(t, result)
			if result.Outcome != testCase.outcome || len(transport.patchConfigCalls) != 1 {
				t.Fatalf("ambiguous readback mismatch: %#v", result)
			}
			if testCase.outcome == Unknown && (result.Error == nil || result.Error.Category != CategoryUnknown || result.Data.Verification != Unverified) {
				t.Fatalf("unresolved write was not UNKNOWN: %#v", result)
			}
		})
	}
}

func TestStandbyClusterTransitionConvergenceConcurrencyCancellation(t *testing.T) {
	target := model.Target{Context: "lab", Scope: "alpha"}
	primary := snapshotWithClusterRole(t, readFixtureSnapshot(), "node-a", map[string]string{"node-a": "primary"}, nil)
	request := DemoteClusterRequest{Target: target, Force: true, Standby: StandbyConfig{Host: "remote.invalid"}}

	t.Run("material topology change fails before send", func(t *testing.T) {
		changed := snapshotWithClusterRole(t, primary, "node-b", map[string]string{"node-b": "replica"}, nil)
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, changed}}
		transport := &fakePatroniStatusReader{}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(context.Background(), request)
		result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
		requireValidStandbyResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("pre-write concurrency mismatch: %#v calls=%v", result, transport.patchConfigCalls)
		}
	})

	t.Run("concurrent desired convergence is verified no-op", func(t *testing.T) {
		demoted := snapshotWithClusterRole(t, primary, "node-b",
			map[string]string{"node-a": "replica", "node-b": "standby_leader"},
			map[string]string{"node-a": "running", "node-b": "running"})
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, demoted}}
		transport := &fakePatroniStatusReader{}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(context.Background(), request)
		result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
		requireValidStandbyResult(t, result)
		if result.Outcome != Succeeded || !result.Data.Noop || result.Data.RESTSendState != SendNotSent || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("concurrent desired no-op mismatch: %#v", result)
		}
	})

	t.Run("old leader must return to running after demote", func(t *testing.T) {
		pending := snapshotWithClusterRole(t, primary, "node-b",
			map[string]string{"node-a": "replica", "node-b": "standby_leader"},
			map[string]string{"node-a": "stopping", "node-b": "running"})
		reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{primary, primary, pending, pending}}
		transport := &fakePatroniStatusReader{patchConfigResponses: map[string]patroni.Response[patroni.DynamicConfig]{"https://node-a:8008": {StatusCode: 200}}}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(context.Background(), request)
		result := service.ExecuteDemoteCluster(context.Background(), request, prepared.Data)
		requireValidStandbyResult(t, result)
		if result.Outcome != Unknown || !reflect.DeepEqual(result.Data.PendingConditions, []string{"previous-leader.state"}) {
			t.Fatalf("old-leader convergence mismatch: %#v", result)
		}
	})

	t.Run("request tampering is rejected before send", func(t *testing.T) {
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}}
		transport := &fakePatroniStatusReader{}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(context.Background(), request)
		tampered := request
		tampered.Standby.Host = "different.invalid"
		result := service.ExecuteDemoteCluster(context.Background(), tampered, prepared.Data)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("request binding mismatch: %#v", result)
		}
		planTampered := prepared.Data
		for index := range planTampered.Preconditions {
			if planTampered.Preconditions[index].Field == "leader.state" {
				planTampered.Preconditions[index].Expected = "stopped"
			}
		}
		result = service.ExecuteDemoteCluster(context.Background(), request, planTampered)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("Plan binding mismatch: %#v", result)
		}
	})

	t.Run("cancellation before send is definite", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}}
		transport := &fakePatroniStatusReader{}
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(ctx, request)
		cancel()
		result := service.ExecuteDemoteCluster(ctx, request, prepared.Data)
		requireValidStandbyResult(t, result)
		if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryFailed || len(transport.patchConfigCalls) != 0 {
			t.Fatalf("pre-send cancellation mismatch: %#v", result)
		}
	})

	t.Run("cancellation after maybe sent remains unknown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": primary}}
		transport := &fakePatroniStatusReader{patchConfigErrors: map[string]error{
			"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "PATCH", Endpoint: "/config", Delivery: patroni.DeliveryMaybeSent},
		}}
		transport.patchConfigHook = func(string) { cancel() }
		service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
		prepared := service.PrepareDemoteCluster(ctx, request)
		result := service.ExecuteDemoteCluster(ctx, request, prepared.Data)
		requireValidStandbyResult(t, result)
		if result.Outcome != Unknown || !errors.Is(result.Error, context.Canceled) || len(transport.patchConfigCalls) != 1 {
			t.Fatalf("post-send cancellation mismatch: %#v calls=%v", result, transport.patchConfigCalls)
		}
	})
}
