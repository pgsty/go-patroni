package control

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
)

func snapshotWithPatroniVersion(t *testing.T, snapshot dcs.Snapshot, version string) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.Kind == dcs.KeyMember {
			var document map[string]any
			if err := json.Unmarshal(entry.Value, &document); err != nil {
				t.Fatal(err)
			}
			document["version"] = version
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			entry.Value = encoded
		}
		entries = append(entries, entry)
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision, entries)
}

func TestAuditedPatroniThreeAndFourVersionsPassHighLevelGate(t *testing.T) {
	for _, version := range []string{"3.0.0", "3.3.8", "4.0.0", "4.1.3"} {
		t.Run(version, func(t *testing.T) {
			snapshot := snapshotWithPatroniVersion(t, readFixtureSnapshot(), version)
			if err := checkSnapshotPatroniVersion(snapshot, false); err != nil {
				t.Fatalf("supported version rejected: %v", err)
			}
		})
	}
}

func TestUnsupportedPatroniReadRequiresExplicitBestEffortPolicy(t *testing.T) {
	unsupported := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "5.0.0")
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": unsupported}}
	service := newReadService(t, reader, &fakePatroniStatusReader{})
	target := model.Target{Context: "lab", Scope: "alpha"}

	blocked := service.List(context.Background(), ListRequest{Targets: []model.Target{target}})
	if blocked.Outcome != Failed || blocked.Error == nil || blocked.Error.Category != CategoryUnsupported || blocked.Path != PathDCS {
		t.Fatalf("unsupported list was not blocked: %#v", blocked)
	}
	if err := blocked.Validate(); err != nil {
		t.Fatalf("unsupported list result invalid: %v", err)
	}

	allowed := service.List(context.Background(), ListRequest{Targets: []model.Target{target}, AllowUnsupportedRead: true})
	if allowed.Outcome != Succeeded || len(allowed.Data.Clusters) != 1 {
		t.Fatalf("explicit best-effort list failed: %#v", allowed)
	}

	executor := &fakePostgresQueryExecutor{}
	queryService := newReadServiceWithQuery(t, reader, &fakePatroniStatusReader{}, executor)
	query := queryService.Query(context.Background(), QueryRequest{
		Target: target, Role: RoleLeader, Connection: postgres.ConnectionOptions{}, SQL: "select 1",
	})
	if query.Outcome != Failed || query.Error == nil || query.Error.Category != CategoryUnsupported || len(executor.requests) != 0 {
		t.Fatalf("unsupported arbitrary SQL query crossed fail-closed gate: result=%#v requests=%v", query, executor.requests)
	}

	invalid := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "future-build")
	reader.snapshots["alpha"] = invalid
	invalidResult := service.Topology(context.Background(), TopologyRequest{Target: target})
	if invalidResult.Outcome != Failed || invalidResult.Error == nil || invalidResult.Error.Category != CategoryUnsupported {
		t.Fatalf("unparseable explicit Patroni version was not fail-closed: %#v", invalidResult)
	}
}

func TestVersionRESTProbeHonorsUnsupportedReadPolicy(t *testing.T) {
	base := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "")
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	serverVersion := 160001
	status := &fakePatroniStatusReader{responses: map[string]patroni.Response[patroni.Status]{
		"https://node-a:8008": {StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "5.0.0"}, ServerVersion: &serverVersion}},
		"https://node-b:8008": {StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "5.0.0"}, ServerVersion: &serverVersion}},
		"https://node-c:8008": {StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "5.0.0"}, ServerVersion: &serverVersion}},
	}}
	service := newReadService(t, reader, status)
	target := model.Target{Context: "lab", Scope: "alpha"}

	blocked := service.Version(context.Background(), VersionRequest{Target: target})
	if blocked.Outcome != Failed || blocked.Error == nil || blocked.Error.Category != CategoryUnsupported || len(status.calls) != 1 {
		t.Fatalf("unsupported REST version probe mismatch: result=%#v calls=%v", blocked, status.calls)
	}
	status.calls = nil
	allowed := service.Version(context.Background(), VersionRequest{Target: target, AllowUnsupportedRead: true})
	if allowed.Outcome != Succeeded || len(allowed.Data.Members) != 3 || len(status.calls) != 3 {
		t.Fatalf("best-effort REST version probe mismatch: result=%#v calls=%v", allowed, status.calls)
	}
}

func TestEveryWritePlanFailsClosedForUnsupportedPatroni(t *testing.T) {
	unsupported := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "5.1.2")
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": unsupported}}
	service, err := NewService(ServiceOptions{
		Snapshots: reader,
		Clock:     func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			return "unsupported-write-plan"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	target := model.Target{Context: "lab", Scope: "alpha"}
	tests := map[string]func() Result[Plan]{
		"reload": func() Result[Plan] {
			return service.PrepareReload(context.Background(), ReloadRequest{Target: target, Members: []string{"node-a"}})
		},
		"restart": func() Result[Plan] {
			return service.PrepareRestart(context.Background(), RestartRequest{Target: target, Members: []string{"node-a"}, Force: true})
		},
		"reinit": func() Result[Plan] {
			return service.PrepareReinitialize(context.Background(), ReinitializeRequest{Target: target, Members: []string{"node-b"}, Force: true})
		},
		"failover": func() Result[Plan] {
			return service.PrepareFailover(context.Background(), FailoverRequest{Target: target, Candidate: "node-b", Force: true})
		},
		"switchover": func() Result[Plan] {
			return service.PrepareSwitchover(context.Background(), SwitchoverRequest{Target: target, Leader: "node-a", Candidate: "node-b", Force: true})
		},
		"flush": func() Result[Plan] {
			return service.PrepareFlush(context.Background(), FlushRequest{Target: target, Event: FlushRestart, Members: []string{"node-b"}, Force: true})
		},
		"pause": func() Result[Plan] {
			return service.PreparePause(context.Background(), PauseRequest{Target: target})
		},
		"resume": func() Result[Plan] {
			return service.PrepareResume(context.Background(), PauseRequest{Target: target})
		},
		"edit-config": func() Result[Plan] {
			return service.PrepareEditConfig(context.Background(), EditConfigRequest{Target: target, Settings: []ConfigSetting{{Path: "ttl", Value: 20}}})
		},
		"remove": func() Result[Plan] {
			return service.PrepareRemove(context.Background(), RemoveRequest{Target: target})
		},
		"demote-cluster": func() Result[Plan] {
			return service.PrepareDemoteCluster(context.Background(), DemoteClusterRequest{Target: target, Standby: StandbyConfig{Host: "standby.invalid"}, Force: true})
		},
		"promote-cluster": func() Result[Plan] {
			return service.PreparePromoteCluster(context.Background(), PromoteClusterRequest{Target: target, Force: true})
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			result := run()
			if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnsupported {
				t.Fatalf("unsupported write plan was not blocked: %#v", result)
			}
			if err := result.Validate(); err != nil {
				t.Fatalf("unsupported write result invalid: %v", err)
			}
		})
	}
}

func TestWriteExecutionRechecksVersionBeforeAnySend(t *testing.T) {
	supported := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "4.1.0")
	unsupported := snapshotWithPatroniVersion(t, readFixtureSnapshot(), "5.0.0")
	reader := &fakeSnapshotReader{sequence: []dcs.Snapshot{supported, unsupported}}
	transport := &fakePatroniStatusReader{reloadResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 200}}}
	service := newReadService(t, reader, transport)
	request := ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}}
	prepared := service.PrepareReload(context.Background(), request)
	if prepared.Outcome != Succeeded {
		t.Fatalf("supported prepare failed: %#v", prepared)
	}
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnsupported || len(transport.reloadCalls) != 0 {
		t.Fatalf("version drift crossed write boundary: result=%#v calls=%v", result, transport.reloadCalls)
	}
}
