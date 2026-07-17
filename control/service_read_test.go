package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/postgres"
)

var fixedControlTime = time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

type fakeSnapshotReader struct {
	snapshots map[string]dcs.Snapshot
	sequence  []dcs.Snapshot
	err       error
	calls     []model.Target
}

func (reader *fakeSnapshotReader) Snapshot(ctx context.Context, target model.Target) (dcs.Snapshot, error) {
	if ctx == nil {
		return dcs.Snapshot{}, errors.New("nil context")
	}
	reader.calls = append(reader.calls, target)
	if reader.err != nil {
		return dcs.Snapshot{}, reader.err
	}
	if len(reader.sequence) > 0 {
		snapshot := reader.sequence[0]
		reader.sequence = reader.sequence[1:]
		return snapshot, nil
	}
	return reader.snapshots[target.Normalize().Scope], nil
}

type fakePatroniStatusReader struct {
	responses                 map[string]patroni.Response[patroni.Status]
	errors                    map[string]error
	calls                     []string
	reloadResponses           map[string]patroni.Response[string]
	reloadErrors              map[string]error
	reloadCalls               []string
	reloadHook                func(string)
	restartResponses          map[string]patroni.Response[string]
	restartErrors             map[string]error
	restartCalls              []string
	restartRequests           []patroni.RestartRequest
	deleteRestartResponses    map[string]patroni.Response[string]
	deleteRestartErrors       map[string]error
	deleteRestartCalls        []string
	reinitializeResponses     map[string]patroni.Response[string]
	reinitializeErrors        map[string]error
	reinitializeCalls         []string
	reinitializeRequests      []patroni.ReinitializeRequest
	failoverResponses         map[string]patroni.Response[string]
	failoverErrors            map[string]error
	failoverCalls             []string
	failoverRequests          []patroni.FailoverRequest
	failoverHook              func(string)
	switchoverResponses       map[string]patroni.Response[string]
	switchoverErrors          map[string]error
	switchoverCalls           []string
	switchoverRequests        []patroni.FailoverRequest
	switchoverHook            func(string)
	deleteSwitchoverResponses map[string]patroni.Response[string]
	deleteSwitchoverErrors    map[string]error
	deleteSwitchoverCalls     []string
	deleteSwitchoverHook      func(string)
	patchConfigResponses      map[string]patroni.Response[patroni.DynamicConfig]
	patchConfigErrors         map[string]error
	patchConfigCalls          []string
	patchConfigRequests       []patroni.DynamicConfig
	patchConfigHook           func(string)
	patroniSequences          map[string][]patroni.Response[patroni.Status]
	patroniErrorSequences     map[string][]error
}

type failoverWriteCall struct {
	Target           model.Target
	Value            []byte
	ExpectedRevision *int64
}

type failoverDeleteCall struct {
	Target           model.Target
	ExpectedRevision *int64
}

type fakeFailoverCAS struct {
	writeResult   dcs.WriteResult
	writeError    error
	writeCalls    []failoverWriteCall
	writeHook     func(failoverWriteCall)
	deleteResult  dcs.WriteResult
	deleteError   error
	deleteCalls   int
	deleteRecords []failoverDeleteCall
	deleteHook    func(failoverDeleteCall)
}

func (store *fakeFailoverCAS) WriteFailover(ctx context.Context, target model.Target, value []byte, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	call := failoverWriteCall{Target: target, Value: append([]byte(nil), value...)}
	if expected != nil {
		revision := *expected
		call.ExpectedRevision = &revision
	}
	store.writeCalls = append(store.writeCalls, call)
	if store.writeHook != nil {
		store.writeHook(call)
	}
	return store.writeResult, store.writeError
}

func (store *fakeFailoverCAS) DeleteFailover(ctx context.Context, target model.Target, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	store.deleteCalls++
	call := failoverDeleteCall{Target: target}
	if expected != nil {
		revision := *expected
		call.ExpectedRevision = &revision
	}
	store.deleteRecords = append(store.deleteRecords, call)
	if store.deleteHook != nil {
		store.deleteHook(call)
	}
	return store.deleteResult, store.deleteError
}

type fakePostgresQueryExecutor struct {
	result      postgres.QueryResult
	err         error
	connections []postgres.ConnectionOptions
	expected    []postgres.RecoveryExpectation
	requests    []postgres.QueryRequest
}

func (executor *fakePostgresQueryExecutor) QueryChecked(
	ctx context.Context,
	connection postgres.ConnectionOptions,
	expectation postgres.RecoveryExpectation,
	request postgres.QueryRequest,
) (postgres.QueryResult, error) {
	if ctx == nil {
		return postgres.QueryResult{}, errors.New("nil context")
	}
	executor.connections = append(executor.connections, connection)
	executor.expected = append(executor.expected, expectation)
	executor.requests = append(executor.requests, request)
	return executor.result, executor.err
}

func (reader *fakePatroniStatusReader) GetPatroni(ctx context.Context, baseURL string) (patroni.Response[patroni.Status], error) {
	if ctx == nil {
		return patroni.Response[patroni.Status]{}, errors.New("nil context")
	}
	reader.calls = append(reader.calls, baseURL)
	if sequence := reader.patroniSequences[baseURL]; len(sequence) > 0 {
		response := sequence[0]
		reader.patroniSequences[baseURL] = sequence[1:]
		var err error
		if errorsSequence := reader.patroniErrorSequences[baseURL]; len(errorsSequence) > 0 {
			err = errorsSequence[0]
			reader.patroniErrorSequences[baseURL] = errorsSequence[1:]
		}
		return response, err
	}
	return reader.responses[baseURL], reader.errors[baseURL]
}

func (reader *fakePatroniStatusReader) PostReload(ctx context.Context, baseURL string) (patroni.Response[string], error) {
	if ctx == nil {
		return patroni.Response[string]{}, errors.New("nil context")
	}
	reader.reloadCalls = append(reader.reloadCalls, baseURL)
	if reader.reloadHook != nil {
		reader.reloadHook(baseURL)
	}
	return reader.reloadResponses[baseURL], reader.reloadErrors[baseURL]
}

func (reader *fakePatroniStatusReader) PostRestart(ctx context.Context, baseURL string, request patroni.RestartRequest) (patroni.Response[string], error) {
	reader.restartCalls = append(reader.restartCalls, baseURL)
	reader.restartRequests = append(reader.restartRequests, request)
	return reader.restartResponses[baseURL], reader.restartErrors[baseURL]
}
func (reader *fakePatroniStatusReader) DeleteRestart(ctx context.Context, baseURL string) (patroni.Response[string], error) {
	reader.deleteRestartCalls = append(reader.deleteRestartCalls, baseURL)
	return reader.deleteRestartResponses[baseURL], reader.deleteRestartErrors[baseURL]
}
func (reader *fakePatroniStatusReader) PostReinitialize(ctx context.Context, baseURL string, request patroni.ReinitializeRequest) (patroni.Response[string], error) {
	reader.reinitializeCalls = append(reader.reinitializeCalls, baseURL)
	reader.reinitializeRequests = append(reader.reinitializeRequests, request)
	return reader.reinitializeResponses[baseURL], reader.reinitializeErrors[baseURL]
}
func (reader *fakePatroniStatusReader) PostFailover(_ context.Context, baseURL string, request patroni.FailoverRequest) (patroni.Response[string], error) {
	reader.failoverCalls = append(reader.failoverCalls, baseURL)
	reader.failoverRequests = append(reader.failoverRequests, request)
	if reader.failoverHook != nil {
		reader.failoverHook(baseURL)
	}
	return reader.failoverResponses[baseURL], reader.failoverErrors[baseURL]
}
func (reader *fakePatroniStatusReader) PostSwitchover(_ context.Context, baseURL string, request patroni.FailoverRequest) (patroni.Response[string], error) {
	reader.switchoverCalls = append(reader.switchoverCalls, baseURL)
	reader.switchoverRequests = append(reader.switchoverRequests, request)
	if reader.switchoverHook != nil {
		reader.switchoverHook(baseURL)
	}
	return reader.switchoverResponses[baseURL], reader.switchoverErrors[baseURL]
}
func (reader *fakePatroniStatusReader) DeleteSwitchover(ctx context.Context, baseURL string) (patroni.Response[string], error) {
	if ctx == nil {
		return patroni.Response[string]{}, errors.New("nil context")
	}
	reader.deleteSwitchoverCalls = append(reader.deleteSwitchoverCalls, baseURL)
	if reader.deleteSwitchoverHook != nil {
		reader.deleteSwitchoverHook(baseURL)
	}
	return reader.deleteSwitchoverResponses[baseURL], reader.deleteSwitchoverErrors[baseURL]
}
func (reader *fakePatroniStatusReader) PatchConfig(ctx context.Context, baseURL string, request patroni.DynamicConfig) (patroni.Response[patroni.DynamicConfig], error) {
	if ctx == nil {
		return patroni.Response[patroni.DynamicConfig]{}, errors.New("nil context")
	}
	reader.patchConfigCalls = append(reader.patchConfigCalls, baseURL)
	reader.patchConfigRequests = append(reader.patchConfigRequests, request)
	if reader.patchConfigHook != nil {
		reader.patchConfigHook(baseURL)
	}
	return reader.patchConfigResponses[baseURL], reader.patchConfigErrors[baseURL]
}

func newReadService(t *testing.T, snapshots *fakeSnapshotReader, status *fakePatroniStatusReader) *Service {
	return newReadServiceWithQuery(t, snapshots, status, nil)
}

func newReadServiceWithQuery(t *testing.T, snapshots *fakeSnapshotReader, status *fakePatroniStatusReader, query *fakePostgresQueryExecutor) *Service {
	t.Helper()
	sequence := 0
	service, err := NewService(ServiceOptions{
		Snapshots:            snapshots,
		Patroni:              status,
		Postgres:             query,
		RandomIndex:          func(length int) (int, error) { return length - 1, nil },
		Wait:                 func(context.Context, time.Duration) error { return nil },
		VerificationAttempts: 2,
		Clock:                func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			sequence++
			return "read-operation-" + string(rune('0'+sequence))
		},
		ProductVersion: "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func newTransitionService(t *testing.T, snapshots *fakeSnapshotReader, status *fakePatroniStatusReader, failover *fakeFailoverCAS) *Service {
	t.Helper()
	sequence := 0
	service, err := NewService(ServiceOptions{
		Snapshots: snapshots,
		Patroni:   status,
		Failover:  failover,
		Wait:      func(context.Context, time.Duration) error { return nil },
		Clock:     func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			sequence++
			return fmt.Sprintf("transition-operation-%d", sequence)
		},
		VerificationAttempts: 2,
		ProductVersion:       "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func readFixtureSnapshot() dcs.Snapshot {
	target := model.Target{Context: "lab", Namespace: "/service", Scope: "alpha"}.Normalize()
	entries := []dcs.Entry{
		{RelativePath: "initialize", ModRevision: 2, Value: []byte("741852963")},
		{RelativePath: "config", ModRevision: 3, Value: []byte(`{"pause":true,"synchronous_mode":true}`)},
		{RelativePath: "leader", ModRevision: 4, Lease: 41, Value: []byte("node-a")},
		{RelativePath: "sync", ModRevision: 5, Value: []byte(`{"leader":"node-a","sync_standby":"node-b"}`)},
		{RelativePath: "status", ModRevision: 6, Value: []byte(`{"optime":50331648}`)},
		{RelativePath: "history", ModRevision: 7, Value: []byte(`[[2,16777216,"no recovery target specified","2026-07-13T10:00:00Z","node-a"],[3,33554432,"manual"]]`)},
		{RelativePath: "members/node-c", ModRevision: 10, Value: []byte(`{"api_url":"https://node-c:8008/patroni","conn_url":"postgres://node-c:5434/postgres","state":"running","replication_state":"streaming","timeline":3,"receive_lsn":33554432,"replay_lsn":16777216,"tags":{"replicatefrom":"node-b"},"version":"4.1.0"}`)},
		{RelativePath: "members/node-a", ModRevision: 8, Value: []byte(`{"api_url":"https://node-a:8008/patroni","conn_url":"postgres://test-only-user:test-only-placeholder@node-a:5433/postgres","state":"running","role":"primary","timeline":3,"pending_restart":true,"pending_restart_reason":{"max_connections":{"old_value":"100","new_value":"200"}},"version":"4.1.0"}`)},
		{RelativePath: "members/node-b", ModRevision: 9, Value: []byte(`{"api_url":"https://node-b:8008/patroni","conn_url":"postgres://node-b:5432/postgres","state":"running","replication_state":"streaming","timeline":3,"xlog_location":33554432,"tags":{},"version":"4.1.0"}`)},
	}
	return dcs.BuildSnapshot(target, "/service/alpha", 11, entries)
}

func snapshotWithConfig(t *testing.T, snapshot dcs.Snapshot, configuration map[string]any) dcs.Snapshot {
	t.Helper()
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]dcs.Entry, len(snapshot.Entries))
	copy(entries, snapshot.Entries)
	for index := range entries {
		if entries[index].RelativePath == "config" {
			entries[index].Value = encoded
			entries[index].ModRevision++
		}
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func snapshotWithScheduledRestart(t *testing.T, snapshot dcs.Snapshot, memberName, schedule string) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, len(snapshot.Entries))
	copy(entries, snapshot.Entries)
	for index := range entries {
		if entries[index].RelativePath != "members/"+memberName {
			continue
		}
		var member map[string]any
		if err := json.Unmarshal(entries[index].Value, &member); err != nil {
			t.Fatal(err)
		}
		member["scheduled_restart"] = map[string]any{"schedule": schedule}
		encoded, err := json.Marshal(member)
		if err != nil {
			t.Fatal(err)
		}
		entries[index].Value = encoded
		entries[index].ModRevision++
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func snapshotWithLeader(t *testing.T, snapshot dcs.Snapshot, leader string) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, len(snapshot.Entries))
	copy(entries, snapshot.Entries)
	for index := range entries {
		if entries[index].RelativePath == "leader" {
			entries[index].Value = []byte(leader)
			entries[index].ModRevision++
		}
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func snapshotWithMemberTag(t *testing.T, snapshot dcs.Snapshot, memberName, tag string, value any) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, len(snapshot.Entries))
	copy(entries, snapshot.Entries)
	for index := range entries {
		if entries[index].RelativePath != "members/"+memberName {
			continue
		}
		var member map[string]any
		if err := json.Unmarshal(entries[index].Value, &member); err != nil {
			t.Fatal(err)
		}
		tags, _ := member["tags"].(map[string]any)
		if tags == nil {
			tags = map[string]any{}
		}
		tags[tag] = value
		member["tags"] = tags
		encoded, err := json.Marshal(member)
		if err != nil {
			t.Fatal(err)
		}
		entries[index].Value = encoded
		entries[index].ModRevision++
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func snapshotWithFailoverValue(t *testing.T, snapshot dcs.Snapshot, value string, revision int64) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, 0, len(snapshot.Entries)+1)
	for _, entry := range snapshot.Entries {
		if entry.RelativePath != "failover" {
			entries = append(entries, entry)
		}
	}
	if value != "" {
		entries = append(entries, dcs.Entry{RelativePath: "failover", ModRevision: revision, Value: []byte(value)})
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, max(snapshot.Revision+1, revision), entries)
}

func snapshotWithPendingRestart(t *testing.T, snapshot dcs.Snapshot, memberName string, pending bool) dcs.Snapshot {
	t.Helper()
	entries := make([]dcs.Entry, len(snapshot.Entries))
	copy(entries, snapshot.Entries)
	for index := range entries {
		if entries[index].RelativePath != "members/"+memberName {
			continue
		}
		var member map[string]any
		if err := json.Unmarshal(entries[index].Value, &member); err != nil {
			t.Fatal(err)
		}
		member["pending_restart"] = pending
		encoded, err := json.Marshal(member)
		if err != nil {
			t.Fatal(err)
		}
		entries[index].Value = encoded
		entries[index].ModRevision++
	}
	return dcs.BuildSnapshot(snapshot.Target, snapshot.Prefix, snapshot.Revision+1, entries)
}

func TestReadServiceListProjectsPatroniClusterDeterministically(t *testing.T) {
	fixture := readFixtureSnapshot()
	snapshots := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}
	service := newReadService(t, snapshots, &fakePatroniStatusReader{})

	result := service.List(context.Background(), ListRequest{Targets: []model.Target{{Context: "lab", Scope: "alpha"}}})
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid result: %v", err)
	}
	if result.Outcome != Succeeded || result.Path != PathDCS || len(result.Data.Clusters) != 1 {
		t.Fatalf("unexpected list result: %#v", result)
	}
	cluster := result.Data.Clusters[0]
	if cluster.Revision != 11 || cluster.Leader != "node-a" || !cluster.Paused || cluster.Initialize != "741852963" {
		t.Fatalf("cluster projection mismatch: %#v", cluster)
	}
	if got := []string{cluster.Members[0].Name, cluster.Members[1].Name, cluster.Members[2].Name}; !reflect.DeepEqual(got, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("member order = %v", got)
	}
	if cluster.Members[0].Role != model.RoleLeader || cluster.Members[1].Role != model.RoleSyncStandby || cluster.Members[2].Role != model.RoleReplica {
		t.Fatalf("member roles = %#v", cluster.Members)
	}
	if cluster.Members[0].PendingRestartReason["max_connections"] == nil {
		t.Fatalf("pending restart reason missing: %#v", cluster.Members[0])
	}
	if cluster.Members[1].ReceiveLSN != "0/2000000" || cluster.Members[1].ReceiveLagBytes != 16777216 {
		t.Fatalf("legacy LSN projection = %#v", cluster.Members[1])
	}
	if cluster.Members[2].ReceiveLSN != "0/2000000" || cluster.Members[2].ReplayLSN != "0/1000000" || cluster.Members[2].ReplayLagBytes != 33554432 {
		t.Fatalf("receive/replay projection = %#v", cluster.Members[2])
	}
	if len(snapshots.calls) != 1 || snapshots.calls[0].Namespace != model.DefaultNamespace {
		t.Fatalf("snapshot calls = %#v", snapshots.calls)
	}
}

func TestReadServiceDSNMatchesMemberSelectionAndNeverReturnsCredentials(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	target := model.Target{Context: "lab", Scope: "alpha"}

	leader := service.DSN(context.Background(), DSNRequest{Target: target})
	if leader.Outcome != Succeeded || leader.Data.Member != "node-a" || leader.Data.Host != "node-a" || leader.Data.Port != 5433 {
		t.Fatalf("default leader DSN = %#v", leader)
	}
	if got := leader.Data.String(); got != "host=node-a port=5433" {
		t.Fatalf("DSN string = %q", got)
	}
	replica := service.DSN(context.Background(), DSNRequest{Target: target, Role: RoleReplica})
	if replica.Outcome != Succeeded || replica.Data.Member != "node-b" {
		t.Fatalf("replica DSN = %#v", replica)
	}
	invalid := service.DSN(context.Background(), DSNRequest{Target: target, Role: RoleAny, Member: "node-b"})
	if invalid.Outcome != Failed || invalid.Error == nil || invalid.Error.Category != CategoryUsage {
		t.Fatalf("mutually-exclusive selection = %#v", invalid)
	}
	missing := service.DSN(context.Background(), DSNRequest{Target: target, Member: "missing"})
	if missing.Outcome != Failed || missing.Error == nil || missing.Error.Category != CategoryNotFound {
		t.Fatalf("missing member = %#v", missing)
	}
}

func TestReadServiceTopologyUsesReplicateFromTree(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	result := service.Topology(context.Background(), TopologyRequest{Target: model.Target{Context: "lab", Scope: "alpha"}})
	if result.Outcome != Succeeded {
		t.Fatalf("topology failed: %#v", result)
	}
	got := make([]string, 0, len(result.Data.Members))
	for _, member := range result.Data.Members {
		got = append(got, member.Member.Name+":"+member.Parent+":"+string(rune('0'+member.Depth)))
	}
	want := []string{"node-a::0", "node-b:node-a:1", "node-c:node-b:2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("topology = %v, want %v", got, want)
	}
}

func TestReadServiceShowConfigAndHistoryAreNormalizedCopies(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	target := model.Target{Context: "lab", Scope: "alpha"}

	configuration := service.ShowConfig(context.Background(), ShowConfigRequest{Target: target})
	if configuration.Outcome != Succeeded || configuration.Data.Revision != 3 || configuration.Data.Config["pause"] != true {
		t.Fatalf("show config = %#v", configuration)
	}
	configuration.Data.Config["pause"] = false
	if fixture.Cluster.Config["pause"] != true {
		t.Fatal("show config leaked mutable DCS state")
	}

	history := service.History(context.Background(), HistoryRequest{Target: target})
	if history.Outcome != Succeeded || len(history.Data.Entries) != 2 {
		t.Fatalf("history = %#v", history)
	}
	if history.Data.Entries[0].Timeline != 2 || history.Data.Entries[0].NewLeader != "node-a" || history.Data.Entries[1].Timestamp != "" {
		t.Fatalf("normalized history = %#v", history.Data.Entries)
	}
}

func TestReadServiceVersionIsolatesPerMemberRESTFailure(t *testing.T) {
	fixture := readFixtureSnapshot()
	serverVersion := 160004
	status := &fakePatroniStatusReader{
		responses: map[string]patroni.Response[patroni.Status]{
			"https://node-a:8008": {StatusCode: 200, Data: patroni.Status{ServerVersion: &serverVersion, Patroni: patroni.PatroniIdentity{Version: "4.1.0"}}},
			"https://node-c:8008": {StatusCode: 503},
		},
		errors: map[string]error{"https://node-b:8008": errors.New("unreachable")},
	}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, status)
	result := service.Version(context.Background(), VersionRequest{Target: model.Target{Context: "lab", Scope: "alpha"}})
	if result.Outcome != Succeeded || result.Data.ProductVersion != "v0.1.0-test" || len(result.Data.Members) != 3 {
		t.Fatalf("version result = %#v", result)
	}
	if result.Data.Members[0].PatroniVersion != "4.1.0" || result.Data.Members[0].PostgresVersion != "16.4" {
		t.Fatalf("healthy version = %#v", result.Data.Members[0])
	}
	if result.Data.Members[1].Error == nil || result.Data.Members[2].Error == nil {
		t.Fatalf("member failures were not isolated: %#v", result.Data.Members)
	}
	if !reflect.DeepEqual(status.calls, []string{"https://node-a:8008", "https://node-b:8008", "https://node-c:8008"}) {
		t.Fatalf("REST order = %v", status.calls)
	}
}

func TestReadServiceMapsSnapshotFailureAndCancellation(t *testing.T) {
	reader := &fakeSnapshotReader{err: dcs.NewError(dcs.ErrorTransport, "snapshot", "/service/alpha", errors.New("dial secret.example"))}
	service := newReadService(t, reader, &fakePatroniStatusReader{})
	result := service.List(context.Background(), ListRequest{Targets: []model.Target{{Scope: "alpha"}}})
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUnreachable || !result.Error.Retryable {
		t.Fatalf("snapshot failure = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid failed result: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	reader.err = context.Canceled
	result = service.List(canceled, ListRequest{Targets: []model.Target{{Scope: "alpha"}}})
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryFailed || result.Error.Retryable {
		t.Fatalf("canceled result = %#v", result)
	}
}

func TestReadServiceQueryResolvesMemberAndChecksNonLeaderRole(t *testing.T) {
	fixture := readFixtureSnapshot()
	executor := &fakePostgresQueryExecutor{result: postgres.QueryResult{
		Sets: []postgres.ResultSet{{Columns: []postgres.Column{{Name: "value"}}, Rows: []postgres.Row{{{Text: "ok"}}}, CommandTag: "SELECT 1"}},
	}}
	service := newReadServiceWithQuery(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{}, executor)
	connection := postgres.NewConnectionOptions("").WithTLSMode(postgres.TLSDisable)
	connection.Username = "operator"
	result := service.Query(context.Background(), QueryRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Role: RoleReplica,
		SQL: "select protected", Connection: connection,
	})
	if result.Outcome != Succeeded || result.Data.Member != "node-b" || len(result.Data.Result.Sets) != 1 {
		t.Fatalf("query result = %#v", result)
	}
	if len(executor.connections) != 1 || executor.connections[0].Host != "node-b" || executor.connections[0].Port != 5432 ||
		executor.connections[0].Username != "operator" || executor.expected[0] != postgres.RecoveryStandby {
		t.Fatalf("resolved query connection = %#v expectation=%v", executor.connections, executor.expected)
	}
	if executor.requests[0].SQL != "select protected" {
		t.Fatal("control did not pass SQL only to the PostgreSQL transport")
	}
}

func TestReadServiceQueryPreservesPatronictlResultRowErrorSemantics(t *testing.T) {
	fixture := readFixtureSnapshot()
	executor := &fakePostgresQueryExecutor{err: &postgres.Error{Kind: postgres.ErrorDatabase, Stage: "query", SQLState: "42P01"}}
	service := newReadServiceWithQuery(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{}, executor)
	target := model.Target{Context: "lab", Scope: "alpha"}
	result := service.Query(context.Background(), QueryRequest{Target: target, Member: "node-a", SQL: "select private"})
	if result.Outcome != Succeeded || result.Error != nil || result.Data.Error == nil || result.Data.Error.SQLState != "42P01" {
		t.Fatalf("database error compatibility result = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid compatible query result: %v", err)
	}

	missing := service.Query(context.Background(), QueryRequest{Target: target, Member: "missing", SQL: "select private"})
	if missing.Outcome != Succeeded || missing.Data.Error == nil || missing.Data.Error.Kind != QueryErrorNoConnection || len(executor.connections) != 1 {
		t.Fatalf("missing query member = %#v", missing)
	}
}

func TestControlQueryRequestFormattingAndJSONRedactSQLAndConnection(t *testing.T) {
	const protected = "__BOAR_TEST_ONLY_CONTROL_QUERY_DETAIL__"
	connection := postgres.NewConnectionOptions("postgres://user:placeholder@" + protected + "/db").WithPassword("placeholder")
	request := QueryRequest{
		Target: model.Target{Scope: "alpha"}, Member: "node-a", Connection: connection,
		SQL: "select '" + protected + "'", Limits: postgres.Limits{MaxRows: 10},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, rendered := range []string{request.String(), fmt.Sprintf("%#v", request), string(encoded)} {
		if strings.Contains(rendered, protected) || strings.Contains(rendered, "placeholder") {
			t.Fatalf("control query request leaked SQL or connection detail: %s", rendered)
		}
	}
}

func TestReloadPrepareBuildsCompleteDeterministicPlan(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	prepared := service.PrepareReload(context.Background(), ReloadRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Role: RoleAny,
	})
	if prepared.Outcome != Succeeded || prepared.Data.Operation != "reload" || prepared.Data.Risk != RiskAdminWrite ||
		prepared.Data.Path != PathREST || prepared.Data.RetrySafety != UnsafeAfterSend {
		t.Fatalf("reload plan = %#v", prepared)
	}
	if err := prepared.Data.Validate(); err != nil {
		t.Fatalf("invalid reload plan: %v", err)
	}
	got := make([]string, 0, len(prepared.Data.Targets))
	for _, target := range prepared.Data.Targets {
		got = append(got, target.Member)
	}
	if !reflect.DeepEqual(got, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("planned members = %v", got)
	}
	if len(prepared.Data.Preconditions) == 0 || prepared.Data.Preconditions[0].Source != EvidenceDCS {
		t.Fatalf("reload preconditions = %#v", prepared.Data.Preconditions)
	}
}

func TestReloadExecuteClassifiesAcceptedAndDefiniteHTTPFailure(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{reloadResponses: map[string]patroni.Response[string]{
		"https://node-a:8008": {StatusCode: 200},
		"https://node-b:8008": {StatusCode: 202},
		"https://node-c:8008": {StatusCode: 500},
	}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Role: RoleAny}
	prepared := service.PrepareReload(context.Background(), request)
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryFailed || len(result.Data.Members) != 3 {
		t.Fatalf("reload batch result = %#v", result)
	}
	if result.Data.Members[0].Outcome != Succeeded || result.Data.Members[0].SendState != SendAccepted ||
		result.Data.Members[1].Outcome != Succeeded || result.Data.Members[2].Outcome != Failed || result.Data.Members[2].HTTPStatus != 500 {
		t.Fatalf("reload member results = %#v", result.Data.Members)
	}
	if !reflect.DeepEqual(transport.reloadCalls, []string{"https://node-a:8008", "https://node-b:8008", "https://node-c:8008"}) {
		t.Fatalf("reload call order/retry = %v", transport.reloadCalls)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid reload result: %v", err)
	}
}

func TestReloadAmbiguousTransportIsUnknownAndNeverRetried(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{reloadErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/reload", Delivery: patroni.DeliveryMaybeSent},
	}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny}
	prepared := service.PrepareReload(context.Background(), request)
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown ||
		len(result.Data.Members) != 1 || result.Data.Members[0].Outcome != Unknown || result.Data.Members[0].SendState != SendMaybeSent {
		t.Fatalf("ambiguous reload = %#v", result)
	}
	if len(transport.reloadCalls) != 1 {
		t.Fatalf("ambiguous reload retried %d times", len(transport.reloadCalls))
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid UNKNOWN reload result: %v", err)
	}

	transport.reloadErrors["https://node-a:8008"] = errors.New("transport omitted delivery metadata")
	transport.reloadCalls = nil
	result = service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Data.Members[0].SendState != SendMaybeSent || len(transport.reloadCalls) != 1 {
		t.Fatalf("untyped ambiguous reload = %#v calls=%v", result, transport.reloadCalls)
	}
}

func TestReloadNotSentIsDefiniteAndCancellationStopsSubsequentWrites(t *testing.T) {
	fixture := readFixtureSnapshot()
	notSent := &fakePatroniStatusReader{reloadErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/reload", Delivery: patroni.DeliveryNotSent},
	}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, notSent)
	request := ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny}
	prepared := service.PrepareReload(context.Background(), request)
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].Outcome != Failed || result.Data.Members[0].SendState != SendNotSent {
		t.Fatalf("not-sent reload = %#v", result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceling := &fakePatroniStatusReader{reloadResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 200}}}
	canceling.reloadHook = func(baseURL string) {
		if baseURL == "https://node-a:8008" {
			cancel()
		}
	}
	service = newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, canceling)
	request = ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Role: RoleAny}
	prepared = service.PrepareReload(context.Background(), request)
	result = service.ExecuteReload(ctx, request, prepared.Data)
	if result.Outcome != Failed || len(canceling.reloadCalls) != 1 || len(result.Data.Members) != 3 ||
		result.Data.Members[0].Outcome != Succeeded || result.Data.Members[1].SendState != SendNotSent || result.Data.Members[2].SendState != SendNotSent {
		t.Fatalf("canceled reload = result=%#v calls=%v", result, canceling.reloadCalls)
	}
}

func TestReloadFreshSnapshotDetectsConcurrentMembershipChangeBeforeWrite(t *testing.T) {
	fixture := readFixtureSnapshot()
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}
	transport := &fakePatroniStatusReader{}
	service := newReadService(t, reader, transport)
	request := ReloadRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-b"}, Role: RoleReplica}
	prepared := service.PrepareReload(context.Background(), request)
	changedEntries := make([]dcs.Entry, 0, len(fixture.Entries))
	for _, entry := range fixture.Entries {
		if entry.RelativePath != "members/node-b" {
			changedEntries = append(changedEntries, entry)
		}
	}
	reader.snapshots["alpha"] = dcs.BuildSnapshot(fixture.Target, fixture.Prefix, fixture.Revision+1, changedEntries)
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].Error == nil || result.Data.Members[0].Error.Category != CategoryConflict || len(transport.reloadCalls) != 0 {
		t.Fatalf("concurrent membership result = %#v calls=%v", result, transport.reloadCalls)
	}

	forged := prepared.Data
	forged.Operation = "restart"
	result = service.ExecuteReload(context.Background(), request, forged)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.reloadCalls) != 0 {
		t.Fatalf("forged reload plan = %#v", result)
	}

	forged = prepared.Data
	forged.Preconditions = forged.Preconditions[:1]
	result = service.ExecuteReload(context.Background(), request, forged)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.reloadCalls) != 0 {
		t.Fatalf("reload plan without selectors = %#v", result)
	}

	result = service.ExecuteReload(context.Background(), request, Plan{})
	if result.Outcome != Failed || result.OperationID == "" {
		t.Fatalf("zero reload plan did not produce a valid failure: %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("zero-plan failure is invalid: %v", err)
	}
}

func TestRestartPrepareFiltersPendingValidatesConditionsAndFreezesAnySelection(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	target := model.Target{Context: "lab", Scope: "alpha"}
	pending := service.PrepareRestart(context.Background(), RestartRequest{
		Target: target, Role: RoleAny, Pending: true, PostgresVersion: "16.4", Timeout: "10s",
	})
	if pending.Outcome != Succeeded || len(pending.Data.Targets) != 1 || pending.Data.Targets[0].Member != "node-a" ||
		pending.Data.Risk != RiskAvailability || pending.Data.RetrySafety != UnsafeAfterSend {
		t.Fatalf("pending restart plan = %#v", pending)
	}
	any := service.PrepareRestart(context.Background(), RestartRequest{Target: target, Role: RoleAny, Any: true})
	if any.Outcome != Succeeded || len(any.Data.Targets) != 1 || any.Data.Targets[0].Member != "node-c" {
		t.Fatalf("--any restart plan = %#v", any)
	}
	invalidVersion := service.PrepareRestart(context.Background(), RestartRequest{Target: target, PostgresVersion: "9.6"})
	if invalidVersion.Outcome != Failed || invalidVersion.Error == nil || invalidVersion.Error.Category != CategoryUsage {
		t.Fatalf("invalid PostgreSQL version = %#v", invalidVersion)
	}
	invalidTimeout := service.PrepareRestart(context.Background(), RestartRequest{Target: target, Timeout: "0s"})
	if invalidTimeout.Outcome != Failed || invalidTimeout.Error == nil || invalidTimeout.Error.Category != CategoryUsage {
		t.Fatalf("invalid restart timeout = %#v", invalidTimeout)
	}
	scheduled := fixedControlTime.Add(time.Hour)
	paused := service.PrepareRestart(context.Background(), RestartRequest{Target: target, ScheduledAt: &scheduled})
	if paused.Outcome != Failed || paused.Error == nil || paused.Error.Category != CategoryConflict {
		t.Fatalf("scheduled restart in paused cluster = %#v", paused)
	}
}

func TestRestartImmediateSuccessAndDefiniteConflict(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{restartResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 200}}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := RestartRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny,
		Pending: true, PostgresVersion: "16.4", Timeout: "10s",
	}
	prepared := service.PrepareRestart(context.Background(), request)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(result.Data.Members) != 1 || result.Data.Members[0].HTTPStatus != 200 {
		t.Fatalf("immediate restart = %#v", result)
	}
	if len(transport.restartRequests) != 1 || transport.restartRequests[0].PostgresVersion != "16.4" ||
		transport.restartRequests[0].Timeout != "10s" || transport.restartRequests[0].RestartPending == nil || !*transport.restartRequests[0].RestartPending {
		t.Fatalf("restart payload = %#v", transport.restartRequests)
	}

	transport.restartResponses["https://node-a:8008"] = patroni.Response[string]{StatusCode: 409}
	transport.restartCalls = nil
	transport.restartRequests = nil
	result = service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].Outcome != Failed || result.Data.Members[0].Error.Category != CategoryConflict || len(transport.restartCalls) != 1 {
		t.Fatalf("restart conflict = %#v", result)
	}
}

func TestRestartImmediateAmbiguousAndNotSentAreNeverRetried(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{restartErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/restart", Delivery: patroni.DeliveryMaybeSent},
	}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := RestartRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny}
	prepared := service.PrepareRestart(context.Background(), request)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Data.Members[0].SendState != SendMaybeSent || len(transport.restartCalls) != 1 {
		t.Fatalf("ambiguous immediate restart = %#v calls=%v", result, transport.restartCalls)
	}
	transport.restartErrors["https://node-a:8008"] = &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/restart", Delivery: patroni.DeliveryNotSent}
	transport.restartCalls = nil
	result = service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].SendState != SendNotSent || len(transport.restartCalls) != 1 {
		t.Fatalf("not-sent immediate restart = %#v calls=%v", result, transport.restartCalls)
	}
}

func TestRestartDetectsPendingConcurrencyAndSelectorTamperingBeforeWrite(t *testing.T) {
	fixture := readFixtureSnapshot()
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}
	transport := &fakePatroniStatusReader{}
	service := newReadService(t, reader, transport)
	request := RestartRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny, Pending: true,
	}
	prepared := service.PrepareRestart(context.Background(), request)
	reader.snapshots["alpha"] = snapshotWithPendingRestart(t, fixture, "node-a", false)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].Error == nil || result.Data.Members[0].Error.Category != CategoryConflict || len(transport.restartCalls) != 0 {
		t.Fatalf("pending restart concurrency = %#v calls=%v", result, transport.restartCalls)
	}

	reader.snapshots["alpha"] = fixture
	tamperedRequest := request
	tamperedRequest.Timeout = "11s"
	result = service.ExecuteRestart(context.Background(), tamperedRequest, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.restartCalls) != 0 {
		t.Fatalf("restart selector tampering = %#v", result)
	}
}

func TestRestartScheduledReadAfterWriteResolvesAcceptedAndAmbiguousSend(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false, "synchronous_mode": true})
	scheduledAt := fixedControlTime.Add(time.Hour)
	schedule := formatPatroniTimestamp(scheduledAt)
	confirmed := snapshotWithScheduledRestart(t, base, "node-a", schedule)
	transport := &fakePatroniStatusReader{restartResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 202}}}
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}, sequence: []dcs.Snapshot{base, base, confirmed}}
	service := newReadService(t, reader, transport)
	request := RestartRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny, ScheduledAt: &scheduledAt}
	prepared := service.PrepareRestart(context.Background(), request)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Data.Members[0].Verification != VerifiedSucceeded || len(reader.calls) != 3 {
		t.Fatalf("verified scheduled restart = %#v calls=%d", result, len(reader.calls))
	}

	transport.restartResponses = nil
	transport.restartErrors = map[string]error{"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/restart", Delivery: patroni.DeliveryMaybeSent}}
	transport.restartCalls = nil
	reader.calls = nil
	reader.sequence = []dcs.Snapshot{base, base, confirmed}
	prepared = service.PrepareRestart(context.Background(), request)
	result = service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Data.Members[0].SendState != SendMaybeSent || result.Data.Members[0].Verification != VerifiedSucceeded || len(transport.restartCalls) != 1 {
		t.Fatalf("evidence-resolved ambiguous scheduled restart = %#v", result)
	}

	reader.calls = nil
	reader.sequence = []dcs.Snapshot{base, base, base, base}
	prepared = service.PrepareRestart(context.Background(), request)
	result = service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Data.Members[0].Outcome != Unknown || result.Data.Members[0].SendState != SendMaybeSent || len(transport.restartCalls) != 2 {
		t.Fatalf("unverified ambiguous scheduled restart = %#v", result)
	}
}

func TestRestartForcedReplacementStopsAfterAmbiguousFlush(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	scheduledAt := fixedControlTime.Add(time.Hour)
	base = snapshotWithScheduledRestart(t, base, "node-a", fixedControlTime.Add(30*time.Minute).Format(time.RFC3339))
	transport := &fakePatroniStatusReader{
		deleteRestartErrors: map[string]error{"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/restart", Delivery: patroni.DeliveryMaybeSent}},
	}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}, transport)
	request := RestartRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny,
		ScheduledAt: &scheduledAt, Force: true,
	}
	prepared := service.PrepareRestart(context.Background(), request)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || len(transport.deleteRestartCalls) != 1 || len(transport.restartCalls) != 0 || result.Data.Members[0].SendState != SendMaybeSent {
		t.Fatalf("ambiguous forced flush = result=%#v delete=%v post=%v", result, transport.deleteRestartCalls, transport.restartCalls)
	}
}

func TestRestartForcedReplacementContinuesAfterDefiniteFlushResponse(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	scheduledAt := fixedControlTime.Add(time.Hour)
	schedule := formatPatroniTimestamp(scheduledAt)
	base = snapshotWithScheduledRestart(t, base, "node-a", fixedControlTime.Add(30*time.Minute).Format(time.RFC3339))
	confirmed := snapshotWithScheduledRestart(t, base, "node-a", schedule)
	transport := &fakePatroniStatusReader{
		deleteRestartResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 404}},
		restartResponses:       map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 202}},
	}
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}, sequence: []dcs.Snapshot{base, base, confirmed}}
	service := newReadService(t, reader, transport)
	request := RestartRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-a"}, Role: RoleAny,
		ScheduledAt: &scheduledAt, Force: true,
	}
	prepared := service.PrepareRestart(context.Background(), request)
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(transport.deleteRestartCalls) != 1 || len(transport.restartCalls) != 1 || len(result.Data.Members[0].Evidence) < 3 {
		t.Fatalf("definite flush continuation = result=%#v delete=%v post=%v", result, transport.deleteRestartCalls, transport.restartCalls)
	}

	transport.deleteRestartResponses = nil
	transport.deleteRestartErrors = map[string]error{"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "DELETE", Endpoint: "/restart", Delivery: patroni.DeliveryNotSent}}
	transport.deleteRestartCalls = nil
	transport.restartCalls = nil
	reader.sequence = []dcs.Snapshot{base, base}
	prepared = service.PrepareRestart(context.Background(), request)
	result = service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || len(transport.deleteRestartCalls) != 1 || len(transport.restartCalls) != 0 || result.Data.Members[0].SendState != SendNotSent {
		t.Fatalf("not-sent flush stop = result=%#v delete=%v post=%v", result, transport.deleteRestartCalls, transport.restartCalls)
	}
}

func TestReinitializePrepareIsReplicaOnlyAndPreservesForceNoop(t *testing.T) {
	fixture := readFixtureSnapshot()
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, &fakePatroniStatusReader{})
	target := model.Target{Context: "lab", Scope: "alpha"}
	prepared := service.PrepareReinitialize(context.Background(), ReinitializeRequest{
		Target: target, Members: []string{"node-b"}, FromLeader: true, Wait: true,
	})
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 1 || prepared.Data.Targets[0].Member != "node-b" || prepared.Data.Risk != RiskDestructive {
		t.Fatalf("replica reinitialize plan = %#v", prepared)
	}
	leader := service.PrepareReinitialize(context.Background(), ReinitializeRequest{Target: target, Members: []string{"node-a"}})
	if leader.Outcome != Failed || leader.Error == nil || leader.Error.Category != CategoryNotFound {
		t.Fatalf("leader reinitialize selection = %#v", leader)
	}
	missingSelection := service.PrepareReinitialize(context.Background(), ReinitializeRequest{Target: target})
	if missingSelection.Outcome != Failed || missingSelection.Error == nil || missingSelection.Error.Category != CategoryUsage {
		t.Fatalf("attended reinitialize without member = %#v", missingSelection)
	}
	forceNoop := service.PrepareReinitialize(context.Background(), ReinitializeRequest{Target: target, Force: true})
	if forceNoop.Outcome != Succeeded || len(forceNoop.Data.Targets) != 0 {
		t.Fatalf("forced no-member compatibility plan = %#v", forceNoop)
	}
	result := service.ExecuteReinitialize(context.Background(), ReinitializeRequest{Target: target, Force: true}, forceNoop.Data)
	if result.Outcome != Succeeded || len(result.Data.Members) != 0 {
		t.Fatalf("forced no-member compatibility result = %#v", result)
	}

	leaderOnlyEntries := make([]dcs.Entry, 0, len(fixture.Entries))
	for _, entry := range fixture.Entries {
		if strings.HasPrefix(entry.RelativePath, "members/") && entry.RelativePath != "members/node-a" {
			continue
		}
		leaderOnlyEntries = append(leaderOnlyEntries, entry)
	}
	leaderOnly := dcs.BuildSnapshot(fixture.Target, fixture.Prefix, fixture.Revision+1, leaderOnlyEntries)
	noReplicaService := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": leaderOnly}}, &fakePatroniStatusReader{})
	noReplica := noReplicaService.PrepareReinitialize(context.Background(), ReinitializeRequest{Target: target, Force: true})
	if noReplica.Outcome != Failed || noReplica.Error == nil || noReplica.Error.Category != CategoryNotFound {
		t.Fatalf("forced reinitialize without replica candidates = %#v", noReplica)
	}
}

func TestReinitializeSuccessFailureUnknownAndNoRetry(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{reinitializeResponses: map[string]patroni.Response[string]{"https://node-b:8008": {StatusCode: 200}}}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := ReinitializeRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-b"}, Force: true, FromLeader: true,
	}
	prepared := service.PrepareReinitialize(context.Background(), request)
	result := service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(transport.reinitializeRequests) != 1 || !transport.reinitializeRequests[0].Force || !transport.reinitializeRequests[0].FromLeader {
		t.Fatalf("successful reinitialize = %#v payload=%#v", result, transport.reinitializeRequests)
	}

	transport.reinitializeCalls = nil
	tamperedRequest := request
	tamperedRequest.Force = false
	result = service.ExecuteReinitialize(context.Background(), tamperedRequest, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryUsage || len(transport.reinitializeCalls) != 0 {
		t.Fatalf("reinitialize force-plan tampering = %#v calls=%v", result, transport.reinitializeCalls)
	}

	transport.reinitializeResponses["https://node-b:8008"] = patroni.Response[string]{StatusCode: 503, Data: "reinitialize already in progress"}
	transport.reinitializeCalls = nil
	result = service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].HTTPStatus != 503 || len(transport.reinitializeCalls) != 1 {
		t.Fatalf("reinitialize rejection = %#v", result)
	}

	transport.reinitializeResponses = nil
	transport.reinitializeErrors = map[string]error{"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/reinitialize", Delivery: patroni.DeliveryMaybeSent}}
	transport.reinitializeCalls = nil
	result = service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Data.Members[0].SendState != SendMaybeSent || len(transport.reinitializeCalls) != 1 {
		t.Fatalf("ambiguous reinitialize = %#v calls=%v", result, transport.reinitializeCalls)
	}
}

func TestReinitializeWaitUsesPatroniEvidenceToResolveOutcome(t *testing.T) {
	fixture := readFixtureSnapshot()
	transport := &fakePatroniStatusReader{
		reinitializeResponses: map[string]patroni.Response[string]{"https://node-b:8008": {StatusCode: 200}},
		patroniSequences: map[string][]patroni.Response[patroni.Status]{
			"https://node-b:8008": {
				{StatusCode: 200, Data: patroni.Status{State: "creating replica"}},
				{StatusCode: 200, Data: patroni.Status{State: "running"}},
			},
		},
		patroniErrorSequences: map[string][]error{},
	}
	service := newReadService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}, transport)
	request := ReinitializeRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-b"}, Wait: true}
	prepared := service.PrepareReinitialize(context.Background(), request)
	result := service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Data.Members[0].Verification != VerifiedSucceeded || len(transport.calls) != 2 {
		t.Fatalf("waited reinitialize = %#v statusCalls=%v", result, transport.calls)
	}

	transport.reinitializeResponses = nil
	transport.reinitializeErrors = map[string]error{"https://node-b:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/reinitialize", Delivery: patroni.DeliveryMaybeSent}}
	transport.reinitializeCalls = nil
	transport.calls = nil
	transport.patroniSequences["https://node-b:8008"] = []patroni.Response[patroni.Status]{
		{StatusCode: 200, Data: patroni.Status{State: "creating replica"}},
		{StatusCode: 200, Data: patroni.Status{State: "running"}},
	}
	result = service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Data.Members[0].SendState != SendMaybeSent || len(transport.reinitializeCalls) != 1 {
		t.Fatalf("evidence-resolved ambiguous reinitialize = %#v", result)
	}

	transport.calls = nil
	transport.patroniSequences["https://node-b:8008"] = []patroni.Response[patroni.Status]{
		{StatusCode: 200, Data: patroni.Status{State: "creating replica"}},
		{StatusCode: 200, Data: patroni.Status{State: "creating replica"}},
	}
	result = service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || len(transport.calls) != 2 {
		t.Fatalf("incomplete waited reinitialize = %#v", result)
	}
}

func TestReinitializeDetectsReplicaPromotionBeforeWrite(t *testing.T) {
	fixture := readFixtureSnapshot()
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": fixture}}
	transport := &fakePatroniStatusReader{}
	service := newReadService(t, reader, transport)
	request := ReinitializeRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Members: []string{"node-b"}}
	prepared := service.PrepareReinitialize(context.Background(), request)
	entries := make([]dcs.Entry, len(fixture.Entries))
	copy(entries, fixture.Entries)
	for index := range entries {
		if entries[index].RelativePath == "leader" {
			entries[index].Value = []byte("node-b")
			entries[index].ModRevision++
		}
	}
	reader.snapshots["alpha"] = dcs.BuildSnapshot(fixture.Target, fixture.Prefix, fixture.Revision+1, entries)
	result := service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.Members[0].Error == nil || result.Data.Members[0].Error.Category != CategoryConflict || len(transport.reinitializeCalls) != 0 {
		t.Fatalf("replica promotion concurrency = %#v calls=%v", result, transport.reinitializeCalls)
	}
}

func TestFailoverAndSwitchoverPrepareFreezePatronictlSelection(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false, "synchronous_mode": true})
	target := model.Target{Context: "lab", Scope: "alpha"}
	service := newTransitionService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}, &fakePatroniStatusReader{}, &fakeFailoverCAS{})

	failoverRequest := FailoverRequest{Target: target, Candidate: "node-b"}
	prepared := service.PrepareFailover(context.Background(), failoverRequest)
	if prepared.Outcome != Succeeded || prepared.Data.Risk != RiskAvailability || prepared.Data.Path != PathREST {
		t.Fatalf("failover plan = %#v", prepared)
	}
	if syncEligible, ok := expectedPrecondition(prepared.Data, "candidate.syncEligible"); !ok || syncEligible != "true" {
		t.Fatalf("sync candidate precondition = %q exists=%t plan=%#v", syncEligible, ok, prepared.Data)
	}
	async := service.PrepareFailover(context.Background(), FailoverRequest{Target: target, Candidate: "node-c"})
	if async.Outcome != Succeeded {
		t.Fatalf("asynchronous candidate must produce a confirmable plan: %#v", async)
	}
	if syncEligible, ok := expectedPrecondition(async.Data, "candidate.syncEligible"); !ok || syncEligible != "false" {
		t.Fatalf("async candidate evidence = %q exists=%t", syncEligible, ok)
	}
	for _, request := range []FailoverRequest{{Target: target}, {Target: target, Force: true}} {
		missing := service.PrepareFailover(context.Background(), request)
		if missing.Outcome != Failed || missing.Error == nil || missing.Error.Category != CategoryUsage {
			t.Fatalf("failover without candidate = %#v", missing)
		}
	}
	leaderCandidate := service.PrepareFailover(context.Background(), FailoverRequest{Target: target, Candidate: "node-a"})
	if leaderCandidate.Outcome != Failed || leaderCandidate.Error == nil || leaderCandidate.Error.Category != CategoryConflict {
		t.Fatalf("leader as failover candidate = %#v", leaderCandidate)
	}

	noFailover := snapshotWithMemberTag(t, base, "node-b", "nofailover", true)
	service = newTransitionService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": noFailover}}, &fakePatroniStatusReader{}, &fakeFailoverCAS{})
	rejected := service.PrepareFailover(context.Background(), failoverRequest)
	if rejected.Outcome != Failed || rejected.Error == nil || rejected.Error.Category != CategoryNotFound {
		t.Fatalf("nofailover candidate = %#v", rejected)
	}

	service = newTransitionService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}, &fakePatroniStatusReader{}, &fakeFailoverCAS{})
	citus := service.PrepareFailover(context.Background(), FailoverRequest{Target: target, Candidate: "node-b", Citus: true})
	if citus.Outcome != Failed || citus.Error == nil || citus.Error.Category != CategoryUsage {
		t.Fatalf("Citus failover without group = %#v", citus)
	}

	attended := service.PrepareSwitchover(context.Background(), SwitchoverRequest{Target: target})
	if attended.Outcome != Failed || attended.Error == nil || attended.Error.Category != CategoryUsage {
		t.Fatalf("attended switchover without prompted selections = %#v", attended)
	}
	forced := service.PrepareSwitchover(context.Background(), SwitchoverRequest{Target: target, Force: true})
	if forced.Outcome != Succeeded {
		t.Fatalf("forced switchover without candidate = %#v", forced)
	}
	if leader, _ := expectedPrecondition(forced.Data, "leader.name"); leader != "node-a" {
		t.Fatalf("forced switchover leader = %q", leader)
	}
	wrongLeader := service.PrepareSwitchover(context.Background(), SwitchoverRequest{Target: target, Leader: "node-b", Candidate: "node-c"})
	if wrongLeader.Outcome != Failed || wrongLeader.Error == nil || wrongLeader.Error.Category != CategoryConflict {
		t.Fatalf("wrong switchover leader = %#v", wrongLeader)
	}

	paused := snapshotWithConfig(t, base, map[string]any{"pause": true, "synchronous_mode": true})
	service = newTransitionService(t, &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": paused}}, &fakePatroniStatusReader{}, &fakeFailoverCAS{})
	scheduled := fixedControlTime.Add(time.Hour)
	pausedResult := service.PrepareSwitchover(context.Background(), SwitchoverRequest{
		Target: target, Leader: "node-a", Candidate: "node-b", ScheduledAt: &scheduled, Force: true,
	})
	if pausedResult.Outcome != Failed || pausedResult.Error == nil || pausedResult.Error.Category != CategoryConflict {
		t.Fatalf("scheduled switchover in pause = %#v", pausedResult)
	}
}

func TestFailoverRESTSuccessDefiniteFailureAndConcurrency(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	transport := &fakePatroniStatusReader{failoverResponses: map[string]patroni.Response[string]{
		"https://node-a:8008": {StatusCode: 200, Data: "Successfully failed over"},
	}}
	writer := &fakeFailoverCAS{}
	service := newTransitionService(t, reader, transport, writer)
	request := FailoverRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Candidate: "node-b", Force: true}
	prepared := service.PrepareFailover(context.Background(), request)
	tampered := request
	tampered.Candidate = "node-c"
	tamperedResult := service.ExecuteFailover(context.Background(), tampered, prepared.Data)
	if tamperedResult.Outcome != Failed || tamperedResult.Error == nil || tamperedResult.Error.Category != CategoryUsage || len(transport.failoverCalls) != 0 {
		t.Fatalf("failover Plan/request tampering = %#v calls=%v", tamperedResult, transport.failoverCalls)
	}
	result := service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Path != PathREST || result.Data.RESTSendState != SendAccepted ||
		len(transport.failoverCalls) != 1 || transport.failoverCalls[0] != "https://node-a:8008" || len(writer.writeCalls) != 0 {
		t.Fatalf("REST failover success = result=%#v calls=%v writes=%#v", result, transport.failoverCalls, writer.writeCalls)
	}
	if got := transport.failoverRequests[0]; got.Candidate != "node-b" || got.Leader != "" || got.Member != "" || got.ScheduledAt != "" {
		t.Fatalf("REST failover payload = %#v", got)
	}

	transport.failoverResponses["https://node-a:8008"] = patroni.Response[string]{StatusCode: 500, Data: "rejected"}
	transport.failoverCalls = nil
	result = service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Data.HTTPStatus != 500 || len(transport.failoverCalls) != 1 || len(writer.writeCalls) != 0 {
		t.Fatalf("definite REST rejection must not fallback = %#v", result)
	}

	transport.failoverCalls = nil
	reader.snapshots["alpha"] = snapshotWithLeader(t, base, "node-c")
	result = service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(transport.failoverCalls) != 0 {
		t.Fatalf("leader drift before failover = %#v calls=%v", result, transport.failoverCalls)
	}
}

func TestSwitchoverUsesLegacyEndpointOnlyForExact501Contract(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	transport := &fakePatroniStatusReader{
		switchoverResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 501, Data: "Server does not support this operation"}},
		failoverResponses:   map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 200, Data: "legacy success"}},
	}
	service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
	request := SwitchoverRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Leader: "node-a", Candidate: "node-b", Force: true,
	}
	prepared := service.PrepareSwitchover(context.Background(), request)
	result := service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Path != PathREST || !result.Data.LegacyEndpoint || len(transport.switchoverCalls) != 1 || len(transport.failoverCalls) != 1 {
		t.Fatalf("legacy switchover = %#v switchover=%v failover=%v", result, transport.switchoverCalls, transport.failoverCalls)
	}
	if got := transport.failoverRequests[0]; got.Leader != "node-a" || got.Candidate != "node-b" {
		t.Fatalf("legacy payload = %#v", got)
	}

	transport.switchoverResponses["https://node-a:8008"] = patroni.Response[string]{StatusCode: 501, Data: "different 501"}
	transport.switchoverCalls = nil
	transport.failoverCalls = nil
	result = service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || len(transport.switchoverCalls) != 1 || len(transport.failoverCalls) != 0 {
		t.Fatalf("non-contract 501 = %#v switchover=%v failover=%v", result, transport.switchoverCalls, transport.failoverCalls)
	}

	transport.switchoverResponses["https://node-a:8008"] = patroni.Response[string]{StatusCode: 501, Data: "Server does not support this operation"}
	transport.failoverResponses = nil
	transport.failoverErrors = map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/failover", Delivery: patroni.DeliveryMaybeSent},
	}
	transport.switchoverCalls = nil
	transport.failoverCalls = nil
	writer := &fakeFailoverCAS{writeResult: dcs.WriteResult{Applied: true, Revision: 38}}
	writer.writeHook = func(call failoverWriteCall) {
		reader.snapshots["alpha"] = snapshotWithFailoverValue(t, base, string(call.Value), 38)
	}
	service = newTransitionService(t, reader, transport, writer)
	prepared = service.PrepareSwitchover(context.Background(), request)
	result = service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Path != PathRESTToDCS || !result.Data.LegacyEndpoint ||
		len(transport.switchoverCalls) != 1 || len(transport.failoverCalls) != 1 || len(writer.writeCalls) != 1 {
		t.Fatalf("legacy transport fallback chain = result=%#v switchover=%v failover=%v writes=%#v",
			result, transport.switchoverCalls, transport.failoverCalls, writer.writeCalls)
	}
}

func TestScheduledSwitchoverRESTAcceptanceRequiresDCSReadback(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	scheduled := fixedControlTime.Add(time.Hour)
	expected := `{"leader":"node-a","member":"node-b","scheduled_at":"` + formatPatroniTimestamp(scheduled) + `"}`
	transport := &fakePatroniStatusReader{
		switchoverResponses: map[string]patroni.Response[string]{"https://node-a:8008": {StatusCode: 202, Data: "Switchover scheduled"}},
	}
	transport.switchoverHook = func(string) {
		reader.snapshots["alpha"] = snapshotWithFailoverValue(t, base, expected, 37)
	}
	service := newTransitionService(t, reader, transport, &fakeFailoverCAS{})
	request := SwitchoverRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Leader: "node-a", Candidate: "node-b", ScheduledAt: &scheduled, Force: true,
	}
	prepared := service.PrepareSwitchover(context.Background(), request)
	result := service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Data.Verification != VerifiedSucceeded || result.Data.DCSRevision < 37 {
		t.Fatalf("verified scheduled switchover = %#v", result)
	}

	reader.snapshots["alpha"] = base
	transport.switchoverHook = nil
	result = service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown {
		t.Fatalf("unverified scheduled switchover acceptance = %#v", result)
	}
}

func TestFailoverTransportExceptionFallsBackToExactDCSCAS(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	transport := &fakePatroniStatusReader{failoverErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/failover", Delivery: patroni.DeliveryMaybeSent},
	}}
	writer := &fakeFailoverCAS{writeResult: dcs.WriteResult{Applied: true, Revision: 31}}
	writer.writeHook = func(call failoverWriteCall) {
		reader.snapshots["alpha"] = snapshotWithFailoverValue(t, base, string(call.Value), 31)
	}
	service := newTransitionService(t, reader, transport, writer)
	request := FailoverRequest{Target: model.Target{Context: "lab", Scope: "alpha"}, Candidate: "node-b", Force: true}
	prepared := service.PrepareFailover(context.Background(), request)
	result := service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Path != PathRESTToDCS || result.Data.RESTSendState != SendMaybeSent ||
		result.Data.DCSSendState != SendAccepted || result.Data.Verification != VerifiedSucceeded || len(writer.writeCalls) != 1 {
		t.Fatalf("MAYBE_SENT failover fallback = %#v writes=%#v", result, writer.writeCalls)
	}
	call := writer.writeCalls[0]
	if string(call.Value) != `{"member":"node-b"}` || call.ExpectedRevision == nil || *call.ExpectedRevision != 0 {
		t.Fatalf("failover fallback CAS = value=%s expected=%v", call.Value, call.ExpectedRevision)
	}
	if err := result.Data.Validate(); err != nil {
		t.Fatalf("fallback data validation: %v", err)
	}

	existing := snapshotWithFailoverValue(t, base, `{"member":"node-c"}`, 29)
	reader.snapshots["alpha"] = existing
	writer.writeCalls = nil
	writer.writeHook = func(call failoverWriteCall) {
		reader.snapshots["alpha"] = snapshotWithFailoverValue(t, existing, string(call.Value), 31)
	}
	preparedExisting := service.PrepareFailover(context.Background(), request)
	result = service.ExecuteFailover(context.Background(), request, preparedExisting.Data)
	if result.Outcome != Succeeded || len(writer.writeCalls) != 1 || writer.writeCalls[0].ExpectedRevision == nil || *writer.writeCalls[0].ExpectedRevision != 29 {
		t.Fatalf("fallback CAS over existing failover key = result=%#v writes=%#v", result, writer.writeCalls)
	}

	reader.snapshots["alpha"] = base
	writer.writeCalls = nil
	writer.writeHook = nil
	transport.failoverHook = func(string) { reader.snapshots["alpha"] = snapshotWithLeader(t, base, "node-b") }
	result = service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || result.Path != PathRESTToDCS || result.Data.Verification != VerifiedSucceeded || len(writer.writeCalls) != 0 {
		t.Fatalf("ambiguous REST resolved by leader evidence = result=%#v writes=%#v", result, writer.writeCalls)
	}
	transport.failoverHook = nil

	reader.snapshots["alpha"] = base
	writer.writeCalls = nil
	writer.writeHook = nil
	writer.writeResult = dcs.WriteResult{}
	writer.writeError = &dcs.ConflictError{Key: "/service/alpha/failover", ExpectedRevision: 0, ObservedRevision: 44}
	result = service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Unknown || result.Error == nil || result.Error.Category != CategoryUnknown || len(writer.writeCalls) != 1 {
		t.Fatalf("ambiguous REST plus DCS conflict = %#v", result)
	}

	transport.failoverErrors["https://node-a:8008"] = &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/failover", Delivery: patroni.DeliveryNotSent}
	writer.writeCalls = nil
	result = service.ExecuteFailover(context.Background(), request, prepared.Data)
	if result.Outcome != Failed || result.Error == nil || result.Error.Category != CategoryConflict || len(writer.writeCalls) != 1 {
		t.Fatalf("not-sent REST plus definite DCS conflict = %#v", result)
	}
}

func TestScheduledSwitchoverFallbackAndCancellation(t *testing.T) {
	base := snapshotWithConfig(t, readFixtureSnapshot(), map[string]any{"pause": false})
	reader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": base}}
	scheduled := fixedControlTime.Add(time.Hour)
	transport := &fakePatroniStatusReader{switchoverErrors: map[string]error{
		"https://node-a:8008": &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/switchover", Delivery: patroni.DeliveryNotSent},
	}}
	writer := &fakeFailoverCAS{writeResult: dcs.WriteResult{Applied: true, Revision: 41}}
	writer.writeHook = func(call failoverWriteCall) {
		reader.snapshots["alpha"] = snapshotWithFailoverValue(t, base, string(call.Value), 41)
	}
	service := newTransitionService(t, reader, transport, writer)
	request := SwitchoverRequest{
		Target: model.Target{Context: "lab", Scope: "alpha"}, Leader: "node-a", Candidate: "node-b", ScheduledAt: &scheduled, Force: true,
	}
	prepared := service.PrepareSwitchover(context.Background(), request)
	result := service.ExecuteSwitchover(context.Background(), request, prepared.Data)
	expected := `{"leader":"node-a","member":"node-b","scheduled_at":"` + formatPatroniTimestamp(scheduled) + `"}`
	if result.Outcome != Succeeded || result.Path != PathRESTToDCS || len(writer.writeCalls) != 1 || string(writer.writeCalls[0].Value) != expected {
		t.Fatalf("scheduled switchover fallback = result=%#v value=%s", result, writer.writeCalls[0].Value)
	}

	reader.snapshots["alpha"] = base
	writer.writeCalls = nil
	writer.writeHook = nil
	transport.switchoverErrors["https://node-a:8008"] = &patroni.Error{Kind: patroni.ErrorTransport, Method: "POST", Endpoint: "/switchover", Delivery: patroni.DeliveryMaybeSent}
	ctx, cancel := context.WithCancel(context.Background())
	transport.switchoverCalls = nil
	transport.switchoverResponses = nil
	// The REST call owns no retry; cancellation after it returns prevents the DCS fallback write.
	transport.switchoverHook = func(string) { cancel() }
	result = service.ExecuteSwitchover(ctx, request, prepared.Data)
	if result.Outcome != Unknown || len(writer.writeCalls) != 0 || len(transport.switchoverCalls) != 1 {
		t.Fatalf("canceled switchover fallback = %#v calls=%v writes=%#v", result, transport.switchoverCalls, writer.writeCalls)
	}
}
