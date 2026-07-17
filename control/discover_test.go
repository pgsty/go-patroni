package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
)

type fakeDiscoverer struct {
	clusters []dcs.DiscoveredCluster
	err      error
	calls    []dcs.DiscoveryRequest
}

func (discoverer *fakeDiscoverer) Discover(ctx context.Context, request dcs.DiscoveryRequest) ([]dcs.DiscoveredCluster, error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	discoverer.calls = append(discoverer.calls, request)
	return append([]dcs.DiscoveredCluster(nil), discoverer.clusters...), discoverer.err
}

func TestDiscoverListAllAndTopologyAllUseOneNamespaceSnapshot(t *testing.T) {
	alpha := readFixtureSnapshot()
	beta := discoveredFixtureSnapshot("beta", 23, "beta-1", "beta-1", "4.0.7")
	discoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{
		{Target: beta.Target, Revision: beta.Revision, EvidenceKeys: []string{"/service/beta/leader", "/service/beta/members/beta-1"}, Snapshot: &beta},
		{Target: alpha.Target, Revision: alpha.Revision, EvidenceKeys: []string{"/service/alpha/config", "/service/alpha/members/node-a"}, Snapshot: &alpha},
	}}
	snapshots := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}
	service := newDiscoveryService(t, snapshots, discoverer)

	discovered := service.Discover(context.Background(), DiscoverRequest{Context: "lab", Namespace: "//service/"})
	if discovered.Outcome != Succeeded || len(discovered.Data.Clusters) != 2 || len(discoverer.calls) != 1 || len(snapshots.calls) != 0 {
		t.Fatalf("discover did not use exactly one namespace snapshot: result=%#v discoverCalls=%v snapshotCalls=%v",
			discovered, discoverer.calls, snapshots.calls)
	}
	if err := discovered.Validate(); err != nil {
		t.Fatalf("discover result invalid: %v", err)
	}
	alphaSummary, betaSummary := discovered.Data.Clusters[0], discovered.Data.Clusters[1]
	if alphaSummary.Target.Scope != "alpha" || alphaSummary.MemberCount != 3 || alphaSummary.Leader != "node-a" ||
		alphaSummary.DiscoveryState != model.DiscoveryDiscovered || alphaSummary.ManagementState != model.ManagementUnmanaged ||
		alphaSummary.ReachabilityState != model.ReachabilityUnknown || alphaSummary.HealthState != model.HealthUnknown ||
		betaSummary.Target.Scope != "beta" || betaSummary.MemberCount != 1 {
		t.Fatalf("discovery summary/state axes mismatch: %#v", discovered.Data.Clusters)
	}

	listed := service.ListAll(context.Background(), ListAllRequest{Context: "lab", Namespace: "/service"})
	if listed.Outcome != Succeeded || len(listed.Data.Clusters) != 2 || len(discoverer.calls) != 2 || len(snapshots.calls) != 0 {
		t.Fatalf("list-all performed N+1 reads: result=%#v discoverCalls=%v snapshotCalls=%v", listed, discoverer.calls, snapshots.calls)
	}
	if err := listed.Validate(); err != nil {
		t.Fatalf("list-all result invalid: %v", err)
	}
	for _, cluster := range listed.Data.Clusters {
		if cluster.DiscoveryState != model.DiscoveryDiscovered || cluster.ManagementState != model.ManagementAllSelected ||
			cluster.ReachabilityState != model.ReachabilityUnknown || cluster.HealthState != model.HealthUnknown {
			t.Fatalf("list-all conflated state axes: %#v", cluster)
		}
	}

	topologies := service.TopologyAll(context.Background(), TopologyAllRequest{Context: "lab", Namespace: "/service"})
	if topologies.Outcome != Succeeded || len(topologies.Data.Topologies) != 2 || len(discoverer.calls) != 3 || len(snapshots.calls) != 0 {
		t.Fatalf("topology-all performed N+1 reads: result=%#v discoverCalls=%v snapshotCalls=%v", topologies, discoverer.calls, snapshots.calls)
	}
	if err := topologies.Validate(); err != nil {
		t.Fatalf("topology-all result invalid: %v", err)
	}
	if topologies.Data.Topologies[0].Cluster.Target.Scope != "alpha" || topologies.Data.Topologies[1].Cluster.Target.Scope != "beta" {
		t.Fatalf("topology-all order is not deterministic: %#v", topologies.Data.Topologies)
	}
}

func TestListExpandsGroupLessCitusScopeInsideControl(t *testing.T) {
	coordinator := discoveredGroupFixtureSnapshot("citus", 0, 31, "coordinator")
	worker := discoveredGroupFixtureSnapshot("citus", 1, 32, "worker")
	other := discoveredFixtureSnapshot("ordinary", 33, "other", "other", "4.1.0")
	discoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{
		{Target: other.Target, Revision: other.Revision, Snapshot: &other},
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}}
	snapshots := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}
	service := newDiscoveryService(t, snapshots, discoverer)

	result := service.List(context.Background(), ListRequest{
		Targets: []model.Target{{Context: "lab", Namespace: "/service", Scope: "citus"}}, Citus: true,
	})
	if result.Outcome != Succeeded || len(result.Data.Clusters) != 2 || len(discoverer.calls) != 1 || len(snapshots.calls) != 0 {
		t.Fatalf("group-less Citus list did not use one namespace snapshot: result=%#v discover=%v snapshots=%v", result, discoverer.calls, snapshots.calls)
	}
	for index, cluster := range result.Data.Clusters {
		if cluster.Target.Group == nil || *cluster.Target.Group != index || cluster.Target.Scope != "citus" ||
			cluster.DiscoveryState != model.DiscoveryDiscovered || cluster.ManagementState != model.ManagementExplicit || len(cluster.Members) != 1 {
			t.Fatalf("Citus group projection mismatch at %d: %#v", index, cluster)
		}
	}
	if result.Data.Clusters[0].Leader != "coordinator" || result.Data.Clusters[1].Leader != "worker" {
		t.Fatalf("Citus group ordering/leader mismatch: %#v", result.Data.Clusters)
	}
	topologies := service.TopologyGroups(context.Background(), TopologyGroupsRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"},
	})
	if topologies.Outcome != Succeeded || len(topologies.Data.Topologies) != 2 || topologies.Data.Topologies[0].Cluster.Target.Group == nil ||
		*topologies.Data.Topologies[0].Cluster.Target.Group != 0 || topologies.Data.Topologies[1].Cluster.Target.Group == nil ||
		*topologies.Data.Topologies[1].Cluster.Target.Group != 1 {
		t.Fatalf("Citus topology expansion mismatch: %#v", topologies)
	}
	defaultDSN := service.DSN(context.Background(), DSNRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Citus: true,
	})
	workerDSN := service.DSN(context.Background(), DSNRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Member: "worker", Citus: true,
	})
	if defaultDSN.Outcome != Succeeded || defaultDSN.Data.Target.Group == nil || *defaultDSN.Data.Target.Group != 0 || defaultDSN.Data.Member != "coordinator" ||
		workerDSN.Outcome != Succeeded || workerDSN.Data.Target.Group == nil || *workerDSN.Data.Target.Group != 1 || workerDSN.Data.Member != "worker" {
		t.Fatalf("Citus DSN cross-group selection mismatch: default=%#v worker=%#v", defaultDSN, workerDSN)
	}

	executor := &fakePostgresQueryExecutor{result: postgres.QueryResult{Sets: []postgres.ResultSet{}}}
	queryService, err := NewService(ServiceOptions{Snapshots: snapshots, Discovery: discoverer, Postgres: executor})
	if err != nil {
		t.Fatal(err)
	}
	queried := queryService.Query(context.Background(), QueryRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Member: "worker", Citus: true, SQL: "select 1",
	})
	if queried.Outcome != Succeeded || queried.Data.Target.Group == nil || *queried.Data.Target.Group != 1 || queried.Data.Member != "worker" ||
		len(executor.connections) != 1 || executor.connections[0].Host != "worker" {
		t.Fatalf("Citus query cross-group selection mismatch: result=%#v connections=%#v", queried, executor.connections)
	}

	coordinatorWithoutDCSVersion := snapshotWithPatroniVersion(t, coordinator, "")
	workerWithoutDCSVersion := snapshotWithPatroniVersion(t, worker, "")
	versionDiscoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &workerWithoutDCSVersion},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinatorWithoutDCSVersion},
	}}
	serverVersion := 170010
	versionTransport := &fakePatroniStatusReader{responses: map[string]patroni.Response[patroni.Status]{
		"https://coordinator:8008": {StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "4.1.0"}, ServerVersion: &serverVersion}},
		"https://worker:8008":      {StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "4.1.0"}, ServerVersion: &serverVersion}},
	}}
	versionService, err := NewService(ServiceOptions{Snapshots: snapshots, Discovery: versionDiscoverer, Patroni: versionTransport})
	if err != nil {
		t.Fatal(err)
	}
	versioned := versionService.Version(context.Background(), VersionRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Citus: true,
	})
	if versioned.Outcome != Succeeded || len(versioned.Data.Members) != 2 || versioned.Data.Members[0].Target.Group == nil ||
		*versioned.Data.Members[0].Target.Group != 0 || versioned.Data.Members[1].Target.Group == nil ||
		*versioned.Data.Members[1].Target.Group != 1 || !reflect.DeepEqual(versionTransport.calls, []string{"https://coordinator:8008", "https://worker:8008"}) {
		t.Fatalf("Citus version group expansion/REST probe mismatch: result=%#v calls=%v", versioned, versionTransport.calls)
	}

	missing := newDiscoveryService(t, &fakeSnapshotReader{}, &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{{
		Target: other.Target, Revision: other.Revision, Snapshot: &other,
	}}}).List(context.Background(), ListRequest{
		Targets: []model.Target{{Context: "lab", Namespace: "/service", Scope: "citus"}}, Citus: true,
	})
	if missing.Outcome != Failed || missing.Error == nil || missing.Error.Category != CategoryNotFound {
		t.Fatalf("missing Citus groups were not typed NOT_FOUND: %#v", missing)
	}
}

func TestReloadExpandsAndRevalidatesGroupLessCitusScope(t *testing.T) {
	coordinator := discoveredGroupFixtureSnapshot("citus", 0, 41, "coordinator")
	worker := discoveredGroupFixtureSnapshot("citus", 1, 42, "worker")
	items := []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}
	discoverer := &fakeDiscoverer{clusters: items}
	transport := &fakePatroniStatusReader{reloadResponses: map[string]patroni.Response[string]{
		"https://coordinator:8008": {StatusCode: 200},
		"https://worker:8008":      {StatusCode: 200},
	}}
	service, err := NewService(ServiceOptions{
		Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}, Discovery: discoverer, Patroni: transport,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-reload-operation" },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ReloadRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Role: RoleAny, Citus: true,
	}
	prepared := service.PrepareReload(context.Background(), request)
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 2 || prepared.Data.Targets[0].Group == nil ||
		*prepared.Data.Targets[0].Group != 0 || prepared.Data.Targets[1].Group == nil || *prepared.Data.Targets[1].Group != 1 {
		t.Fatalf("Citus reload plan mismatch: %#v", prepared)
	}
	if groups, ok := expectedPrecondition(prepared.Data, "citus.groups"); !ok || groups != "0,1" {
		t.Fatalf("Citus group inventory was not frozen: %#v", prepared.Data.Preconditions)
	}
	result := service.ExecuteReload(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(result.Data.Members) != 2 ||
		!reflect.DeepEqual(transport.reloadCalls, []string{"https://coordinator:8008", "https://worker:8008"}) {
		t.Fatalf("Citus all-group reload mismatch: result=%#v calls=%v", result, transport.reloadCalls)
	}
	for index, member := range result.Data.Members {
		if member.Target.Group == nil || *member.Target.Group != index || member.SendState != SendAccepted {
			t.Fatalf("Citus reload result target mismatch at %d: %#v", index, member)
		}
	}

	transport.reloadCalls = nil
	discoverer.clusters = items[:1]
	changed := service.ExecuteReload(context.Background(), request, prepared.Data)
	if changed.Outcome != Failed || changed.Error == nil || changed.Error.Category != CategoryConflict || len(transport.reloadCalls) != 0 {
		t.Fatalf("Citus group inventory drift was not rejected before send: result=%#v calls=%v", changed, transport.reloadCalls)
	}
}

func TestRestartExpandsAndRevalidatesGroupLessCitusScope(t *testing.T) {
	coordinator := discoveredGroupFixtureSnapshot("citus", 0, 51, "coordinator")
	worker := discoveredGroupFixtureSnapshot("citus", 1, 52, "worker")
	items := []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}
	discoverer := &fakeDiscoverer{clusters: items}
	transport := &fakePatroniStatusReader{restartResponses: map[string]patroni.Response[string]{
		"https://coordinator:8008": {StatusCode: 200},
		"https://worker:8008":      {StatusCode: 200},
	}}
	service, err := NewService(ServiceOptions{
		Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}, Discovery: discoverer, Patroni: transport,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-restart-operation" },
		RandomIndex: func(length int) (int, error) { return length - 1, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := RestartRequest{
		Target: model.Target{Context: "lab", Namespace: "/service", Scope: "citus"}, Role: RoleAny, Force: true, Citus: true,
	}
	prepared := service.PrepareRestart(context.Background(), request)
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 2 || prepared.Data.Targets[0].Group == nil ||
		*prepared.Data.Targets[0].Group != 0 || prepared.Data.Targets[1].Group == nil || *prepared.Data.Targets[1].Group != 1 {
		t.Fatalf("Citus restart plan mismatch: %#v", prepared)
	}
	if groups, ok := expectedPrecondition(prepared.Data, "citus.groups"); !ok || groups != "0,1" {
		t.Fatalf("Citus restart group inventory was not frozen: %#v", prepared.Data.Preconditions)
	}
	result := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(result.Data.Members) != 2 ||
		!reflect.DeepEqual(transport.restartCalls, []string{"https://coordinator:8008", "https://worker:8008"}) {
		t.Fatalf("Citus all-group restart mismatch: result=%#v calls=%v", result, transport.restartCalls)
	}
	for index, member := range result.Data.Members {
		if member.Target.Group == nil || *member.Target.Group != index || member.SendState != SendAccepted {
			t.Fatalf("Citus restart result target mismatch at %d: %#v", index, member)
		}
	}

	transport.restartCalls = nil
	discoverer.clusters = items[:1]
	changed := service.ExecuteRestart(context.Background(), request, prepared.Data)
	if changed.Outcome != Failed || changed.Error == nil || changed.Error.Category != CategoryConflict || len(transport.restartCalls) != 0 {
		t.Fatalf("Citus restart group drift was not rejected before send: result=%#v calls=%v", changed, transport.restartCalls)
	}

	discoverer.clusters = items
	anyRequest := request
	anyRequest.Any = true
	any := service.PrepareRestart(context.Background(), anyRequest)
	if any.Outcome != Succeeded || len(any.Data.Targets) != 1 || any.Data.Targets[0].Group == nil ||
		*any.Data.Targets[0].Group != 1 || any.Data.Targets[0].Member != "worker" {
		t.Fatalf("Citus restart --any was not selected once across all groups: %#v", any)
	}
}

func TestReinitializeExpandsAndRevalidatesGroupLessCitusScope(t *testing.T) {
	coordinator := discoveredGroupReplicaFixtureSnapshot("citus", 0, 61, "coordinator", "coordinator-replica")
	worker := discoveredGroupReplicaFixtureSnapshot("citus", 1, 62, "worker", "worker-replica")
	items := []dcs.DiscoveredCluster{
		{Target: worker.Target, Revision: worker.Revision, Snapshot: &worker},
		{Target: coordinator.Target, Revision: coordinator.Revision, Snapshot: &coordinator},
	}
	discoverer := &fakeDiscoverer{clusters: items}
	transport := &fakePatroniStatusReader{reinitializeResponses: map[string]patroni.Response[string]{
		"https://coordinator-replica:8008": {StatusCode: 200},
		"https://worker-replica:8008":      {StatusCode: 200},
	}}
	service, err := NewService(ServiceOptions{
		Snapshots: &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}, Discovery: discoverer, Patroni: transport,
		Clock: func() time.Time { return fixedControlTime }, NewOperationID: func() string { return "citus-reinit-operation" },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ReinitializeRequest{
		Target:  model.Target{Context: "lab", Namespace: "/service", Scope: "citus"},
		Members: []string{"coordinator-replica", "worker-replica"}, Force: true, Citus: true,
	}
	prepared := service.PrepareReinitialize(context.Background(), request)
	if prepared.Outcome != Succeeded || len(prepared.Data.Targets) != 2 || prepared.Data.Targets[0].Group == nil ||
		*prepared.Data.Targets[0].Group != 0 || prepared.Data.Targets[1].Group == nil || *prepared.Data.Targets[1].Group != 1 {
		t.Fatalf("Citus reinitialize plan mismatch: %#v", prepared)
	}
	if groups, ok := expectedPrecondition(prepared.Data, "citus.groups"); !ok || groups != "0,1" {
		t.Fatalf("Citus reinitialize group inventory was not frozen: %#v", prepared.Data.Preconditions)
	}
	result := service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if result.Outcome != Succeeded || len(result.Data.Members) != 2 ||
		!reflect.DeepEqual(transport.reinitializeCalls, []string{"https://coordinator-replica:8008", "https://worker-replica:8008"}) {
		t.Fatalf("Citus all-group reinitialize mismatch: result=%#v calls=%v", result, transport.reinitializeCalls)
	}

	transport.reinitializeCalls = nil
	discoverer.clusters = items[:1]
	changed := service.ExecuteReinitialize(context.Background(), request, prepared.Data)
	if changed.Outcome != Failed || changed.Error == nil || changed.Error.Category != CategoryConflict || len(transport.reinitializeCalls) != 0 {
		t.Fatalf("Citus reinitialize group drift was not rejected before send: result=%#v calls=%v", changed, transport.reinitializeCalls)
	}

	discoverer.clusters = items
	forcedNoop := service.PrepareReinitialize(context.Background(), ReinitializeRequest{
		Target: request.Target, Force: true, Citus: true,
	})
	if forcedNoop.Outcome != Succeeded || len(forcedNoop.Data.Targets) != 0 {
		t.Fatalf("forced group-less Citus reinitialize no-op mismatch: %#v", forcedNoop)
	}
}

func TestDiscoveryVersionPolicyAndLegacyDiscovererFallback(t *testing.T) {
	unsupported := discoveredFixtureSnapshot("unsupported", 31, "node-u", "node-u", "5.0.0")
	discoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{{
		Target: unsupported.Target, Revision: unsupported.Revision,
		EvidenceKeys: []string{"/service/unsupported/members/node-u"}, Snapshot: &unsupported,
	}}}
	snapshots := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{}}
	service := newDiscoveryService(t, snapshots, discoverer)

	blocked := service.Discover(context.Background(), DiscoverRequest{Context: "lab", Namespace: "/service"})
	if blocked.Outcome != Failed || blocked.Error == nil || blocked.Error.Category != CategoryUnsupported || len(snapshots.calls) != 0 {
		t.Fatalf("unsupported discovery was not fail-closed: %#v snapshotCalls=%v", blocked, snapshots.calls)
	}
	allowed := service.Discover(context.Background(), DiscoverRequest{
		Context: "lab", Namespace: "/service", AllowUnsupportedRead: true,
	})
	if allowed.Outcome != Succeeded || len(allowed.Data.Clusters) != 1 {
		t.Fatalf("explicit unsupported discovery opt-in failed: %#v", allowed)
	}

	legacySnapshot := discoveredFixtureSnapshot("legacy", 41, "node-l", "node-l", "4.1.0")
	legacyDiscoverer := &fakeDiscoverer{clusters: []dcs.DiscoveredCluster{{
		Target: legacySnapshot.Target, Revision: legacySnapshot.Revision,
		EvidenceKeys: []string{"/service/legacy/members/node-l"}, Snapshot: nil,
	}}}
	legacyReader := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"legacy": legacySnapshot}}
	legacyService := newDiscoveryService(t, legacyReader, legacyDiscoverer)
	legacy := legacyService.ListAll(context.Background(), ListAllRequest{Context: "lab", Namespace: "/service"})
	if legacy.Outcome != Succeeded || len(legacy.Data.Clusters) != 1 || len(legacyReader.calls) != 1 {
		t.Fatalf("legacy discoverer compatibility fallback mismatch: result=%#v calls=%v", legacy, legacyReader.calls)
	}
}

func TestDiscoveryErrorsAreTypedAndExplicitListDoesNotInferReachability(t *testing.T) {
	discoverer := &fakeDiscoverer{err: dcs.NewError(dcs.ErrorDeadline, "discover", "/service/", context.DeadlineExceeded)}
	snapshots := &fakeSnapshotReader{snapshots: map[string]dcs.Snapshot{"alpha": readFixtureSnapshot()}}
	service := newDiscoveryService(t, snapshots, discoverer)

	failed := service.Discover(context.Background(), DiscoverRequest{Context: "lab", Namespace: "/service"})
	if failed.Outcome != Failed || failed.Error == nil || failed.Error.Category != CategoryUnreachable || !failed.Error.Retryable {
		t.Fatalf("discovery deadline classification mismatch: %#v", failed)
	}
	if err := failed.Validate(); err != nil {
		t.Fatalf("failed discovery result invalid: %v", err)
	}

	discoverer.err = dcs.NewError(dcs.ErrorAuthentication, "discover", "/service/", errors.New("permission denied"))
	denied := service.Discover(context.Background(), DiscoverRequest{Context: "lab", Namespace: "/service"})
	if denied.Outcome != Failed || denied.Error == nil || denied.Error.Category != CategoryAuth || denied.Error.Retryable {
		t.Fatalf("discovery authentication classification mismatch: %#v", denied)
	}

	listed := service.List(context.Background(), ListRequest{Targets: []model.Target{{Context: "lab", Scope: "alpha"}}})
	if listed.Outcome != Succeeded || len(listed.Data.Clusters) != 1 {
		t.Fatalf("explicit list failed: %#v", listed)
	}
	cluster := listed.Data.Clusters[0]
	if cluster.ManagementState != model.ManagementExplicit || cluster.DiscoveryState != model.DiscoveryDiscovered ||
		cluster.ReachabilityState != model.ReachabilityUnknown || cluster.HealthState != model.HealthUnknown {
		t.Fatalf("explicit list inferred endpoint reachability/health from DCS: %#v", cluster)
	}
}

func newDiscoveryService(t *testing.T, snapshots *fakeSnapshotReader, discoverer *fakeDiscoverer) *Service {
	t.Helper()
	sequence := 0
	service, err := NewService(ServiceOptions{
		Snapshots: snapshots, Discovery: discoverer,
		Clock: func() time.Time { return fixedControlTime },
		NewOperationID: func() string {
			sequence++
			return fmt.Sprintf("discovery-operation-%d", sequence)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func discoveredFixtureSnapshot(scope string, revision int64, leader, member, version string) dcs.Snapshot {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: scope}).Normalize()
	memberValue, _ := json.Marshal(map[string]any{
		"api_url": "https://" + member + ":8008/patroni", "conn_url": "postgres://" + member + ":5432/postgres",
		"state": "running", "role": "primary", "version": version,
	})
	return dcs.BuildSnapshot(target, "/service/"+scope, revision, []dcs.Entry{
		{RelativePath: "config", ModRevision: revision - 2, Value: []byte(`{"ttl":30}`)},
		{RelativePath: "leader", ModRevision: revision - 1, Value: []byte(leader)},
		{RelativePath: "members/" + member, ModRevision: revision, Value: memberValue},
	})
}

func discoveredGroupFixtureSnapshot(scope string, group int, revision int64, member string) dcs.Snapshot {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: scope, Group: &group}).Normalize()
	memberValue, _ := json.Marshal(map[string]any{
		"api_url": "https://" + member + ":8008/patroni", "conn_url": "postgres://" + member + ":5432/postgres",
		"state": "running", "role": "primary", "version": "4.1.0",
	})
	prefix := fmt.Sprintf("/service/%s/%d", scope, group)
	return dcs.BuildSnapshot(target, prefix, revision, []dcs.Entry{
		{RelativePath: "config", ModRevision: revision - 2, Value: []byte(`{"ttl":30}`)},
		{RelativePath: "leader", ModRevision: revision - 1, Value: []byte(member)},
		{RelativePath: "members/" + member, ModRevision: revision, Value: memberValue},
	})
}

func discoveredGroupReplicaFixtureSnapshot(scope string, group int, revision int64, leader, replica string) dcs.Snapshot {
	base := discoveredGroupFixtureSnapshot(scope, group, revision, leader)
	replicaValue, _ := json.Marshal(map[string]any{
		"api_url": "https://" + replica + ":8008/patroni", "conn_url": "postgres://" + replica + ":5432/postgres",
		"state": "running", "role": "replica", "version": "4.1.0",
	})
	entries := append([]dcs.Entry(nil), base.Entries...)
	entries = append(entries, dcs.Entry{RelativePath: "members/" + replica, ModRevision: revision, Value: replicaValue})
	return dcs.BuildSnapshot(base.Target, base.Prefix, revision, entries)
}
