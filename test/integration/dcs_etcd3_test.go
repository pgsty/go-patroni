//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/dcs/etcd3"
	"github.com/pgsty/go-patroni/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var isolatedNamespace = regexp.MustCompile(`^go-patroni-test-[a-zA-Z0-9-]+$`)

type faultConfigCAS struct {
	delegate  dcs.ConfigCAS
	apply     bool
	ambiguous bool
	calls     int
}

type faultClusterRemover struct {
	delegate  dcs.ClusterRemover
	apply     bool
	ambiguous bool
	calls     int
}

func (store *faultClusterRemover) DeleteCluster(ctx context.Context, target model.Target) (dcs.RemoveResult, error) {
	store.calls++
	var result dcs.RemoveResult
	var err error
	if store.apply && store.delegate != nil {
		result, err = store.delegate.DeleteCluster(ctx, target)
		if err != nil {
			return result, err
		}
	}
	if store.ambiguous {
		return result, dcs.NewWriteError(dcs.ErrorTransport, "remove-cluster", "", dcs.DeliveryMaybeSent, errors.New("injected response loss"))
	}
	return result, err
}

func (store *faultConfigCAS) CompareAndSwapConfig(ctx context.Context, target model.Target, value []byte, expected *int64) (dcs.WriteResult, error) {
	store.calls++
	var result dcs.WriteResult
	var err error
	if store.apply && store.delegate != nil {
		result, err = store.delegate.CompareAndSwapConfig(ctx, target, value, expected)
		if err != nil {
			return result, err
		}
	}
	if store.ambiguous {
		return result, dcs.NewWriteError(dcs.ErrorTransport, "config-cas", "", dcs.DeliveryMaybeSent, errors.New("injected response loss"))
	}
	return result, err
}

func TestEtcd3PatroniSnapshotDiscoveryCASRemoveAndWatch(t *testing.T) {
	if os.Getenv("GO_PATRONI_TEST_ETCD_ISOLATED") != "1" {
		t.Fatal("refusing DCS integration test without GO_PATRONI_TEST_ETCD_ISOLATED=1")
	}
	endpoint := os.Getenv("GO_PATRONI_TEST_ETCD_ENDPOINT")
	namespace := os.Getenv("GO_PATRONI_TEST_ETCD_NAMESPACE")
	if endpoint == "" {
		t.Fatal("GO_PATRONI_TEST_ETCD_ENDPOINT is required")
	}
	if !isolatedNamespace.MatchString(namespace) {
		t.Fatalf("refusing destructive fixture cleanup for unsafe namespace %q", namespace)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	receivedFailover := make(chan []byte, 4)
	receivedFlush := make(chan struct{}, 4)
	ambiguousREST := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if request.Method == http.MethodDelete && request.URL.Path == "/switchover" {
			receivedFlush <- struct{}{}
		} else {
			receivedFailover <- body
		}
		connection, _, hijackError := writer.(http.Hijacker).Hijack()
		if hijackError != nil {
			http.Error(writer, "hijack failed", http.StatusInternalServerError)
			return
		}
		_ = connection.Close()
	}))
	defer ambiguousREST.Close()
	raw, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("close etcd fixture client: %v", closeErr)
		}
	})
	prefix := "/" + namespace + "/"
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, cleanupErr := raw.Delete(cleanupContext, prefix, clientv3.WithPrefix()); cleanupErr != nil {
			t.Errorf("clean isolated etcd namespace: %v", cleanupErr)
		}
	})

	store, err := etcd3.New(ctx, etcd3.Options{
		Endpoints:      []string{endpoint},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	lease, err := raw.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		key   string
		value string
		lease clientv3.LeaseID
	}{
		{prefix + "alpha/initialize", "sysid-alpha", 0},
		{prefix + "alpha/config", `{"ttl":30,"loop_wait":10}`, 0},
		{prefix + "alpha/members/node-a", fmt.Sprintf(`{"api_url":%q,"state":"running","role":"primary","version":"4.1.0"}`, ambiguousREST.URL+"/patroni"), lease.ID},
		{prefix + "alpha/members/node-b", `{"api_url":"http://127.0.0.1:1/patroni","state":"running","role":"replica","version":"4.1.0"}`, lease.ID},
		{prefix + "alpha/leader", "node-a", lease.ID},
		{prefix + "alpha/history", `[[1,42,"integration"]]`, 0},
		{prefix + "alpha/status", `{"optime":42,"retain_slots":["node-b"]}`, 0},
		{prefix + "alpha/optime/leader", `41`, 0},
		{prefix + "alpha/sync", `{"leader":"node-a","sync_standby":"node-b","quorum":1}`, 0},
		{prefix + "alpha/failsafe", `{"node-a":"http://node-a.invalid:8008/patroni"}`, 0},
		{prefix + "alphabet/config", `{}`, 0},
		{prefix + "beta/config", `{}`, 0},
		{prefix + "beta/members/node-b", `{"state":"running","role":"replica","version":"4.1.0"}`, lease.ID},
		{prefix + "gamma/0/config", `{}`, 0},
		{prefix + "gamma/0/members/coordinator", `{"state":"running","role":"primary","version":"4.1.0"}`, lease.ID},
		{prefix + "unrelated/future-key", `{"not":"cluster evidence"}`, 0},
	}
	for _, fixture := range fixtures {
		options := []clientv3.OpOption(nil)
		if fixture.lease != 0 {
			options = append(options, clientv3.WithLease(fixture.lease))
		}
		if _, err := raw.Put(ctx, fixture.key, fixture.value, options...); err != nil {
			t.Fatalf("seed %s: %v", fixture.key, err)
		}
	}

	alpha := model.Target{Context: "default", Namespace: namespace, Scope: "alpha"}
	snapshot, err := store.Snapshot(ctx, alpha)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision <= 0 || snapshot.Cluster.Initialize != "sysid-alpha" || snapshot.Cluster.Leader == nil ||
		snapshot.Cluster.Leader.Name != "node-a" || len(snapshot.Cluster.Members) != 2 || snapshot.Cluster.Status.LastLSN != 42 {
		t.Fatalf("linearizable Patroni snapshot mismatch: %#v", snapshot)
	}
	if len(snapshot.Entries) != 10 || snapshot.Entries[0].CreateRevision <= 0 || snapshot.Entries[0].ModRevision <= 0 {
		t.Fatalf("snapshot did not preserve all key revisions: %#v", snapshot.Entries)
	}
	if legacy, ok := snapshot.Entry("optime/leader"); !ok || string(legacy.Value) != "41" || snapshot.Cluster.Status.Source != dcs.KeyStatus {
		t.Fatalf("modern status did not retain and supersede legacy optime evidence: %#v", snapshot)
	}

	clusters, err := store.Discover(ctx, dcs.DiscoveryRequest{Context: "default", Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	identities := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		group := "-"
		if cluster.Target.Group != nil {
			group = fmt.Sprint(*cluster.Target.Group)
		}
		identities = append(identities, cluster.Target.Scope+"/"+group)
		if cluster.Revision <= 0 || len(cluster.EvidenceKeys) == 0 || !sort.StringsAreSorted(cluster.EvidenceKeys) ||
			cluster.Snapshot == nil || cluster.Snapshot.Revision != cluster.Revision || cluster.Snapshot.Target.ClusterID() != cluster.Target.ClusterID() {
			t.Fatalf("discovery evidence is not revisioned and deterministic: %#v", cluster)
		}
	}
	wantIdentities := []string{"alpha/-", "alphabet/-", "beta/-", "gamma/0"}
	if fmt.Sprint(identities) != fmt.Sprint(wantIdentities) {
		t.Fatalf("discovery included unrelated/orphan keys or missed a cluster: got %v want %v", identities, wantIdentities)
	}

	config, ok := snapshot.Entry("config")
	if !ok {
		t.Fatal("snapshot config entry missing")
	}
	expected := config.ModRevision
	applied, err := store.CompareAndSwapConfig(ctx, alpha, []byte(`{"ttl":31}`), &expected)
	if err != nil || !applied.Applied || applied.Revision <= snapshot.Revision || applied.Previous == nil || applied.Previous.ModRevision != expected {
		t.Fatalf("config CAS success mismatch: result=%#v err=%v", applied, err)
	}
	stale, err := store.CompareAndSwapConfig(ctx, alpha, []byte(`{"ttl":99}`), &expected)
	var conflict *dcs.ConflictError
	if !errors.As(err, &conflict) || stale.Applied || stale.Current == nil || conflict.ObservedRevision != stale.Current.ModRevision || conflict.ObservedRevision == expected {
		t.Fatalf("stale config CAS did not return observed evidence: result=%#v err=%#v", stale, err)
	}

	failover, err := store.WriteFailover(ctx, alpha, []byte(`{"leader":"node-a","member":"node-b"}`), nil)
	if err != nil || !failover.Applied {
		t.Fatalf("failover write failed: result=%#v err=%v", failover, err)
	}
	fresh, err := store.Snapshot(ctx, alpha)
	if err != nil {
		t.Fatal(err)
	}
	failoverEntry, ok := fresh.Entry("failover")
	if !ok || fresh.Cluster.Failover == nil || fresh.Cluster.Failover.Candidate != "node-b" {
		t.Fatalf("failover write was not observable: %#v", fresh)
	}
	failoverRevision := failoverEntry.ModRevision
	deletedFailover, err := store.DeleteFailover(ctx, alpha, &failoverRevision)
	if err != nil || !deletedFailover.Applied || deletedFailover.Previous == nil {
		t.Fatalf("failover CAS delete failed: result=%#v err=%v", deletedFailover, err)
	}

	patroniClient, err := patroni.NewClient(patroni.ClientOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	noWait := func(waitContext context.Context, _ time.Duration) error { return waitContext.Err() }
	controlService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Discovery: store, Patroni: patroniClient, Config: store, Failover: store, Remover: store,
		Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	discovered := controlService.Discover(ctx, control.DiscoverRequest{Context: "default", Namespace: namespace})
	if discovered.Outcome != control.Succeeded || len(discovered.Data.Clusters) != len(wantIdentities) {
		t.Fatalf("real-etcd control discovery mismatch: %#v", discovered)
	}
	if err := discovered.Validate(); err != nil {
		t.Fatalf("real-etcd discovery result invalid: %v", err)
	}
	for index, summary := range discovered.Data.Clusters {
		group := "-"
		if summary.Target.Group != nil {
			group = fmt.Sprint(*summary.Target.Group)
		}
		if summary.Target.Scope+"/"+group != wantIdentities[index] ||
			summary.DiscoveryState != model.DiscoveryDiscovered || summary.ManagementState != model.ManagementUnmanaged ||
			summary.ReachabilityState != model.ReachabilityUnknown || summary.HealthState != model.HealthUnknown {
			t.Fatalf("real-etcd discovery ordering/state mismatch at %d: %#v", index, summary)
		}
	}
	if discovered.Data.Clusters[0].MemberCount != 2 || discovered.Data.Clusters[0].Leader != "node-a" ||
		discovered.Data.Clusters[1].MemberCount != 0 || discovered.Data.Clusters[2].MemberCount != 1 || discovered.Data.Clusters[3].MemberCount != 1 {
		t.Fatalf("real-etcd discovery summary mismatch: %#v", discovered.Data.Clusters)
	}
	listedAll := controlService.ListAll(ctx, control.ListAllRequest{Context: "default", Namespace: namespace})
	if listedAll.Outcome != control.Succeeded || len(listedAll.Data.Clusters) != len(wantIdentities) {
		t.Fatalf("real-etcd list-all mismatch: %#v", listedAll)
	}
	for _, cluster := range listedAll.Data.Clusters {
		if cluster.DiscoveryState != model.DiscoveryDiscovered || cluster.ManagementState != model.ManagementAllSelected ||
			cluster.ReachabilityState != model.ReachabilityUnknown || cluster.HealthState != model.HealthUnknown {
			t.Fatalf("real-etcd list-all conflated state axes: %#v", cluster)
		}
	}
	topologyAll := controlService.TopologyAll(ctx, control.TopologyAllRequest{Context: "default", Namespace: namespace})
	if topologyAll.Outcome != control.Succeeded || len(topologyAll.Data.Topologies) != len(wantIdentities) ||
		topologyAll.Data.Topologies[0].Cluster.Target.Scope != "alpha" || len(topologyAll.Data.Topologies[0].Members) != 2 {
		t.Fatalf("real-etcd topology-all mismatch: %#v", topologyAll)
	}
	if err := topologyAll.Validate(); err != nil {
		t.Fatalf("real-etcd topology-all result invalid: %v", err)
	}
	editRequest := control.EditConfigRequest{Target: alpha, Apply: map[string]any{"loop_wait": 11}}
	preparedEdit := controlService.PrepareEditConfig(ctx, editRequest)
	if preparedEdit.Outcome != control.Succeeded {
		t.Fatalf("prepare real-etcd config CAS: %#v", preparedEdit)
	}
	edited := controlService.ExecuteEditConfig(ctx, editRequest, preparedEdit.Data)
	if edited.Outcome != control.Succeeded || edited.Data.DCSSendState != control.SendAccepted ||
		edited.Data.Verification != control.VerifiedSucceeded || edited.Data.Noop {
		t.Fatalf("real-etcd control config CAS mismatch: %#v", edited)
	}
	if err := edited.Validate(); err != nil {
		t.Fatalf("real config CAS result is invalid: %v", err)
	}
	if err := edited.Data.Validate(); err != nil {
		t.Fatalf("real config CAS data is invalid: %v", err)
	}

	ambiguousApplied := &faultConfigCAS{delegate: store, apply: true, ambiguous: true}
	ambiguousService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Config: ambiguousApplied, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ambiguousRequest := control.EditConfigRequest{Target: alpha, Apply: map[string]any{"ttl": 32}}
	preparedAmbiguous := ambiguousService.PrepareEditConfig(ctx, ambiguousRequest)
	resolvedAmbiguous := ambiguousService.ExecuteEditConfig(ctx, ambiguousRequest, preparedAmbiguous.Data)
	if resolvedAmbiguous.Outcome != control.Succeeded || resolvedAmbiguous.Data.DCSSendState != control.SendMaybeSent ||
		resolvedAmbiguous.Data.Verification != control.VerifiedSucceeded || ambiguousApplied.calls != 1 {
		t.Fatalf("real-etcd readback-resolved config CAS mismatch: result=%#v calls=%d", resolvedAmbiguous, ambiguousApplied.calls)
	}

	ambiguousUnapplied := &faultConfigCAS{ambiguous: true}
	unknownService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Config: ambiguousUnapplied, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownRequest := control.EditConfigRequest{Target: alpha, Apply: map[string]any{"ttl": 33}}
	preparedUnknown := unknownService.PrepareEditConfig(ctx, unknownRequest)
	unknownEdit := unknownService.ExecuteEditConfig(ctx, unknownRequest, preparedUnknown.Data)
	if unknownEdit.Outcome != control.Unknown || unknownEdit.Data.DCSSendState != control.SendMaybeSent ||
		unknownEdit.Data.Verification != control.Unverified || ambiguousUnapplied.calls != 1 {
		t.Fatalf("real-etcd unresolved config CAS mismatch: result=%#v calls=%d", unknownEdit, ambiguousUnapplied.calls)
	}

	concurrencyWriter := &faultConfigCAS{delegate: store, apply: true}
	concurrencyService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Config: concurrencyWriter, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	concurrentRequest := control.EditConfigRequest{Target: alpha, Apply: map[string]any{"ttl": 34}}
	preparedConcurrent := concurrencyService.PrepareEditConfig(ctx, concurrentRequest)
	beforeConcurrent, err := store.Snapshot(ctx, alpha)
	if err != nil {
		t.Fatal(err)
	}
	concurrentEntry, ok := beforeConcurrent.Entry("config")
	if !ok {
		t.Fatal("real config entry disappeared before concurrency injection")
	}
	concurrentRevision := concurrentEntry.ModRevision
	if changed, changeError := store.CompareAndSwapConfig(ctx, alpha, []byte(`{"loop_wait":11,"ttl":35}`), &concurrentRevision); changeError != nil || !changed.Applied {
		t.Fatalf("inject real concurrent config writer: result=%#v err=%v", changed, changeError)
	}
	concurrentEdit := concurrencyService.ExecuteEditConfig(ctx, concurrentRequest, preparedConcurrent.Data)
	if concurrentEdit.Outcome != control.Failed || concurrentEdit.Error == nil || concurrentEdit.Error.Category != control.CategoryConflict || concurrencyWriter.calls != 0 {
		t.Fatalf("real config concurrency boundary mismatch: result=%#v calls=%d", concurrentEdit, concurrencyWriter.calls)
	}

	failoverRequest := control.FailoverRequest{Target: alpha, Candidate: "node-b", Force: true}
	preparedFailover := controlService.PrepareFailover(ctx, failoverRequest)
	if preparedFailover.Outcome != control.Succeeded {
		t.Fatalf("prepare real-etcd control fallback: %#v", preparedFailover)
	}
	executedFailover := controlService.ExecuteFailover(ctx, failoverRequest, preparedFailover.Data)
	if executedFailover.Outcome != control.Succeeded || executedFailover.Path != control.PathRESTToDCS ||
		executedFailover.Data.RESTSendState != control.SendMaybeSent || executedFailover.Data.DCSSendState != control.SendAccepted ||
		executedFailover.Data.Verification != control.VerifiedSucceeded {
		t.Fatalf("real transport-to-etcd control fallback mismatch: %#v", executedFailover)
	}
	if err := executedFailover.Validate(); err != nil {
		t.Fatalf("real fallback result is invalid: %v", err)
	}
	select {
	case body := <-receivedFailover:
		if string(body) != `{"candidate":"node-b"}` {
			t.Fatalf("real ambiguous REST payload = %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ambiguous REST server did not observe the failover request")
	}
	select {
	case duplicate := <-receivedFailover:
		t.Fatalf("control retried the ambiguous REST write: duplicate payload=%s", duplicate)
	default:
	}
	controlSnapshot, err := store.Snapshot(ctx, alpha)
	if err != nil || controlSnapshot.Cluster.Failover == nil || controlSnapshot.Cluster.Failover.Leader != "" || controlSnapshot.Cluster.Failover.Candidate != "node-b" {
		t.Fatalf("control fallback value was not read back from real etcd: snapshot=%#v err=%v", controlSnapshot, err)
	}
	controlFailoverEntry, ok := controlSnapshot.Entry("failover")
	if !ok {
		t.Fatal("control fallback failover entry missing")
	}
	controlFailoverRevision := controlFailoverEntry.ModRevision
	if cleared, clearError := store.DeleteFailover(ctx, alpha, &controlFailoverRevision); clearError != nil || !cleared.Applied {
		t.Fatalf("clear control fallback fixture: result=%#v err=%v", cleared, clearError)
	}

	scheduledAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339)
	scheduledPayload := []byte(fmt.Sprintf(`{"leader":"node-a","member":"node-b","scheduled_at":%q}`, scheduledAt))
	scheduledWrite, err := store.WriteFailover(ctx, alpha, scheduledPayload, nil)
	if err != nil || !scheduledWrite.Applied {
		t.Fatalf("seed scheduled switchover for control flush: result=%#v err=%v", scheduledWrite, err)
	}
	flushRequest := control.FlushRequest{Target: alpha, Event: control.FlushSwitchover, Force: true}
	preparedFlush := controlService.PrepareFlush(ctx, flushRequest)
	if preparedFlush.Outcome != control.Succeeded {
		t.Fatalf("prepare real-etcd scheduled switchover flush: %#v", preparedFlush)
	}
	flushed := controlService.ExecuteFlush(ctx, flushRequest, preparedFlush.Data)
	if flushed.Outcome != control.Succeeded || flushed.Path != control.PathRESTToDCS ||
		flushed.Data.RESTSendState != control.SendMaybeSent || flushed.Data.DCSSendState != control.SendAccepted ||
		flushed.Data.Verification != control.VerifiedSucceeded || flushed.Data.Noop {
		t.Fatalf("real REST-to-etcd scheduled switchover flush mismatch: %#v", flushed)
	}
	if err := flushed.Validate(); err != nil {
		t.Fatalf("real scheduled switchover flush result is invalid: %v", err)
	}
	if err := flushed.Data.Validate(); err != nil {
		t.Fatalf("real scheduled switchover flush data is invalid: %v", err)
	}
	select {
	case <-receivedFlush:
	case <-time.After(5 * time.Second):
		t.Fatal("ambiguous REST server did not observe switchover DELETE")
	}
	select {
	case <-receivedFlush:
		t.Fatal("control retried the ambiguous switchover DELETE")
	default:
	}
	flushedSnapshot, err := store.Snapshot(ctx, alpha)
	if err != nil || flushedSnapshot.Cluster.Failover != nil {
		t.Fatalf("real DCS fallback did not clear scheduled switchover: snapshot=%#v err=%v", flushedSnapshot, err)
	}

	beta := model.Target{Context: "default", Namespace: namespace, Scope: "beta"}
	betaSnapshot, err := store.Snapshot(ctx, beta)
	if err != nil {
		t.Fatal(err)
	}
	watchContext, watchCancel := context.WithCancel(ctx)
	watch := store.Watch(watchContext, beta, betaSnapshot.Revision)
	if _, err := raw.Put(ctx, prefix+"beta/failover", `{"member":"node-b"}`); err != nil {
		t.Fatal(err)
	}
	event := receiveWatchEvent(t, watch.Events)
	if event.Type != dcs.WatchPut || event.Entry == nil || event.Entry.RelativePath != "failover" || event.Revision <= betaSnapshot.Revision {
		t.Fatalf("watch put event mismatch: %#v", event)
	}
	watchCancel()
	assertWatchCloses(t, watch)

	// A long-lived watch is implemented as bounded request-timeout leases. Let
	// two leases expire before writing, then prove the revision cursor resumes
	// without closing the public stream or losing the next event.
	renewingStore, err := etcd3.New(ctx, etcd3.Options{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, RequestTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := renewingStore.Close(); closeErr != nil {
			t.Errorf("close renewing etcd store: %v", closeErr)
		}
	})
	renewBase, err := renewingStore.Snapshot(ctx, beta)
	if err != nil {
		t.Fatal(err)
	}
	renewContext, renewCancel := context.WithCancel(ctx)
	renewed := renewingStore.Watch(renewContext, beta, renewBase.Revision)
	time.Sleep(250 * time.Millisecond)
	if _, err := raw.Put(ctx, prefix+"beta/leader", "node-b"); err != nil {
		t.Fatal(err)
	}
	renewedEvent := receiveWatchEvent(t, renewed.Events)
	if renewedEvent.Type != dcs.WatchPut || renewedEvent.Entry == nil || renewedEvent.Entry.RelativePath != "leader" ||
		renewedEvent.Revision <= renewBase.Revision {
		t.Fatalf("bounded watch lease did not resume from its revision: %#v", renewedEvent)
	}
	renewCancel()
	assertWatchCloses(t, renewed)

	compactedBase, err := store.Snapshot(ctx, beta)
	if err != nil {
		t.Fatal(err)
	}
	var compactRevision int64
	for index := 0; index < 3; index++ {
		response, putErr := raw.Put(ctx, fmt.Sprintf("%sbeta/future/%d", prefix, index), fmt.Sprint(index))
		if putErr != nil {
			t.Fatal(putErr)
		}
		compactRevision = response.Header.Revision
	}
	if _, err := raw.Compact(ctx, compactRevision); err != nil {
		t.Fatal(err)
	}
	resyncContext, resyncCancel := context.WithCancel(ctx)
	resync := store.Watch(resyncContext, beta, compactedBase.Revision)
	resyncEvent := receiveWatchEvent(t, resync.Events)
	if resyncEvent.Type != dcs.WatchResync || resyncEvent.Snapshot == nil || resyncEvent.Revision < compactRevision {
		t.Fatalf("compacted watch did not perform a full resnapshot: %#v", resyncEvent)
	}
	resyncCancel()
	assertWatchCloses(t, resync)

	removeRequest := control.RemoveRequest{Target: alpha}
	preparedRemove := controlService.PrepareRemove(ctx, removeRequest)
	if preparedRemove.Outcome != control.Succeeded {
		t.Fatalf("prepare real-etcd exact cluster remove: %#v", preparedRemove)
	}
	removed := controlService.ExecuteRemove(ctx, removeRequest, control.RemoveConfirmation{
		ClusterName: "alpha", Acknowledgement: control.RemoveAcknowledgement, Leader: "node-a",
	}, preparedRemove.Data)
	if removed.Outcome != control.Succeeded || removed.Data.Deleted == 0 || removed.Data.DCSSendState != control.SendAccepted ||
		removed.Data.Verification != control.VerifiedSucceeded {
		t.Fatalf("real-etcd exact cluster remove failed: %#v", removed)
	}
	if err := removed.Validate(); err != nil {
		t.Fatalf("real-etcd remove result invalid: %v", err)
	}
	if err := removed.Data.Validate(); err != nil {
		t.Fatalf("real-etcd remove data invalid: %v", err)
	}
	alphaAfter, err := store.Snapshot(ctx, alpha)
	if err != nil || len(alphaAfter.Entries) != 0 {
		t.Fatalf("removed cluster still has state: snapshot=%#v err=%v", alphaAfter, err)
	}
	sibling, err := raw.Get(ctx, prefix+"alphabet/config")
	if err != nil || len(sibling.Kvs) != 1 {
		t.Fatalf("exact remove deleted sibling scope: response=%#v err=%v", sibling, err)
	}

	for _, scope := range []string{"remove-maybe", "remove-unknown", "remove-concurrent"} {
		if _, err := raw.Put(ctx, prefix+scope+"/config", `{"ttl":30}`); err != nil {
			t.Fatalf("seed %s remove fixture: %v", scope, err)
		}
	}
	maybeTarget := model.Target{Context: "default", Namespace: namespace, Scope: "remove-maybe"}
	maybeRemover := &faultClusterRemover{delegate: store, apply: true, ambiguous: true}
	maybeService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Remover: maybeRemover, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	maybeRequest := control.RemoveRequest{Target: maybeTarget}
	preparedMaybe := maybeService.PrepareRemove(ctx, maybeRequest)
	resolvedRemove := maybeService.ExecuteRemove(ctx, maybeRequest, control.RemoveConfirmation{
		ClusterName: "remove-maybe", Acknowledgement: control.RemoveAcknowledgement,
	}, preparedMaybe.Data)
	if resolvedRemove.Outcome != control.Succeeded || resolvedRemove.Data.DCSSendState != control.SendMaybeSent ||
		resolvedRemove.Data.Verification != control.VerifiedSucceeded || maybeRemover.calls != 1 {
		t.Fatalf("real-etcd readback-resolved remove mismatch: result=%#v calls=%d", resolvedRemove, maybeRemover.calls)
	}

	unknownTarget := model.Target{Context: "default", Namespace: namespace, Scope: "remove-unknown"}
	unknownRemover := &faultClusterRemover{ambiguous: true}
	unknownRemoveService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Remover: unknownRemover, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownRemoveRequest := control.RemoveRequest{Target: unknownTarget}
	preparedUnknownRemove := unknownRemoveService.PrepareRemove(ctx, unknownRemoveRequest)
	unknownRemove := unknownRemoveService.ExecuteRemove(ctx, unknownRemoveRequest, control.RemoveConfirmation{
		ClusterName: "remove-unknown", Acknowledgement: control.RemoveAcknowledgement,
	}, preparedUnknownRemove.Data)
	if unknownRemove.Outcome != control.Unknown || unknownRemove.Data.DCSSendState != control.SendMaybeSent ||
		unknownRemove.Data.Verification != control.Unverified || unknownRemover.calls != 1 || len(unknownRemove.Data.RemainingKeys) == 0 {
		t.Fatalf("real-etcd unresolved remove mismatch: result=%#v calls=%d", unknownRemove, unknownRemover.calls)
	}

	concurrentTarget := model.Target{Context: "default", Namespace: namespace, Scope: "remove-concurrent"}
	concurrentRemover := &faultClusterRemover{delegate: store, apply: true}
	concurrentRemoveService, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Remover: concurrentRemover, Wait: noWait, VerificationAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	concurrentRemoveRequest := control.RemoveRequest{Target: concurrentTarget}
	preparedConcurrentRemove := concurrentRemoveService.PrepareRemove(ctx, concurrentRemoveRequest)
	if _, err := raw.Put(ctx, prefix+"remove-concurrent/members/node-new", `{"state":"running","role":"replica","version":"4.1.0"}`); err != nil {
		t.Fatal(err)
	}
	concurrentRemove := concurrentRemoveService.ExecuteRemove(ctx, concurrentRemoveRequest, control.RemoveConfirmation{
		ClusterName: "remove-concurrent", Acknowledgement: control.RemoveAcknowledgement,
	}, preparedConcurrentRemove.Data)
	if concurrentRemove.Outcome != control.Failed || concurrentRemove.Error == nil ||
		concurrentRemove.Error.Category != control.CategoryConflict || concurrentRemover.calls != 0 {
		t.Fatalf("real-etcd remove concurrency mismatch: result=%#v calls=%d", concurrentRemove, concurrentRemover.calls)
	}
}

func receiveWatchEvent(t *testing.T, events <-chan dcs.WatchEvent) dcs.WatchEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch event channel closed before evidence arrived")
		}
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for etcd watch evidence")
		return dcs.WatchEvent{}
	}
}

func assertWatchCloses(t *testing.T, stream dcs.WatchStream) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for stream.Events != nil || stream.Errors != nil {
		select {
		case _, ok := <-stream.Events:
			if !ok {
				stream.Events = nil
			}
		case err, ok := <-stream.Errors:
			if !ok {
				stream.Errors = nil
			} else if err != nil {
				t.Fatalf("canceled watch returned an error: %v", err)
			}
		case <-deadline:
			t.Fatal("watch goroutine did not close after cancellation")
		}
	}
}
