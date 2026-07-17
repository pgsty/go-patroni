package dcs_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"go.yaml.in/yaml/v3"
)

func entry(key, value string, revision int64, lease int64) dcs.Entry {
	return dcs.Entry{Key: key, Value: []byte(value), CreateRevision: 1, ModRevision: revision, Version: 1, Lease: lease}
}

func TestPatroniPathsMatchPinnedLayout(t *testing.T) {
	target := model.Target{Context: "prod", Namespace: "//service//nested/", Scope: "alpha"}
	prefix, err := dcs.ClusterPrefix(target)
	if err != nil || prefix != "/service/nested/alpha" {
		t.Fatalf("PostgreSQL prefix mismatch: prefix=%q err=%v", prefix, err)
	}
	key, err := dcs.KeyPath(target, "members/node-1")
	if err != nil || key != "/service/nested/alpha/members/node-1" {
		t.Fatalf("member key mismatch: key=%q err=%v", key, err)
	}
	group := 0
	target.Group = &group
	prefix, err = dcs.ClusterPrefix(target)
	if err != nil || prefix != "/service/nested/alpha/0" {
		t.Fatalf("MPP prefix mismatch: prefix=%q err=%v", prefix, err)
	}
	if got := dcs.NamespacePrefix(""); got != "/service/" {
		t.Fatalf("default namespace prefix mismatch: %q", got)
	}
	if _, err := dcs.ClusterPrefix(model.Target{}); err == nil {
		t.Fatal("cluster path accepted a missing scope")
	}
}

func TestSnapshotDecodesAllPinnedKeyKindsAndRetainsUnknown(t *testing.T) {
	target := (model.Target{Scope: "alpha"}).Normalize()
	prefix := "/service/alpha"
	entries := []dcs.Entry{
		entry(prefix+"/unknown/future", `{"kept":true}`, 14, 0),
		entry(prefix+"/members/node-b", `{"api_url":"http://node-b:8008/patroni","conn_url":"postgres://user:__BOAR_TEST_ONLY_DCS_DSN_PASSWORD__@node-b/app","state":"running","role":"replica","timeline":2,"pending_restart":true,"tags":{"zone":"east"},"version":"4.1.0"}`, 12, 99),
		entry(prefix+"/initialize", "sysid-1", 2, 0),
		entry(prefix+"/config", `{"ttl":30,"postgresql":{"parameters":{"password":"__BOAR_TEST_ONLY_DCS_CONFIG_PASSWORD__"}}}`, 3, 0),
		entry(prefix+"/leader", "node-a", 11, 77),
		entry(prefix+"/failover", `{"leader":"node-a","member":"node-b","scheduled_at":"2026-08-01T00:00:00Z"}`, 8, 0),
		entry(prefix+"/history", `[[1,42,"reason","2026-01-01T00:00:00Z"]]`, 4, 0),
		entry(prefix+"/status", `{"optime":123,"slots":{"slot_a":100},"retain_slots":["node-b"]}`, 10, 0),
		entry(prefix+"/optime/leader", "99", 5, 0),
		entry(prefix+"/sync", `{"leader":"node-a","sync_standby":"node-b,node-c","quorum":1}`, 9, 0),
		entry(prefix+"/failsafe", `{"node-a":"http://node-a:8008/patroni","node-b":"http://node-b:8008/patroni"}`, 7, 0),
		entry(prefix+"/members/node-a", `{"api_url":"http://node-a:8008/patroni","state":"running","role":"primary"}`, 13, 77),
	}
	snapshot := dcs.BuildSnapshot(target, prefix, 20, entries)
	if snapshot.Revision != 20 || snapshot.Cluster.Initialize != "sysid-1" || snapshot.Cluster.Leader == nil || snapshot.Cluster.Leader.Name != "node-a" {
		t.Fatalf("snapshot identity mismatch: %#v", snapshot)
	}
	if len(snapshot.Cluster.Members) != 2 || snapshot.Cluster.Members[0].Name != "node-a" || snapshot.Cluster.Members[1].Name != "node-b" {
		t.Fatalf("members not decoded/sorted: %#v", snapshot.Cluster.Members)
	}
	if snapshot.Cluster.Failover == nil || snapshot.Cluster.Failover.Candidate != "node-b" || snapshot.Cluster.Sync.Quorum != 1 || len(snapshot.Cluster.Sync.Standbys) != 2 {
		t.Fatalf("control state mismatch: failover=%#v sync=%#v", snapshot.Cluster.Failover, snapshot.Cluster.Sync)
	}
	if snapshot.Cluster.Status.Source != dcs.KeyStatus || snapshot.Cluster.Status.LastLSN != 123 || snapshot.Cluster.Status.Slots["slot_a"] != 100 {
		t.Fatalf("status did not prefer modern key: %#v", snapshot.Cluster.Status)
	}
	if len(snapshot.Cluster.History) != 1 || len(snapshot.Cluster.Failsafe) != 2 || len(snapshot.Issues) != 0 {
		t.Fatalf("history/failsafe/issues mismatch: %#v", snapshot)
	}
	unknown, ok := snapshot.Entry("unknown/future")
	if !ok || unknown.Kind != dcs.KeyUnknown || string(unknown.Value) != `{"kept":true}` {
		t.Fatalf("unknown DCS key was not retained: %#v", unknown)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"__BOAR_TEST_ONLY_DCS_DSN_PASSWORD__", "__BOAR_TEST_ONLY_DCS_CONFIG_PASSWORD__"} {
		if strings.Contains(string(encoded), forbidden) || strings.Contains(fmt.Sprintf("%#v", snapshot), forbidden) || strings.Contains(fmt.Sprintf("%#v", snapshot.Cluster.Members[1]), forbidden) {
			t.Fatal("default DCS formatting or JSON leaked a protected raw value")
		}
	}
}

func TestSnapshotMalformedValuesBecomeIssuesNotLostRawData(t *testing.T) {
	prefix := "/service/broken"
	snapshot := dcs.BuildSnapshot(model.Target{Scope: "broken"}, prefix, 9, []dcs.Entry{
		entry(prefix+"/config", `[1,2]`, 2, 0),
		entry(prefix+"/members/node-1", `{`, 3, 0),
		entry(prefix+"/history", `{}`, 4, 0),
		entry(prefix+"/status", `not-a-status`, 5, 0),
		entry(prefix+"/sync", `[]`, 6, 0),
		entry(prefix+"/failsafe", `[1]`, 7, 0),
	})
	if len(snapshot.Issues) != 6 || len(snapshot.Entries) != 6 {
		t.Fatalf("malformed keys were not surfaced and retained: issues=%#v entries=%d", snapshot.Issues, len(snapshot.Entries))
	}
	if snapshot.Cluster.Config == nil || len(snapshot.Cluster.Config) != 0 {
		t.Fatalf("invalid dynamic config did not project to an empty map: %#v", snapshot.Cluster.Config)
	}
}

func TestLegacyFailoverMemberAndStatusForms(t *testing.T) {
	prefix := "/service/legacy"
	snapshot := dcs.BuildSnapshot(model.Target{Scope: "legacy"}, prefix, 8, []dcs.Entry{
		entry(prefix+"/members/node-1", `postgres://user:__BOAR_TEST_ONLY_DCS_LEGACY_PASSWORD__@node-1/app?application_name=http%3A%2F%2Fnode-1%3A8008%2Fpatroni`, 3, 10),
		entry(prefix+"/failover", `leader-a:candidate-b`, 4, 0),
		entry(prefix+"/optime/leader", `321`, 5, 0),
	})
	if snapshot.Cluster.Members[0].Data.ConnURL != "postgres://user:__BOAR_TEST_ONLY_DCS_LEGACY_PASSWORD__@node-1/app" ||
		snapshot.Cluster.Members[0].Data.APIURL != "http://node-1:8008/patroni" ||
		snapshot.Cluster.Failover.Candidate != "candidate-b" || snapshot.Cluster.Status.LastLSN != 321 {
		t.Fatalf("legacy projection mismatch: %#v", snapshot.Cluster)
	}
}

func TestStatusMatchesPatroniStringEncodedCompatibilityForms(t *testing.T) {
	prefix := "/service/status-compat"
	snapshot := dcs.BuildSnapshot(model.Target{Scope: "status-compat"}, prefix, 8, []dcs.Entry{
		entry(prefix+"/status", `{"optime":"42","slots":"{\"slot_a\":\"40\",\"invalid\":\"x\"}","retain_slots":"[\"node-b\"]"}`, 5, 0),
	})
	status := snapshot.Cluster.Status
	if status.LastLSN != 42 || status.Slots["slot_a"] != 40 || status.Slots["invalid"] != 0 ||
		len(status.RetainSlots) != 1 || status.RetainSlots[0] != "node-b" || len(snapshot.Issues) != 0 {
		t.Fatalf("string-encoded status projection mismatch: %#v issues=%#v", status, snapshot.Issues)
	}
}

func TestSyncWithoutLeaderIsEmptyLikePatroni(t *testing.T) {
	prefix := "/service/sync-empty"
	snapshot := dcs.BuildSnapshot(model.Target{Scope: "sync-empty"}, prefix, 8, []dcs.Entry{
		entry(prefix+"/sync", `{"leader":"","sync_standby":"node-b","quorum":1}`, 5, 0),
	})
	if snapshot.Cluster.Sync.Leader != "" || len(snapshot.Cluster.Sync.Standbys) != 0 || snapshot.Cluster.Sync.Quorum != 0 {
		t.Fatalf("leaderless sync state is not empty: %#v", snapshot.Cluster.Sync)
	}
}

func TestFailoverJSONUsesPatroniMemberFieldAndJSONDecoderRejectsTrailingData(t *testing.T) {
	prefix := "/service/strict"
	snapshot := dcs.BuildSnapshot(model.Target{Scope: "strict"}, prefix, 8, []dcs.Entry{
		entry(prefix+"/failover", `{"leader":"node-a","candidate":"not-a-patroni-field"}`, 4, 0),
		entry(prefix+"/config", `{} {}`, 5, 0),
	})
	if snapshot.Cluster.Failover == nil || snapshot.Cluster.Failover.Candidate != "" {
		t.Fatalf("non-Patroni candidate field was projected: %#v", snapshot.Cluster.Failover)
	}
	if len(snapshot.Issues) != 1 || snapshot.Issues[0].RelativePath != "config" {
		t.Fatalf("trailing JSON was not rejected precisely: %#v", snapshot.Issues)
	}
}

func TestDiscoveryRecognizesKnownEvidenceAndExcludesOrphans(t *testing.T) {
	entries := []dcs.Entry{
		entry("/service/alpha/config", `{}`, 2, 0),
		entry("/service/alpha/members/node-1", `{"conn_url":"postgres://user:__BOAR_TEST_ONLY_DISCOVERY_PASSWORD__@node-1/postgres","version":"4.1.0"}`, 3, 0),
		entry("/service/beta/0/config", `{}`, 4, 0),
		entry("/service/beta/1/members/node-2", `{}`, 5, 0),
		entry("/service/gamma/random", `{}`, 6, 0),
		entry("/service/orphan/members/", `{}`, 7, 0),
		entry("/serviceX/wrong/config", `{}`, 8, 0),
		entry("/service/beta/not-a-group/random", `{}`, 9, 0),
	}
	clusters := dcs.DiscoverFromEntries(dcs.DiscoveryRequest{Context: "staging", Namespace: "/service"}, 10, entries)
	if len(clusters) != 3 {
		t.Fatalf("discovery returned %d clusters, want 3: %#v", len(clusters), clusters)
	}
	if clusters[0].Target.Scope != "alpha" || clusters[0].Target.Group != nil || clusters[0].Revision != 10 || len(clusters[0].EvidenceKeys) != 2 {
		t.Fatalf("non-MPP discovery mismatch: %#v", clusters[0])
	}
	if clusters[0].Snapshot == nil || clusters[0].Snapshot.Target != clusters[0].Target ||
		clusters[0].Snapshot.Revision != 10 || len(clusters[0].Snapshot.Cluster.Members) != 1 ||
		clusters[0].Snapshot.Cluster.Config == nil {
		t.Fatalf("discovery did not retain its one-read cluster snapshot: %#v", clusters[0].Snapshot)
	}
	if clusters[1].Target.Scope != "beta" || clusters[1].Target.Group == nil || *clusters[1].Target.Group != 0 ||
		clusters[2].Target.Group == nil || *clusters[2].Target.Group != 1 {
		t.Fatalf("MPP discovery order mismatch: %#v", clusters)
	}
	if clusters[1].Snapshot == nil || clusters[1].Snapshot.Prefix != "/service/beta/0" ||
		clusters[2].Snapshot == nil || clusters[2].Snapshot.Prefix != "/service/beta/1" {
		t.Fatalf("Citus discovery snapshots lost exact group roots: %#v", clusters)
	}
	encodedJSON, jsonErr := json.Marshal(clusters[0])
	encodedYAML, yamlErr := yaml.Marshal(clusters[0])
	for _, encoded := range [][]byte{encodedJSON, encodedYAML} {
		if strings.Contains(string(encoded), "__BOAR_TEST_ONLY_DISCOVERY_PASSWORD__") || strings.Contains(strings.ToLower(string(encoded)), "snapshot") {
			t.Fatalf("same-read discovery snapshot leaked through public serialization: %s", encoded)
		}
	}
	if jsonErr != nil || yamlErr != nil {
		t.Fatalf("serialize secret-safe discovery identity: json=%v yaml=%v", jsonErr, yamlErr)
	}
}

func TestKeyClassificationIsClosedToPinnedInventory(t *testing.T) {
	tests := map[string]dcs.KeyKind{
		"initialize": dcs.KeyInitialize, "config": dcs.KeyConfig, "members/node": dcs.KeyMember,
		"leader": dcs.KeyLeader, "failover": dcs.KeyFailover, "history": dcs.KeyHistory,
		"status": dcs.KeyStatus, "optime/leader": dcs.KeyLeaderOptime, "sync": dcs.KeySync,
		"failsafe": dcs.KeyFailsafe, "members/": dcs.KeyUnknown, "members/node/nested": dcs.KeyUnknown,
		"future": dcs.KeyUnknown,
	}
	for relative, want := range tests {
		if got := dcs.ClassifyRelativePath(relative); got != want {
			t.Errorf("classify %q = %q, want %q", relative, got, want)
		}
	}
}
