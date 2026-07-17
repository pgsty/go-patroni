package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

func TestM5DiscoverAndAllMachineAdaptersUseBoundedDiscovery(t *testing.T) {
	alpha := cliFixtureSnapshot()
	beta := cliDiscoverySnapshot("beta", 13, "beta-1")
	newReader := func() *cliSnapshotReader {
		return &cliSnapshotReader{snapshot: alpha, discoveries: []dcs.DiscoveredCluster{
			{Target: beta.Target, Revision: beta.Revision, EvidenceKeys: []string{"/service/beta/leader"}, Snapshot: &beta},
			{Target: alpha.Target, Revision: alpha.Revision, EvidenceKeys: []string{"/service/alpha/leader"}, Snapshot: &alpha},
		}}
	}

	t.Run("discover", func(t *testing.T) {
		reader := newReader()
		service := newCLIService(t, reader, &cliPatroni{}, nil)
		stdout, stderr, err, invocations, closes := executeCLIForTest(t, "", service,
			"--output", "json", "discover", "--context", "lab")
		if err != nil || stderr != "" || closes != 1 {
			t.Fatalf("discover adapter failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
		}
		var envelope struct {
			Kind string `json:"kind"`
			Data struct {
				Clusters []model.ClusterSummary `json:"clusters"`
			} `json:"data"`
		}
		if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "ClusterDiscovery" || len(envelope.Data.Clusters) != 2 {
			t.Fatalf("discover envelope mismatch: decode=%v envelope=%#v output=%s", decodeErr, envelope, stdout)
		}
		if envelope.Data.Clusters[0].Target.Scope != "alpha" || envelope.Data.Clusters[1].Target.Scope != "beta" ||
			envelope.Data.Clusters[0].ManagementState != model.ManagementUnmanaged ||
			envelope.Data.Clusters[0].ReachabilityState != model.ReachabilityUnknown ||
			envelope.Data.Clusters[0].MemberCount != 2 || envelope.Data.Clusters[1].Leader != "beta-1" {
			t.Fatalf("discover data/state mismatch: %#v", envelope.Data.Clusters)
		}
		if len(reader.discoveryCalls) != 1 || len(reader.calls) != 0 || len(invocations) != 1 || invocations[0].request.operation != config.OperationDiscover {
			t.Fatalf("discover did not use one bounded scan: discovery=%v snapshots=%v invocations=%#v", reader.discoveryCalls, reader.calls, invocations)
		}
	})

	t.Run("list-all", func(t *testing.T) {
		reader := newReader()
		service := newCLIService(t, reader, &cliPatroni{}, nil)
		stdout, stderr, err, invocations, closes := executeCLIForTest(t, "", service, "--output", "json", "list", "--all")
		if err != nil || stderr != "" || closes != 1 {
			t.Fatalf("list --all failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
		}
		var envelope struct {
			Kind string `json:"kind"`
			Data struct {
				Clusters []model.Cluster `json:"clusters"`
			} `json:"data"`
		}
		if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "ClusterList" || len(envelope.Data.Clusters) != 2 {
			t.Fatalf("list --all envelope mismatch: decode=%v envelope=%#v output=%s", decodeErr, envelope, stdout)
		}
		if envelope.Data.Clusters[0].Target.Scope != "alpha" || envelope.Data.Clusters[0].ManagementState != model.ManagementAllSelected ||
			envelope.Data.Clusters[0].ReachabilityState != model.ReachabilityUnknown || len(reader.discoveryCalls) != 1 || len(reader.calls) != 0 ||
			len(invocations) != 1 || invocations[0].request.operation != config.OperationDiscover {
			t.Fatalf("list --all selection/read contract mismatch: data=%#v discovery=%v snapshots=%v invocations=%#v", envelope.Data, reader.discoveryCalls, reader.calls, invocations)
		}
	})

	t.Run("topology-all", func(t *testing.T) {
		reader := newReader()
		service := newCLIService(t, reader, &cliPatroni{}, nil)
		stdout, stderr, err, _, closes := executeCLIForTest(t, "", service, "--output", "json", "topology", "--all")
		if err != nil || stderr != "" || closes != 1 {
			t.Fatalf("topology --all failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
		}
		var envelope struct {
			Kind string `json:"kind"`
			Data struct {
				Topologies []control.TopologyData `json:"topologies"`
			} `json:"data"`
		}
		if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "ClusterTopologyList" ||
			len(envelope.Data.Topologies) != 2 || envelope.Data.Topologies[0].Cluster.Target.Scope != "alpha" ||
			len(reader.discoveryCalls) != 1 || len(reader.calls) != 0 {
			t.Fatalf("topology --all envelope/read mismatch: decode=%v envelope=%#v discovery=%v snapshots=%v output=%s",
				decodeErr, envelope, reader.discoveryCalls, reader.calls, stdout)
		}
	})
}

func TestM5NoArgListCompatibilityDoesNotSelectAll(t *testing.T) {
	alpha := cliFixtureSnapshot()
	reader := &cliSnapshotReader{snapshot: alpha, discoveries: []dcs.DiscoveredCluster{{Target: alpha.Target, Snapshot: &alpha}}}
	service := newCLIService(t, reader, &cliPatroni{}, nil)
	stdout, stderr, err, invocations, closes := executeCLIForTest(t, "", service, "--output", "json", "list")
	if err != nil || stderr != "" || closes != 1 {
		t.Fatalf("no-arg list failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
	}
	var envelope struct {
		Data struct {
			Clusters []model.Cluster `json:"clusters"`
		} `json:"data"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || len(envelope.Data.Clusters) != 1 {
		t.Fatalf("no-arg list envelope mismatch: decode=%v output=%s", decodeErr, stdout)
	}
	cluster := envelope.Data.Clusters[0]
	if cluster.Target.Scope != "alpha" || cluster.ManagementState != model.ManagementExplicit ||
		cluster.ReachabilityState != model.ReachabilityUnknown || len(reader.calls) != 1 || len(reader.discoveryCalls) != 0 ||
		len(invocations) != 1 || invocations[0].request.operation != config.OperationClusterRead {
		t.Fatalf("no-arg list compatibility changed: cluster=%#v snapshots=%v discovery=%v invocations=%#v", cluster, reader.calls, reader.discoveryCalls, invocations)
	}
}

func TestM5NoArgTopologyCompatibilityDoesNotSelectAll(t *testing.T) {
	alpha := cliFixtureSnapshot()
	reader := &cliSnapshotReader{snapshot: alpha, discoveries: []dcs.DiscoveredCluster{{Target: alpha.Target, Snapshot: &alpha}}}
	service := newCLIService(t, reader, &cliPatroni{}, nil)
	stdout, stderr, err, invocations, closes := executeCLIForTest(t, "", service, "--output", "json", "topology")
	if err != nil || stderr != "" || closes != 1 {
		t.Fatalf("no-arg topology failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
	}
	var envelope struct {
		Kind string `json:"kind"`
		Data struct {
			Cluster model.Cluster `json:"cluster"`
		} `json:"data"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "ClusterTopology" {
		t.Fatalf("no-arg topology envelope mismatch: decode=%v output=%s", decodeErr, stdout)
	}
	cluster := envelope.Data.Cluster
	if cluster.Target.Scope != "alpha" || cluster.ManagementState != model.ManagementExplicit ||
		cluster.ReachabilityState != model.ReachabilityUnknown || len(reader.calls) != 1 || len(reader.discoveryCalls) != 0 ||
		len(invocations) != 1 || invocations[0].request.operation != config.OperationClusterRead {
		t.Fatalf("no-arg topology compatibility changed: cluster=%#v snapshots=%v discovery=%v invocations=%#v", cluster, reader.calls, reader.discoveryCalls, invocations)
	}
}

func TestM5AllSelectionRejectsExplicitTargetsGroupsAndWatchMachineMode(t *testing.T) {
	for _, arguments := range [][]string{
		{"list", "--all", "alpha"},
		{"list", "--all", "--group", "0"},
		{"topology", "--all", "alpha"},
		{"topology", "--all", "--group", "0"},
		{"--output", "json", "list", "--all", "--watch", "1"},
		{"--output", "json", "topology", "--all", "--watch", "1"},
	} {
		t.Run(strings.Join(arguments, "_"), func(t *testing.T) {
			opened := false
			factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
				opened = true
				return nil, nil
			}
			var stdout, stderr strings.Builder
			root := newRootCommand(strings.NewReader(""), &stdout, &stderr, factory)
			root.SetArgs(arguments)
			err := root.ExecuteContext(context.Background())
			if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || opened {
				t.Fatalf("invalid --all selection was accepted: args=%v err=%v opened=%t stdout=%s stderr=%s", arguments, err, opened, stdout.String(), stderr.String())
			}
		})
	}
}

func TestM5DiscoverHumanOutputContainsRequiredFacts(t *testing.T) {
	alpha := cliFixtureSnapshot()
	reader := &cliSnapshotReader{snapshot: alpha, discoveries: []dcs.DiscoveredCluster{{
		Target: alpha.Target, Revision: alpha.Revision, EvidenceKeys: []string{"/service/alpha/leader"}, Snapshot: &alpha,
	}}}
	service := newCLIService(t, reader, &cliPatroni{}, nil)
	stdout, stderr, err, _, _ := executeCLIForTest(t, "", service, "discover")
	if err != nil || stderr != "" {
		t.Fatalf("human discover failed: err=%v stderr=%q output=%s", err, stderr, stdout)
	}
	for _, required := range []string{"Scope", "Members", "Leader", "Reachability", "DCS Revision", "alpha", "node-a", "UNKNOWN", "8"} {
		if !strings.Contains(stdout, required) {
			t.Errorf("human discover output lacks %q: %s", required, stdout)
		}
	}
}

func TestM5MachineDiscoveryGoldens(t *testing.T) {
	fixedTime := time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC)
	tests := []struct {
		name   string
		golden string
		args   []string
	}{
		{name: "discover", golden: "testdata/machine-cluster-discovery.golden.json", args: []string{"--output", "json", "discover"}},
		{name: "list-all", golden: "testdata/machine-cluster-list-all.golden.json", args: []string{"--output", "json", "list", "--all"}},
		{name: "topology-all", golden: "testdata/machine-cluster-topology-list.golden.json", args: []string{"--output", "json", "topology", "--all"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alpha := cliFixtureSnapshot()
			beta := cliDiscoverySnapshot("beta", 13, "beta-1")
			reader := &cliSnapshotReader{snapshot: alpha, discoveries: []dcs.DiscoveredCluster{
				{Target: beta.Target, Revision: beta.Revision, EvidenceKeys: []string{"/service/beta/leader"}, Snapshot: &beta},
				{Target: alpha.Target, Revision: alpha.Revision, EvidenceKeys: []string{"/service/alpha/leader"}, Snapshot: &alpha},
			}}
			service := newCLIService(t, reader, &cliPatroni{}, nil)
			invocations, closes := make([]runtimeInvocation, 0, 1), 0
			var stdout, stderr bytes.Buffer
			root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr,
				cliRuntimeFactory(service, &invocations, &closes), func() time.Time { return fixedTime }, func() string { return "m5-machine" })
			root.SetArgs(test.args)
			if err := root.ExecuteContext(context.Background()); err != nil || stderr.String() != "" || closes != 1 {
				t.Fatalf("M5 machine fixture failed: err=%v stderr=%q closes=%d output=%s", err, stderr.String(), closes, stdout.String())
			}
			requireGolden(t, test.golden, stdout.String())
		})
	}
}

func cliDiscoverySnapshot(scope string, revision int64, leader string) dcs.Snapshot {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: scope}).Normalize()
	return dcs.BuildSnapshot(target, "/service/"+scope, revision, []dcs.Entry{
		{RelativePath: "leader", ModRevision: revision - 1, Value: []byte(leader)},
		{RelativePath: "members/" + leader, ModRevision: revision, Value: []byte(`{"api_url":"https://node:8008/patroni","conn_url":"postgres://node:5432/postgres","state":"running","role":"primary","version":"4.1.0"}`)},
	})
}
