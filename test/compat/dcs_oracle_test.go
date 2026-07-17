//go:build oracle

package compat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

type dcsOracleInput struct {
	Members     []dcsOracleMember `json:"members"`
	Failovers   []string          `json:"failovers"`
	SyncStates  []string          `json:"sync_states"`
	Statuses    []string          `json:"statuses"`
	Configs     []string          `json:"configs"`
	Histories   []string          `json:"histories"`
	RawClusters []rawClusterInput `json:"raw_clusters"`
}

type rawClusterInput struct {
	Initialize   string `json:"initialize"`
	Leader       string `json:"leader"`
	LegacyOptime string `json:"legacy_optime"`
	Failsafe     string `json:"failsafe"`
}

type dcsOracleMember struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type dcsProjection struct {
	Members     []memberProjection     `json:"members"`
	Failovers   []failoverProjection   `json:"failovers"`
	SyncStates  []syncProjection       `json:"sync_states"`
	Statuses    []statusProjection     `json:"statuses"`
	Configs     []map[string]any       `json:"configs"`
	Histories   [][][]any              `json:"histories"`
	RawClusters []rawClusterProjection `json:"raw_clusters"`
}

type rawClusterProjection struct {
	Initialize string            `json:"initialize"`
	Leader     string            `json:"leader"`
	LegacyLSN  int64             `json:"legacy_lsn"`
	Failsafe   map[string]string `json:"failsafe"`
}

type memberProjection struct {
	Name    string `json:"name"`
	ConnURL string `json:"conn_url"`
	APIURL  string `json:"api_url"`
	State   string `json:"state"`
	Role    string `json:"role"`
}

type failoverProjection struct {
	Leader    string `json:"leader"`
	Candidate string `json:"candidate"`
}

type syncProjection struct {
	Leader   string   `json:"leader"`
	Standbys []string `json:"standbys"`
	Quorum   int      `json:"quorum"`
}

type statusProjection struct {
	LastLSN     int64            `json:"last_lsn"`
	Slots       map[string]int64 `json:"slots"`
	RetainSlots []string         `json:"retain_slots"`
}

func TestDCSProjectionAgainstPinnedPatroni(t *testing.T) {
	input := dcsOracleInput{
		Members: []dcsOracleMember{
			{Name: "legacy", Value: "postgres://operator@node-1/app?application_name=http%3A%2F%2Fnode-1%3A8008%2Fpatroni"},
			{Name: "normal", Value: `{"conn_url":"postgres://node-2/app","api_url":"http://node-2:8008/patroni","state":"running","role":"replica","future":true}`},
			{Name: "invalid", Value: `{`},
		},
		Failovers: []string{
			`{"leader":"node-1","member":"node-2"}`,
			`{"leader":"node-1","candidate":"not-a-Patroni-field"}`,
			`node-1:node-3`,
			`null`,
		},
		SyncStates: []string{
			`{"leader":"node-1","sync_standby":"node-2, node-3","quorum":"1"}`,
			`[]`,
			`{"leader":"","sync_standby":"node-2","quorum":2}`,
		},
		Statuses: []string{
			`{"optime":"42","slots":"{\"slot_a\":\"40\",\"invalid\":\"x\"}","retain_slots":"[\"node-2\"]"}`,
			`321`,
			`not-json`,
			`{"optime":1,"slots":[],"retain_slots":{}}`,
		},
		Configs:   []string{`{"ttl":30,"nested":{"enabled":true}}`, `{`, `[]`},
		Histories: []string{`[[1,42,"reason"],[2,99,"reason-2","at","node-1"]]`, `{}`, `not-json`},
		RawClusters: []rawClusterInput{
			{Initialize: "sysid-1", Leader: "node-1", LegacyOptime: "123", Failsafe: `{"node-1":"http://node-1:8008/patroni"}`},
			{Initialize: "", Leader: "", LegacyOptime: "invalid", Failsafe: `[]`},
		},
	}
	want := runPatroniDCSOracle(t, input)
	got := projectWithSDK(input)
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(want, "", "  ")
		t.Fatalf("SDK DCS projection differs from pinned Patroni oracle:\nwant=%s\n got=%s", wantJSON, gotJSON)
	}
}

func TestDCSMutationContractAgainstPinnedPatroni(t *testing.T) {
	root := repositoryRoot(t)
	source := patroniSource(root)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	script := filepath.Join(root, "test", "compat", "oracle", "dcs_mutation_contract.py")
	command := exec.CommandContext(ctx, "python3", script, filepath.Join(source, "patroni", "dcs", "etcd3.py"))
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("extract pinned Patroni DCS mutation contract: %v: %s", err, stderr.String())
	}
	var got struct {
		Config struct {
			Method string `json:"method"`
			Path   string `json:"path"`
			CAS    string `json:"cas"`
		} `json:"config"`
		Failover struct {
			Method string `json:"method"`
			Path   string `json:"path"`
			CAS    string `json:"cas"`
		} `json:"failover"`
		Remove struct {
			Method string `json:"method"`
			Path   string `json:"path"`
		} `json:"remove"`
		PutTargets   []string `json:"client_put_compare_targets"`
		DeleteTarget string   `json:"client_delete_compare_target"`
	}
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatal(err)
	}
	if got.Config.Method != "put" || got.Config.Path != "config_path" || got.Config.CAS != "mod_revision" ||
		got.Failover.Method != "put" || got.Failover.Path != "failover_path" || got.Failover.CAS != "mod_revision" ||
		got.Remove.Method != "deleteprefix" || got.Remove.Path != "client_path('')" ||
		!reflect.DeepEqual(got.PutTargets, []string{"CREATE", "MOD"}) || got.DeleteTarget != "MOD" {
		t.Fatalf("pinned Patroni mutation contract drifted: %s", output)
	}
	var _ dcs.ConfigCAS = dcs.Store(nil)
	var _ dcs.FailoverCAS = dcs.Store(nil)
	var _ dcs.ClusterRemover = dcs.Store(nil)
}

func runPatroniDCSOracle(t *testing.T, input dcsOracleInput) dcsProjection {
	t.Helper()
	root := repositoryRoot(t)
	source := patroniSource(root)
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	script := filepath.Join(root, "test", "compat", "oracle", "dcs_projection.py")
	command := exec.CommandContext(ctx, "python3", script)
	command.Stdin = bytes.NewReader(payload)
	command.Env = append(os.Environ(), "PYTHONPATH="+source)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("run pinned Patroni DCS oracle: %v: %s", err, stderr.String())
	}
	var projection dcsProjection
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.UseNumber()
	if err := decoder.Decode(&projection); err != nil {
		t.Fatalf("decode Patroni DCS oracle output: %v", err)
	}
	return projection
}

func patroniSource(root string) string {
	source := os.Getenv("PATRONI_SOURCE")
	if source == "" {
		source = filepath.Clean(filepath.Join(root, "..", "..", "dev", "patroni"))
	}
	if resolved, err := filepath.Abs(source); err == nil {
		return resolved
	}
	return source
}

func projectWithSDK(input dcsOracleInput) dcsProjection {
	const prefix = "/service/oracle"
	target := model.Target{Scope: "oracle"}
	output := dcsProjection{}
	for index, item := range input.Members {
		snapshot := dcs.BuildSnapshot(target, prefix, 10, []dcs.Entry{{
			Key: prefix + "/members/" + item.Name, Value: []byte(item.Value), ModRevision: int64(index + 1), Lease: 9,
		}})
		member := snapshot.Cluster.Members[0]
		output.Members = append(output.Members, memberProjection{
			Name: member.Name, ConnURL: member.Data.ConnURL, APIURL: member.Data.APIURL,
			State: member.Data.State, Role: member.Data.Role,
		})
	}
	for index, value := range input.Failovers {
		snapshot := singleValueSnapshot(target, prefix, "failover", value, index)
		output.Failovers = append(output.Failovers, failoverProjection{
			Leader: snapshot.Cluster.Failover.Leader, Candidate: snapshot.Cluster.Failover.Candidate,
		})
	}
	for index, value := range input.SyncStates {
		snapshot := singleValueSnapshot(target, prefix, "sync", value, index)
		state := snapshot.Cluster.Sync
		output.SyncStates = append(output.SyncStates, syncProjection{
			Leader: state.Leader, Standbys: append([]string{}, state.Standbys...), Quorum: state.Quorum,
		})
	}
	for index, value := range input.Statuses {
		snapshot := singleValueSnapshot(target, prefix, "status", value, index)
		status := snapshot.Cluster.Status
		output.Statuses = append(output.Statuses, statusProjection{
			LastLSN: status.LastLSN, Slots: status.Slots, RetainSlots: append([]string{}, status.RetainSlots...),
		})
	}
	for index, value := range input.Configs {
		snapshot := singleValueSnapshot(target, prefix, "config", value, index)
		output.Configs = append(output.Configs, snapshot.Cluster.Config)
	}
	for index, value := range input.Histories {
		snapshot := singleValueSnapshot(target, prefix, "history", value, index)
		history := make([][]any, 0, len(snapshot.Cluster.History))
		for _, raw := range snapshot.Cluster.History {
			var fields []any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			_ = decoder.Decode(&fields)
			history = append(history, fields)
		}
		output.Histories = append(output.Histories, history)
	}
	for index, value := range input.RawClusters {
		snapshot := dcs.BuildSnapshot(target, prefix, 10, []dcs.Entry{
			{Key: prefix + "/initialize", Value: []byte(value.Initialize), ModRevision: int64(index + 1)},
			{Key: prefix + "/leader", Value: []byte(value.Leader), ModRevision: int64(index + 1)},
			{Key: prefix + "/optime/leader", Value: []byte(value.LegacyOptime), ModRevision: int64(index + 1)},
			{Key: prefix + "/failsafe", Value: []byte(value.Failsafe), ModRevision: int64(index + 1)},
		})
		projection := rawClusterProjection{
			Initialize: snapshot.Cluster.Initialize,
			LegacyLSN:  snapshot.Cluster.Status.LastLSN,
			Failsafe:   snapshot.Cluster.Failsafe,
		}
		if snapshot.Cluster.Leader != nil {
			projection.Leader = snapshot.Cluster.Leader.Name
		}
		output.RawClusters = append(output.RawClusters, projection)
	}
	return output
}

func singleValueSnapshot(target model.Target, prefix, relative, value string, index int) dcs.Snapshot {
	return dcs.BuildSnapshot(target, prefix, 10, []dcs.Entry{{
		Key: prefix + "/" + relative, Value: []byte(value), ModRevision: int64(index + 1),
	}})
}

func (projection dcsProjection) String() string {
	data, _ := json.Marshal(projection)
	return fmt.Sprintf("dcsProjection(%s)", data)
}
