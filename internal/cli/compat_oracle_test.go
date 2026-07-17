//go:build oracle

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
	"go.yaml.in/yaml/v3"
)

type patronictlOracle struct {
	PatroniVersion string       `json:"patroniVersion"`
	Cases          []oracleCase `json:"cases"`
}

type oracleCase struct {
	ID     string           `json:"id"`
	Args   []string         `json:"args"`
	Input  string           `json:"input"`
	Exit   int              `json:"exit"`
	Output string           `json:"output"`
	REST   []oracleRESTCall `json:"rest"`
	DCS    []map[string]any `json:"dcs"`
}

type oracleRESTCall struct {
	Member   string `json:"member"`
	Method   string `json:"method"`
	Endpoint string `json:"endpoint"`
	Data     any    `json:"data"`
}

type oracleStore struct {
	snapshot  dcs.Snapshot
	removed   bool
	mutations []map[string]any
}

func newOracleStore(paused bool) *oracleStore {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: "alpha"}).Normalize()
	configuration := `{"loop_wait":10,"synchronous_mode":true,"ttl":30}`
	pause := ""
	if paused {
		configuration = `{"loop_wait":10,"pause":true,"synchronous_mode":true,"ttl":30}`
		pause = `,"pause":true`
	}
	entries := []dcs.Entry{
		{RelativePath: "initialize", ModRevision: 1, Value: []byte("12345")},
		{RelativePath: "config", ModRevision: 2, Value: []byte(configuration)},
		{RelativePath: "leader", ModRevision: 3, Lease: 31, Value: []byte("node-a")},
		{RelativePath: "sync", ModRevision: 4, Value: []byte(`{"leader":"node-a","sync_standby":"node-b"}`)},
		{RelativePath: "members/node-a", ModRevision: 5, Lease: 35, Value: []byte(`{"api_url":"http://node-a:8008/patroni","conn_url":"postgres://node-a:5433/postgres","role":"primary","state":"running","timeline":2,"version":"4.1.0","xlog_location":33554432}`)},
		{RelativePath: "members/node-b", ModRevision: 6, Lease: 36, Value: []byte(`{"api_url":"http://node-b:8008/patroni","conn_url":"postgres://node-b:5432/postgres","replication_state":"streaming","scheduled_restart":{"schedule":"2100-01-01T10:00:00+00:00","postgres_version":"99.0"},"state":"running","tags":{},"timeline":2,"version":"4.1.0","xlog_location":16777216` + pause + `}`)},
		{RelativePath: "history", ModRevision: 7, Value: []byte(`[[2,16777216,"manual","2026-07-13T10:00:00Z","node-a"]]`)},
		{RelativePath: "status", ModRevision: 8, Value: []byte(`{"optime":33554432}`)},
	}
	return &oracleStore{
		snapshot:  dcs.BuildSnapshot(target, "/service/alpha", 8, entries),
		mutations: make([]map[string]any, 0),
	}
}

func (store *oracleStore) Snapshot(ctx context.Context, target model.Target) (dcs.Snapshot, error) {
	if ctx == nil {
		return dcs.Snapshot{}, errors.New("nil context")
	}
	if store.removed {
		return dcs.BuildSnapshot(target.Normalize(), store.snapshot.Prefix, store.snapshot.Revision, nil), nil
	}
	return store.snapshot, nil
}

func (store *oracleStore) CompareAndSwapConfig(ctx context.Context, target model.Target, value []byte, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	entry, ok := store.snapshot.Entry("config")
	if !ok || expected == nil || entry.ModRevision != *expected {
		return dcs.WriteResult{Applied: false, Revision: store.snapshot.Revision}, nil
	}
	var decoded any
	if err := decodeCanonicalJSON(value, &decoded); err != nil {
		return dcs.WriteResult{}, err
	}
	store.mutations = append(store.mutations, map[string]any{
		"operation": "set_config_value", "value": decoded, "version": *expected,
	})
	store.replaceEntry("config", value)
	updated, _ := store.snapshot.Entry("config")
	return dcs.WriteResult{Applied: true, Revision: store.snapshot.Revision, Previous: &entry, Current: &updated}, nil
}

func (store *oracleStore) WriteFailover(ctx context.Context, target model.Target, value []byte, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	var decoded any
	if err := decodeCanonicalJSON(value, &decoded); err != nil {
		return dcs.WriteResult{}, err
	}
	store.mutations = append(store.mutations, map[string]any{
		"operation": "manual_failover", "value": decoded, "version": pointerValue(expected),
	})
	store.replaceEntry("failover", value)
	return dcs.WriteResult{Applied: true, Revision: store.snapshot.Revision}, nil
}

func (store *oracleStore) DeleteFailover(ctx context.Context, _ model.Target, expected *int64) (dcs.WriteResult, error) {
	if ctx == nil {
		return dcs.WriteResult{}, errors.New("nil context")
	}
	store.mutations = append(store.mutations, map[string]any{"operation": "delete_failover", "version": pointerValue(expected)})
	store.deleteEntry("failover")
	return dcs.WriteResult{Applied: true, Revision: store.snapshot.Revision}, nil
}

func (store *oracleStore) DeleteCluster(ctx context.Context, _ model.Target) (dcs.RemoveResult, error) {
	if ctx == nil {
		return dcs.RemoveResult{}, errors.New("nil context")
	}
	deleted := int64(len(store.snapshot.Entries))
	store.mutations = append(store.mutations, map[string]any{"operation": "delete_cluster"})
	store.removed = true
	store.snapshot.Revision++
	return dcs.RemoveResult{Deleted: deleted, Revision: store.snapshot.Revision}, nil
}

func (store *oracleStore) setScheduledRestart(member string, request *patroni.RestartRequest) {
	relative := "members/" + member
	entry, ok := store.snapshot.Entry(relative)
	if !ok {
		return
	}
	var document map[string]any
	if decodeCanonicalJSON(entry.Value, &document) != nil {
		return
	}
	if request == nil {
		delete(document, "scheduled_restart")
	} else if request.Schedule != "" {
		scheduled := map[string]any{"schedule": request.Schedule}
		if request.PostgresVersion != "" {
			scheduled["postgres_version"] = request.PostgresVersion
		}
		document["scheduled_restart"] = scheduled
	}
	encoded, _ := json.Marshal(document)
	store.replaceEntry(relative, encoded)
}

func (store *oracleStore) replaceEntry(relative string, value []byte) {
	entries := make([]dcs.Entry, 0, len(store.snapshot.Entries)+1)
	found := false
	store.snapshot.Revision++
	for _, entry := range store.snapshot.Entries {
		if entry.RelativePath == relative {
			entry.Value = append([]byte(nil), value...)
			entry.ModRevision = store.snapshot.Revision
			found = true
		}
		entries = append(entries, entry)
	}
	if !found {
		entries = append(entries, dcs.Entry{RelativePath: relative, ModRevision: store.snapshot.Revision, Value: append([]byte(nil), value...)})
	}
	store.snapshot = dcs.BuildSnapshot(store.snapshot.Target, store.snapshot.Prefix, store.snapshot.Revision, entries)
}

func (store *oracleStore) deleteEntry(relative string) {
	entries := make([]dcs.Entry, 0, len(store.snapshot.Entries))
	for _, entry := range store.snapshot.Entries {
		if entry.RelativePath != relative {
			entries = append(entries, entry)
		}
	}
	store.snapshot.Revision++
	store.snapshot = dcs.BuildSnapshot(store.snapshot.Target, store.snapshot.Prefix, store.snapshot.Revision, entries)
}

type oraclePatroni struct {
	status int
	calls  []oracleRESTCall
	store  *oracleStore
}

func (client *oraclePatroni) responseStatus() int {
	if client.status != 0 {
		return client.status
	}
	return 200
}

func (client *oraclePatroni) record(baseURL, method, endpoint string, data any) {
	client.calls = append(client.calls, oracleRESTCall{
		Member: memberFromBaseURL(baseURL), Method: strings.ToLower(method), Endpoint: endpoint, Data: canonicalJSON(data),
	})
}

func (client *oraclePatroni) GetPatroni(_ context.Context, baseURL string) (patroni.Response[patroni.Status], error) {
	client.record(baseURL, "get", "patroni", nil)
	serverVersion := 160001
	return patroni.Response[patroni.Status]{StatusCode: 200, Data: patroni.Status{
		Patroni: patroni.PatroniIdentity{Version: "4.1.0"}, ServerVersion: &serverVersion,
	}}, nil
}

func (client *oraclePatroni) PostReload(_ context.Context, baseURL string) (patroni.Response[string], error) {
	client.record(baseURL, "post", "reload", nil)
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) PostRestart(_ context.Context, baseURL string, request patroni.RestartRequest) (patroni.Response[string], error) {
	client.record(baseURL, "post", "restart", request)
	if client.store != nil {
		client.store.setScheduledRestart(memberFromBaseURL(baseURL), &request)
	}
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) DeleteRestart(_ context.Context, baseURL string) (patroni.Response[string], error) {
	client.record(baseURL, "delete", "restart", nil)
	if client.store != nil {
		client.store.setScheduledRestart(memberFromBaseURL(baseURL), nil)
	}
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) PostReinitialize(_ context.Context, baseURL string, request patroni.ReinitializeRequest) (patroni.Response[string], error) {
	client.record(baseURL, "post", "reinitialize", request)
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) PostFailover(_ context.Context, baseURL string, request patroni.FailoverRequest) (patroni.Response[string], error) {
	client.record(baseURL, "post", "failover", request)
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) PostSwitchover(_ context.Context, baseURL string, request patroni.FailoverRequest) (patroni.Response[string], error) {
	client.record(baseURL, "post", "switchover", request)
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) DeleteSwitchover(_ context.Context, baseURL string) (patroni.Response[string], error) {
	client.record(baseURL, "delete", "switchover", nil)
	return patroni.Response[string]{StatusCode: client.responseStatus()}, nil
}

func (client *oraclePatroni) PatchConfig(_ context.Context, baseURL string, patch patroni.DynamicConfig) (patroni.Response[patroni.DynamicConfig], error) {
	client.record(baseURL, "patch", "config", patch)
	return patroni.Response[patroni.DynamicConfig]{StatusCode: client.responseStatus(), Data: patch}, nil
}

func TestPatronictlSemanticParity(t *testing.T) {
	paths := []struct {
		name string
		env  string
		want string
	}{
		{name: "patroni-4.0", env: "GO_PATRONI_PATRONICTL_ORACLE_40", want: "4.0.10"},
		{name: "patroni-4.1", env: "GO_PATRONI_PATRONICTL_ORACLE_41", want: "4.1.4"},
	}
	for _, version := range paths {
		version := version
		t.Run(version.name, func(t *testing.T) {
			document := loadOracle(t, version.env)
			if document.PatroniVersion != version.want {
				t.Fatalf("oracle version = %q, want %q", document.PatroniVersion, version.want)
			}
			wantCases := 26
			if version.want == "4.1.4" {
				wantCases = 29
			}
			if len(document.Cases) != wantCases {
				t.Fatalf("oracle case count = %d, want %d", len(document.Cases), wantCases)
			}
			for _, testCase := range document.Cases {
				testCase := testCase
				t.Run(testCase.ID, func(t *testing.T) {
					if version.want == "4.0.10" && testCase.ID == "reinit" {
						if testCase.Exit != 2 || !strings.Contains(testCase.Output, "No such option '--from-leader'") {
							t.Fatalf("4.0 additive --from-leader boundary changed: %#v", testCase)
						}
						return
					}
					compareOracleCase(t, testCase)
				})
			}
		})
	}
}

func compareOracleCase(t *testing.T, oracle oracleCase) {
	t.Helper()
	paused := oracle.ID == "resume"
	store := newOracleStore(paused)
	rest := &oraclePatroni{store: store, calls: make([]oracleRESTCall, 0)}
	if oracle.ID == "reload_202" {
		rest.status = 202
	}
	database := &cliPostgres{result: postgres.QueryResult{Sets: []postgres.ResultSet{{
		Index: 0, Columns: []postgres.Column{{Name: "answer"}}, Rows: []postgres.Row{{{Text: "42", Bytes: 2}}}, CommandTag: "SELECT 1",
	}}}}
	service := newOracleCLIService(t, store, rest, database)
	stdout, stderr, err, _, _ := executeCLIForTest(t, oracle.Input, service, oracle.Args...)
	sdkSucceeded := err == nil
	if sdkSucceeded != (oracle.Exit == 0) {
		t.Fatalf("success class differs: Python patronictl exit=%d; Go patronictl err=%v code=%d\nstdout=%s\nstderr=%s\noracle=%s",
			oracle.Exit, err, exitCode(err), stdout, stderr, oracle.Output)
	}
	if !equalCanonical(oracle.REST, rest.calls) {
		t.Fatalf("REST facts differ\nPython patronictl=%s\nGo patronictl=%s", mustJSON(oracle.REST), mustJSON(rest.calls))
	}
	if !equalCanonical(oracle.DCS, store.mutations) {
		t.Fatalf("DCS facts differ\nPython patronictl=%s\nGo patronictl=%s", mustJSON(oracle.DCS), mustJSON(store.mutations))
	}
	compareOracleOutput(t, oracle, stdout, stderr, err)
}

func newOracleCLIService(t *testing.T, store *oracleStore, rest *oraclePatroni, database *cliPostgres) *control.Service {
	t.Helper()
	operation := 0
	service, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Patroni: rest, Postgres: database, Config: store, Failover: store, Remover: store,
		Clock: func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		NewOperationID: func() string {
			operation++
			return fmt.Sprintf("oracle-operation-%d", operation)
		},
		RandomIndex:          func(length int) (int, error) { return length - 1, nil },
		Wait:                 func(context.Context, time.Duration) error { return nil },
		VerificationAttempts: 2,
		ProductVersion:       "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func compareOracleOutput(t *testing.T, oracle oracleCase, stdout, stderr string, commandError error) {
	t.Helper()
	combined := stdout + stderr
	if commandError != nil {
		combined += "\nError: " + commandError.Error()
	}
	switch oracle.ID {
	case "dsn_default", "query_tsv":
		if stdout != oracle.Output {
			t.Fatalf("compatible text differs\nPython patronictl=%q\nGo patronictl=%q", oracle.Output, stdout)
		}
	case "dsn_selector_conflict", "query_missing_input", "demote_requires_source", "promote_primary_noop":
		if normalizedError(oracle.Output) != normalizedError(combined) {
			t.Fatalf("error semantic differs\nPython patronictl=%q\nGo patronictl=%q", normalizedError(oracle.Output), normalizedError(combined))
		}
	case "list_json":
		if !reflect.DeepEqual(normalizedListJSON(t, oracle.Output), normalizedListJSON(t, stdout)) {
			t.Fatalf("JSON member rows differ\nPython patronictl=%s\nGo patronictl=%s", oracle.Output, stdout)
		}
	case "list_tsv":
		if !reflect.DeepEqual(normalizedListTSV(oracle.Output), normalizedListTSV(stdout)) {
			t.Fatalf("TSV member rows differ\nPython patronictl=%s\nGo patronictl=%s", oracle.Output, stdout)
		}
	case "topology":
		for _, token := range []string{"node-a", "node-b", "Leader", "Sync Standby"} {
			if !strings.Contains(oracle.Output, token) || !strings.Contains(stdout, token) {
				t.Fatalf("topology omitted %q\nPython patronictl=%s\nGo patronictl=%s", token, oracle.Output, stdout)
			}
		}
	case "show_config":
		if !equalYAML(oracle.Output, stdout) {
			t.Fatalf("dynamic config differs\nPython patronictl=%s\nGo patronictl=%s", oracle.Output, stdout)
		}
	case "version_local":
		if !strings.HasPrefix(oracle.Output, "patronictl version ") || !strings.HasPrefix(stdout, "patronictl version ") {
			t.Fatalf("local product version contract differs\nPython patronictl=%s\nGo patronictl=%s", oracle.Output, stdout)
		}
	case "version_cluster":
		for _, token := range []string{"node-a: Patroni 4.1.0 PostgreSQL 16.1", "node-b: Patroni 4.1.0 PostgreSQL 16.1"} {
			if !strings.Contains(oracle.Output, token) || !strings.Contains(stdout, token) {
				t.Fatalf("cluster version omitted %q\nPython patronictl=%s\nGo patronictl=%s", token, oracle.Output, stdout)
			}
		}
	case "history_json":
		if !equalCanonical(jsonDocument(t, oracle.Output), jsonDocument(t, stdout)) {
			t.Fatalf("history JSON differs\nPython patronictl=%s\nGo patronictl=%s", oracle.Output, stdout)
		}
	case "reload_abort", "remove_abort", "demote_abort":
		if normalizedError(oracle.Output) != normalizedError(combined) {
			t.Fatalf("abort semantic differs\nPython patronictl=%q\nGo patronictl=%q", normalizedError(oracle.Output), normalizedError(combined))
		}
	case "reload_200":
		requireSharedToken(t, oracle.Output, stdout, "No changes to apply on member node-a")
	case "reload_202":
		requireSharedToken(t, oracle.Output, stdout, "Reload request received for member node-a and will be processed within 10 seconds")
	case "restart_immediate":
		requireSharedToken(t, oracle.Output, stdout, "Success: restart on member node-a")
	case "restart_scheduled_replace":
		requireSharedToken(t, oracle.Output, stdout, "Success: restart on member node-b")
	case "reinit":
		requireSharedToken(t, oracle.Output, stdout, "Success: reinitialize for member node-b")
	case "flush_restart":
		requireSharedToken(t, oracle.Output, stdout, "Success: flush scheduled restart for member node-b")
	case "flush_switchover_noop":
		if strings.TrimSpace(stdout) != strings.TrimSpace(oracle.Output) {
			t.Fatalf("flush no-op differs\nPython patronictl=%q\nGo patronictl=%q", oracle.Output, stdout)
		}
	case "pause", "resume", "edit_config":
		if strings.TrimSpace(stdout) != strings.TrimSpace(oracle.Output) {
			t.Fatalf("write confirmation differs\nPython patronictl=%q\nGo patronictl=%q", oracle.Output, stdout)
		}
	case "remove":
		if strings.Contains(stdout, "Removed cluster") {
			t.Fatalf("Go patronictl added an incompatible remove success line: %q", stdout)
		}
	case "failover", "switchover":
		if strings.TrimSpace(stdout) == "" {
			t.Fatalf("cluster transition returned no human evidence")
		}
	}
}

func loadOracle(t *testing.T, environment string) patronictlOracle {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(environment))
	if path == "" {
		t.Fatalf("%s is required by the oracle-tag compatibility suite", environment)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var document patronictlOracle
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

func memberFromBaseURL(baseURL string) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	return parsed.Hostname()
}

func pointerValue(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func canonicalJSON(value any) any {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	var decoded any
	if decodeCanonicalJSON(encoded, &decoded) != nil {
		return fmt.Sprint(value)
	}
	return decoded
}

func decodeCanonicalJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(output)
}

func equalCanonical(left, right any) bool {
	return string(mustJSON(left)) == string(mustJSON(right))
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(canonicalJSON(value))
	return encoded
}

func jsonDocument(t *testing.T, text string) any {
	t.Helper()
	var value any
	if err := decodeCanonicalJSON([]byte(text), &value); err != nil {
		t.Fatalf("decode JSON %q: %v", text, err)
	}
	return value
}

func normalizedListJSON(t *testing.T, text string) []map[string]string {
	t.Helper()
	value := jsonDocument(t, text)
	rows, ok := value.([]any)
	if !ok {
		t.Fatalf("member JSON is not an array: %#v", value)
	}
	result := make([]map[string]string, 0, len(rows))
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("member JSON row is not an object: %#v", raw)
		}
		result = append(result, normalizedMemberRow(row))
	}
	return result
}

func normalizedListTSV(text string) []map[string]string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) < 2 {
		return nil
	}
	headers := strings.Split(lines[0], "\t")
	result := make([]map[string]string, 0, len(lines)-1)
	for _, line := range lines[1:] {
		fields := strings.Split(line, "\t")
		row := make(map[string]any, len(headers))
		for index, header := range headers {
			if index < len(fields) {
				row[header] = fields[index]
			}
		}
		result = append(result, normalizedMemberRow(row))
	}
	return result
}

func normalizedMemberRow(row map[string]any) map[string]string {
	result := map[string]string{}
	for _, key := range []string{"Cluster", "Member", "Host", "Role", "State", "TL", "Scheduled restart"} {
		if value := strings.TrimSpace(fmt.Sprint(row[key])); value != "" && value != "<nil>" {
			result[key] = value
		}
	}
	lag := row["Receive Lag"]
	if lag == nil {
		lag = row["Lag in MB"]
	}
	if value := strings.TrimSpace(fmt.Sprint(lag)); value != "" && value != "<nil>" && value != "0" {
		result["Lag"] = value
	}
	return result
}

func equalYAML(left, right string) bool {
	var leftValue, rightValue any
	if yaml.Unmarshal([]byte(left), &leftValue) != nil || yaml.Unmarshal([]byte(right), &rightValue) != nil {
		return false
	}
	return equalCanonical(leftValue, rightValue)
}

func normalizedError(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if position := strings.Index(line, "Error:"); position >= 0 {
			line = strings.TrimSpace(line[position+len("Error:"):])
			return strings.ToLower(strings.TrimSuffix(line, "."))
		}
	}
	return ""
}

func requireSharedToken(t *testing.T, oracleOutput, goOutput, token string) {
	t.Helper()
	if !strings.Contains(oracleOutput, token) {
		t.Fatalf("oracle no longer contains expected semantic token %q: %s", token, oracleOutput)
	}
	if !strings.Contains(goOutput, token) {
		t.Fatalf("Go patronictl omitted semantic token %q: %s", token, goOutput)
	}
}

func sortedKeys[V any](input map[string]V) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func intText(value any) string {
	switch typed := value.(type) {
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(value)
	}
}
