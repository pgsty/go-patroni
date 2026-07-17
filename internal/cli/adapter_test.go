package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	sdkversion "github.com/pgsty/go-patroni/internal/version"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
	"go.yaml.in/yaml/v3"
)

type cliSnapshotReader struct {
	snapshot       dcs.Snapshot
	err            error
	calls          []model.Target
	discoveries    []dcs.DiscoveredCluster
	discoveryErr   error
	discoveryCalls []dcs.DiscoveryRequest
}

func (reader *cliSnapshotReader) Discover(ctx context.Context, request dcs.DiscoveryRequest) ([]dcs.DiscoveredCluster, error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	reader.discoveryCalls = append(reader.discoveryCalls, request)
	if reader.discoveryErr != nil {
		return nil, reader.discoveryErr
	}
	return append([]dcs.DiscoveredCluster(nil), reader.discoveries...), nil
}

type cliClusterRemover struct {
	reader *cliSnapshotReader
	calls  int
}

func (remover *cliClusterRemover) DeleteCluster(ctx context.Context, target model.Target) (dcs.RemoveResult, error) {
	if ctx == nil {
		return dcs.RemoveResult{}, errors.New("nil context")
	}
	remover.calls++
	deleted := int64(len(remover.reader.snapshot.Entries))
	remover.reader.snapshot = dcs.BuildSnapshot(target.Normalize(), remover.reader.snapshot.Prefix, remover.reader.snapshot.Revision+1, nil)
	return dcs.RemoveResult{Deleted: deleted, Revision: remover.reader.snapshot.Revision}, nil
}

func (reader *cliSnapshotReader) Snapshot(ctx context.Context, target model.Target) (dcs.Snapshot, error) {
	if ctx == nil {
		return dcs.Snapshot{}, errors.New("nil context")
	}
	reader.calls = append(reader.calls, target)
	if reader.err != nil {
		return dcs.Snapshot{}, reader.err
	}
	return reader.snapshot, nil
}

type cliPatroni struct {
	reloadResponse patroni.Response[string]
	reloadError    error
	reloadCalls    []string
}

func (client *cliPatroni) GetPatroni(context.Context, string) (patroni.Response[patroni.Status], error) {
	return patroni.Response[patroni.Status]{StatusCode: 200, Data: patroni.Status{Patroni: patroni.PatroniIdentity{Version: "4.1.0"}}}, nil
}

func (client *cliPatroni) PostReload(_ context.Context, baseURL string) (patroni.Response[string], error) {
	client.reloadCalls = append(client.reloadCalls, baseURL)
	return client.reloadResponse, client.reloadError
}

func (*cliPatroni) PostRestart(context.Context, string, patroni.RestartRequest) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) DeleteRestart(context.Context, string) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) PostReinitialize(context.Context, string, patroni.ReinitializeRequest) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) PostFailover(context.Context, string, patroni.FailoverRequest) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) PostSwitchover(context.Context, string, patroni.FailoverRequest) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) DeleteSwitchover(context.Context, string) (patroni.Response[string], error) {
	return patroni.Response[string]{StatusCode: 200}, nil
}

func (*cliPatroni) PatchConfig(_ context.Context, _ string, patch patroni.DynamicConfig) (patroni.Response[patroni.DynamicConfig], error) {
	return patroni.Response[patroni.DynamicConfig]{StatusCode: 200, Data: patch}, nil
}

type cliPostgres struct {
	result      postgres.QueryResult
	err         error
	connections []postgres.ConnectionOptions
	expected    []postgres.RecoveryExpectation
	requests    []postgres.QueryRequest
}

func (client *cliPostgres) QueryChecked(
	ctx context.Context,
	connection postgres.ConnectionOptions,
	expectation postgres.RecoveryExpectation,
	request postgres.QueryRequest,
) (postgres.QueryResult, error) {
	if ctx == nil {
		return postgres.QueryResult{}, errors.New("nil context")
	}
	client.connections = append(client.connections, connection)
	client.expected = append(client.expected, expectation)
	client.requests = append(client.requests, request)
	return client.result, client.err
}

func cliFixtureSnapshot() dcs.Snapshot {
	target := (model.Target{Context: "lab", Namespace: "/service", Scope: "alpha"}).Normalize()
	entries := []dcs.Entry{
		{RelativePath: "initialize", ModRevision: 1, Value: []byte("12345")},
		{RelativePath: "config", ModRevision: 2, Value: []byte(`{"loop_wait":10,"synchronous_mode":true}`)},
		{RelativePath: "leader", ModRevision: 3, Lease: 31, Value: []byte("node-a")},
		{RelativePath: "sync", ModRevision: 4, Value: []byte(`{"leader":"node-a","sync_standby":"node-b"}`)},
		{RelativePath: "members/node-a", ModRevision: 5, Lease: 35, Value: []byte(`{"api_url":"https://node-a:8008/patroni","conn_url":"postgres://fixture-user:fixture-placeholder@node-a:5433/postgres","state":"running","role":"primary","timeline":2,"version":"4.1.0"}`)},
		{RelativePath: "members/node-b", ModRevision: 6, Lease: 36, Value: []byte(`{"api_url":"https://node-b:8008/patroni","conn_url":"postgres://node-b:5432/postgres","state":"running","replication_state":"streaming","timeline":2,"version":"4.1.0"}`)},
		{RelativePath: "history", ModRevision: 7, Value: []byte(`[[2,16777216,"manual","2026-07-13T10:00:00Z","node-a"]]`)},
	}
	return dcs.BuildSnapshot(target, "/service/alpha", 8, entries)
}

func newCLIService(t *testing.T, snapshots *cliSnapshotReader, rest *cliPatroni, database *cliPostgres) *control.Service {
	t.Helper()
	operation := 0
	service, err := control.NewService(control.ServiceOptions{
		Snapshots: snapshots, Discovery: snapshots, Patroni: rest, Postgres: database,
		Clock: func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		NewOperationID: func() string {
			operation++
			return fmt.Sprintf("cli-operation-%d", operation)
		},
		Wait:                 func(context.Context, time.Duration) error { return nil },
		VerificationAttempts: 2,
		ProductVersion:       "v0.1.0-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func cliRuntimeFactory(service *control.Service, invocations *[]runtimeInvocation, closes *int) runtimeFactory {
	return func(_ context.Context, invocation runtimeInvocation) (*commandRuntime, error) {
		*invocations = append(*invocations, invocation)
		scope := invocation.request.explicitScope
		if scope == "" {
			scope = "alpha"
		}
		target := (model.Target{Context: "lab", Namespace: "/service", Scope: scope, Group: cloneIntPointer(invocation.request.explicitGroup)}).Normalize()
		return &commandRuntime{
			service: service, target: target,
			resolved: config.Resolved{Context: "lab", Namespace: "/service", Scope: "alpha", Group: cloneIntPointer(invocation.request.explicitGroup)},
			warnings: []string{},
			close: func() error {
				*closes++
				return nil
			},
		}, nil
	}
}

func executeCLIForTest(
	t *testing.T,
	stdin string,
	service *control.Service,
	args ...string,
) (string, string, error, []runtimeInvocation, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	invocations := make([]runtimeInvocation, 0, 1)
	closes := 0
	root := newRootCommand(strings.NewReader(stdin), &stdout, &stderr, cliRuntimeFactory(service, &invocations, &closes))
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return stdout.String(), stderr.String(), err, invocations, closes
}

func TestReadAdaptersUseControlAndPreserveMachineSecretBoundary(t *testing.T) {
	snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
	rest := &cliPatroni{}
	database := &cliPostgres{result: postgres.QueryResult{Sets: []postgres.ResultSet{{
		Index: 0, Columns: []postgres.Column{{Name: "answer"}}, Rows: []postgres.Row{{{Text: "42", Bytes: 2}}}, CommandTag: "SELECT 1",
	}}}}
	service := newCLIService(t, snapshots, rest, database)

	stdout, stderr, err, invocations, closes := executeCLIForTest(t, "", service, "dsn", "alpha", "--member", "node-b")
	if err != nil || stdout != "host=node-b port=5432\n" || stderr != "" {
		t.Fatalf("dsn adapter mismatch: stdout=%q stderr=%q err=%v", stdout, stderr, err)
	}
	if len(invocations) != 1 || invocations[0].request.operation != config.OperationClusterRead || invocations[0].request.explicitScope != "alpha" || closes != 1 {
		t.Fatalf("dsn invocation/close mismatch: %#v closes=%d", invocations, closes)
	}

	const sqlMarker = "select 'sql-private-marker'"
	const passwordMarker = "password-private-marker"
	stdout, stderr, err, _, closes = executeCLIForTest(t, "", service,
		"--output", "json", "query", "alpha", "--command", sqlMarker, "--username", "operator")
	if err != nil {
		t.Fatalf("query adapter failed: %v stdout=%s stderr=%s", err, stdout, stderr)
	}
	var envelope map[string]any
	if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil {
		t.Fatalf("query machine output is not JSON: %v output=%q", decodeErr, stdout)
	}
	if envelope["apiVersion"] != machineAPIVersion || envelope["kind"] != "QueryResult" || envelope["error"] != nil {
		t.Fatalf("query envelope mismatch: %#v", envelope)
	}
	if strings.Contains(stdout+stderr, sqlMarker) || strings.Contains(stdout+stderr, passwordMarker) {
		t.Fatalf("query secret reached adapter output: stdout=%q stderr=%q", stdout, stderr)
	}
	if stderr != "" || closes != 1 || len(database.requests) != 1 || database.requests[0].SQL != sqlMarker {
		t.Fatalf("machine query request mismatch: stderr=%q closes=%d requests=%#v", stderr, closes, database.requests)
	}

	stdout, stderr, err, _, closes = executeCLIForTest(t, passwordMarker+"\n", service,
		"query", "alpha", "--command", sqlMarker, "--password", "--username", "operator", "--format", "json")
	if err != nil || stderr != "Password: " || closes != 1 || len(database.requests) != 2 || database.requests[1].SQL != sqlMarker {
		t.Fatalf("interactive password query mismatch: err=%v stdout=%q stderr=%q closes=%d requests=%#v",
			err, stdout, stderr, closes, database.requests)
	}
	if strings.Contains(stdout+stderr, sqlMarker) || strings.Contains(stdout+stderr, passwordMarker) {
		t.Fatalf("interactive query secret reached output: stdout=%q stderr=%q", stdout, stderr)
	}
	if got := database.connections[1].String(); !strings.Contains(got, "password:true") || strings.Contains(got, passwordMarker) {
		t.Fatalf("password connection boundary mismatch: %s", got)
	}
}

func TestMachineReadFailureIsAnEnvelopeWithExactExitCategory(t *testing.T) {
	snapshots := &cliSnapshotReader{err: dcs.NewError(dcs.ErrorTransport, "snapshot", "/service/alpha", errors.New("fixture transport detail"))}
	service := newCLIService(t, snapshots, &cliPatroni{}, nil)
	stdout, stderr, err, _, closes := executeCLIForTest(t, "", service, "--output", "json", "list", "alpha")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnreachable) || stderr != "" || closes != 1 {
		t.Fatalf("machine failure exit mismatch: err=%v code=%d stderr=%q closes=%d", err, exitCode(err), stderr, closes)
	}
	var envelope struct {
		Kind  string `json:"kind"`
		Error struct {
			Category control.Category `json:"category"`
			Message  string           `json:"message"`
		} `json:"error"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "Error" || envelope.Error.Category != control.CategoryUnreachable {
		t.Fatalf("machine error envelope mismatch: decode=%v envelope=%#v output=%q", decodeErr, envelope, stdout)
	}
	if strings.Contains(stdout, "fixture transport detail") {
		t.Fatalf("underlying transport detail leaked: %s", stdout)
	}
}

func TestWriteAdapterCannotBypassConfirmationAndDoesNotRetryUnknown(t *testing.T) {
	snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
	rest := &cliPatroni{reloadResponse: patroni.Response[string]{StatusCode: 200}}
	service := newCLIService(t, snapshots, rest, nil)

	stdout, stderr, err, _, _ := executeCLIForTest(t, "n\n", service, "reload", "alpha", "node-a")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryFailed) || len(rest.reloadCalls) != 0 || stdout != "" {
		t.Fatalf("aborted reload crossed write boundary: stdout=%q stderr=%q err=%v calls=%v", stdout, stderr, err, rest.reloadCalls)
	}
	for _, required := range []string{"Plan:", "context=lab", "scope=alpha", "node-a"} {
		if !strings.Contains(stderr, required) {
			t.Errorf("confirmation omitted %q: %s", required, stderr)
		}
	}

	rest.reloadError = errors.New("ambiguous fixture transport failure")
	rest.reloadResponse = patroni.Response[string]{}
	stdout, stderr, err, _, _ = executeCLIForTest(t, "", service,
		"--output", "json", "reload", "alpha", "node-a", "--force")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnknown) || stderr != "" {
		t.Fatalf("UNKNOWN reload exit mismatch: err=%v code=%d stdout=%s stderr=%q", err, exitCode(err), stdout, stderr)
	}
	if len(rest.reloadCalls) != 1 {
		t.Fatalf("ambiguous reload was retried: calls=%v", rest.reloadCalls)
	}
	var envelope struct {
		Kind  string `json:"kind"`
		Error struct {
			Category control.Category `json:"category"`
			Cause    string           `json:"cause"`
			Actions  []string         `json:"nextActions"`
		} `json:"error"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Kind != "Error" || envelope.Error.Category != control.CategoryUnknown {
		t.Fatalf("UNKNOWN envelope mismatch: decode=%v envelope=%#v output=%q", decodeErr, envelope, stdout)
	}
	if envelope.Error.Cause != "OUTCOME_UNCONFIRMED" || len(envelope.Error.Actions) != 2 || !strings.Contains(envelope.Error.Actions[0], "not retry blindly") {
		t.Fatalf("UNKNOWN safe cause/actions mismatch: %#v", envelope.Error)
	}
}

func TestFormatAndMachineOutputAreMutuallyExclusiveBeforeRuntime(t *testing.T) {
	service := newCLIService(t, &cliSnapshotReader{snapshot: cliFixtureSnapshot()}, &cliPatroni{}, nil)
	_, _, err, invocations, closes := executeCLIForTest(t, "", service, "--output", "json", "list", "alpha", "--format", "yaml")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || len(invocations) != 0 || closes != 0 {
		t.Fatalf("format/output conflict crossed runtime: err=%v invocations=%#v closes=%d", err, invocations, closes)
	}
}

func TestWriteInputParsersFreezeCompatibilitySemantics(t *testing.T) {
	settings, err := parseConfigSettings([]string{"ttl=30", "loop_wait=null"}, []string{"max.connections=200"})
	if err != nil || len(settings) != 3 || settings[2].Path != "postgresql.parameters.max.connections" || !reflect.DeepEqual(settings[1].Value, nil) {
		t.Fatalf("config setting parse mismatch: settings=%#v err=%v", settings, err)
	}
	if _, err := parseConfigSettings([]string{"missing-separator"}, nil); err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) {
		t.Fatalf("invalid setting error mismatch: %v", err)
	}
	parsed, err := parseScheduled("2026-07-13T21:30:00+08:00")
	if err != nil || parsed == nil || parsed.Format(time.RFC3339) != "2026-07-13T21:30:00+08:00" {
		t.Fatalf("schedule parse mismatch: %v err=%v", parsed, err)
	}
	if parsed, err := parseScheduled("now"); err != nil || parsed != nil {
		t.Fatalf("now schedule mismatch: %v err=%v", parsed, err)
	}
	if _, err := parseScheduled("not-a-time"); err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) {
		t.Fatalf("invalid schedule error mismatch: %v", err)
	}
}

func TestMachineEnvelopeGoldensCoverLocalUsageAndRuntimeFailures(t *testing.T) {
	fixedTime := time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC)
	fixedID := func() string { return "cli-request-1" }
	execute := func(factory runtimeFactory, args ...string) (string, string, error) {
		var stdout, stderr bytes.Buffer
		root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr, factory,
			func() time.Time { return fixedTime }, fixedID)
		root.SetArgs(args)
		err := root.ExecuteContext(context.Background())
		return stdout.String(), stderr.String(), err
	}

	oldVersion, oldCommit, oldBuildTime := sdkversion.Version, sdkversion.Commit, sdkversion.BuildTime
	sdkversion.Version, sdkversion.Commit, sdkversion.BuildTime = "1.2.3-test", "abc123fixture", "2026-07-13T12:00:00Z"
	defer func() {
		sdkversion.Version, sdkversion.Commit, sdkversion.BuildTime = oldVersion, oldCommit, oldBuildTime
	}()

	t.Run("local-version", func(t *testing.T) {
		factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
			t.Fatal("local version opened a runtime")
			return nil, nil
		}
		stdout, stderr, err := execute(factory, "--output", "json", "version")
		if err != nil || stderr != "" {
			t.Fatalf("local machine version failed: err=%v stderr=%q output=%s", err, stderr, stdout)
		}
		normalized := strings.ReplaceAll(stdout, sdkversion.Current().GoVersion, "<go-version>")
		requireGolden(t, "testdata/machine-local-version.golden.json", normalized)
	})

	t.Run("cluster-version", func(t *testing.T) {
		snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
		service := newCLIService(t, snapshots, &cliPatroni{}, nil)
		invocations, closes := make([]runtimeInvocation, 0, 1), 0
		stdout, stderr, err := execute(cliRuntimeFactory(service, &invocations, &closes),
			"--output", "json", "version", "alpha")
		if err != nil || stderr != "" || closes != 1 {
			t.Fatalf("cluster machine version failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
		}
		normalized := strings.ReplaceAll(stdout, sdkversion.Current().GoVersion, "<go-version>")
		requireGolden(t, "testdata/machine-cluster-version.golden.json", normalized)
	})

	t.Run("cluster-list", func(t *testing.T) {
		snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
		service := newCLIService(t, snapshots, &cliPatroni{}, nil)
		invocations, closes := make([]runtimeInvocation, 0, 1), 0
		stdout, stderr, err := execute(cliRuntimeFactory(service, &invocations, &closes),
			"--output", "json", "list", "alpha")
		if err != nil || stderr != "" || closes != 1 {
			t.Fatalf("cluster machine list failed: err=%v stderr=%q closes=%d output=%s", err, stderr, closes, stdout)
		}
		requireGolden(t, "testdata/machine-cluster-list.golden.json", stdout)
	})

	t.Run("usage-error", func(t *testing.T) {
		factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
			t.Fatal("query usage error opened a runtime")
			return nil, nil
		}
		stdout, stderr, err := execute(factory, "--output", "json", "query", "alpha")
		if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stderr != "" || !errorWasRendered(err) {
			t.Fatalf("usage envelope exit mismatch: err=%v code=%d rendered=%t stderr=%q", err, exitCode(err), errorWasRendered(err), stderr)
		}
		requireGolden(t, "testdata/machine-usage-error.golden.json", stdout)
	})

	t.Run("cobra-argument-and-flag-errors", func(t *testing.T) {
		factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
			t.Fatal("Cobra parse error opened a runtime")
			return nil, nil
		}
		for _, arguments := range [][]string{
			{"--output", "json", "not-a-command"},
			{"--output", "json", "list", "alpha", "--not-a-flag"},
			{"--output", "json", "remove"},
		} {
			stdout, stderr, err := execute(factory, arguments...)
			if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stderr != "" || !errorWasRendered(err) {
				t.Fatalf("Cobra usage envelope mismatch: args=%v err=%v code=%d stderr=%q output=%s",
					arguments, err, exitCode(err), stderr, stdout)
			}
			var envelope struct {
				Error struct {
					Category control.Category `json:"category"`
				} `json:"error"`
			}
			if decodeErr := json.Unmarshal([]byte(stdout), &envelope); decodeErr != nil || envelope.Error.Category != control.CategoryUsage {
				t.Fatalf("Cobra usage category mismatch: args=%v decode=%v output=%s", arguments, decodeErr, stdout)
			}
			if strings.Contains(stdout, "\x1b[") {
				t.Fatalf("machine usage output contains ANSI: %q", stdout)
			}
		}
	})

	t.Run("runtime-error", func(t *testing.T) {
		factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
			return nil, dcs.NewError(dcs.ErrorTransport, "snapshot", "/service/alpha", errors.New("private transport detail"))
		}
		stdout, stderr, err := execute(factory, "--output", "json", "list", "alpha")
		if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnreachable) || stderr != "" || !errorWasRendered(err) {
			t.Fatalf("runtime envelope exit mismatch: err=%v code=%d rendered=%t stderr=%q", err, exitCode(err), errorWasRendered(err), stderr)
		}
		if strings.Contains(stdout, "private transport detail") {
			t.Fatalf("runtime cause leaked into machine output: %s", stdout)
		}
		requireGolden(t, "testdata/machine-runtime-error.golden.json", stdout)
	})
}

func TestMachineYAMLHasTheSameEnvelopeAndNeverUsesNullCollections(t *testing.T) {
	factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
		return nil, dcs.NewError(dcs.ErrorTransport, "snapshot", "/service/alpha", errors.New("private detail"))
	}
	var stdout, stderr bytes.Buffer
	root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr, factory,
		func() time.Time { return time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC) }, func() string { return "cli-request-yaml" })
	root.SetArgs([]string{"--output", "yaml", "list", "alpha"})
	err := root.ExecuteContext(context.Background())
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnreachable) || stderr.String() != "" {
		t.Fatalf("YAML runtime envelope mismatch: err=%v stderr=%q output=%s", err, stderr.String(), stdout.String())
	}
	var envelope map[string]any
	if decodeErr := yaml.Unmarshal(stdout.Bytes(), &envelope); decodeErr != nil {
		t.Fatalf("decode YAML envelope: %v output=%s", decodeErr, stdout.String())
	}
	metadata, _ := envelope["metadata"].(map[string]any)
	publicError, _ := envelope["error"].(map[string]any)
	if envelope["apiVersion"] != machineAPIVersion || envelope["kind"] != "Error" ||
		metadata["warnings"] == nil || publicError["evidence"] == nil {
		t.Fatalf("YAML envelope null/stability mismatch: %#v", envelope)
	}
}

func TestMachineRuntimeCategoryDoesNotCollapseSharedExitCodes(t *testing.T) {
	tests := []struct {
		name     string
		runtime  error
		category control.Category
	}{
		{
			name: "configuration",
			runtime: &config.ValidationError{
				Operation: config.OperationClusterRead, Field: "scope", Reason: "fixture",
			},
			category: control.CategoryConfig,
		},
		{
			name:     "tls",
			runtime:  &patroni.TLSConfigError{Field: "cacert"},
			category: control.CategoryTLS,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
				return nil, test.runtime
			}
			var stdout, stderr bytes.Buffer
			root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr, factory,
				func() time.Time { return time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC) },
				func() string { return "cli-request-category" })
			root.SetArgs([]string{"--output", "json", "list", "alpha"})
			err := root.ExecuteContext(context.Background())
			if err == nil || exitCode(err) != control.ExitCode(test.category) || stderr.String() != "" {
				t.Fatalf("machine category exit mismatch: err=%v code=%d stderr=%q output=%s",
					err, exitCode(err), stderr.String(), stdout.String())
			}
			var envelope struct {
				Error struct {
					Category control.Category `json:"category"`
				} `json:"error"`
			}
			if decodeErr := json.Unmarshal(stdout.Bytes(), &envelope); decodeErr != nil || envelope.Error.Category != test.category {
				t.Fatalf("machine category mismatch: decode=%v category=%q want=%q output=%s",
					decodeErr, envelope.Error.Category, test.category, stdout.String())
			}
		})
	}
}

func TestNonInteractiveAndMachinePromptPolicyStopsBeforeWrite(t *testing.T) {
	fixedTime := time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC)
	execute := func(t *testing.T, interactive bool, stdin string, factory runtimeFactory, args ...string) (string, string, error) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		root := newRootCommandWithAllBoundaries(strings.NewReader(stdin), &stdout, &stderr, factory,
			func() time.Time { return fixedTime }, func() string { return "cli-prompt-policy" }, func() bool { return interactive })
		root.SetArgs(args)
		err := root.ExecuteContext(context.Background())
		return stdout.String(), stderr.String(), err
	}

	snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
	rest := &cliPatroni{reloadResponse: patroni.Response[string]{StatusCode: 200}}
	service := newCLIService(t, snapshots, rest, nil)
	invocations, closes := make([]runtimeInvocation, 0, 1), 0
	factory := cliRuntimeFactory(service, &invocations, &closes)

	stdout, stderr, err := execute(t, false, "y\n", factory, "reload", "alpha", "node-a")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stdout != "" || stderr != "" || len(rest.reloadCalls) != 0 {
		t.Fatalf("non-TTY reload crossed prompt/write boundary: err=%v stdout=%q stderr=%q calls=%v", err, stdout, stderr, rest.reloadCalls)
	}

	stdout, stderr, err = execute(t, true, "y\n", factory, "--output", "json", "reload", "alpha", "node-a")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stderr != "" || len(rest.reloadCalls) != 0 {
		t.Fatalf("machine reload crossed prompt/write boundary: err=%v stdout=%s stderr=%q calls=%v", err, stdout, stderr, rest.reloadCalls)
	}
	var machineError struct {
		Error struct {
			Category control.Category `json:"category"`
		} `json:"error"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &machineError); decodeErr != nil || machineError.Error.Category != control.CategoryUsage {
		t.Fatalf("machine prompt error mismatch: decode=%v output=%s", decodeErr, stdout)
	}

	neverOpen := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
		t.Fatal("non-interactive prompt failure opened runtime")
		return nil, nil
	}
	stdout, stderr, err = execute(t, false, "private-password\n", neverOpen,
		"query", "alpha", "--command", "select 1", "--password")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stdout != "" || stderr != "" {
		t.Fatalf("non-TTY password prompt mismatch: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	stdout, stderr, err = execute(t, false, "alpha\nYes I am aware\nnode-a\n", neverOpen, "remove", "alpha")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stdout != "" || stderr != "" {
		t.Fatalf("non-TTY remove prompt mismatch: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
}

func TestNonInteractiveRemoveRequiresAndAcceptsExactExplicitConfirmations(t *testing.T) {
	snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
	remover := &cliClusterRemover{reader: snapshots}
	operation := 0
	service, err := control.NewService(control.ServiceOptions{
		Snapshots: snapshots, Remover: remover,
		Clock: func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) },
		NewOperationID: func() string {
			operation++
			return fmt.Sprintf("remove-operation-%d", operation)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	invocations, closes := make([]runtimeInvocation, 0, 1), 0
	var stdout, stderr bytes.Buffer
	root := newRootCommandWithAllBoundaries(strings.NewReader(""), &stdout, &stderr,
		cliRuntimeFactory(service, &invocations, &closes),
		func() time.Time { return time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC) },
		func() string { return "remove-request" }, func() bool { return false })
	root.SetArgs([]string{
		"--output", "json", "remove", "alpha",
		"--confirm-cluster", "alpha", "--acknowledge-removal", control.RemoveAcknowledgement, "--confirm-leader", "node-a",
	})
	err = root.ExecuteContext(context.Background())
	if err != nil || stderr.String() != "" || remover.calls != 1 || closes != 1 {
		t.Fatalf("explicit noninteractive remove mismatch: err=%v stderr=%q calls=%d closes=%d output=%s",
			err, stderr.String(), remover.calls, closes, stdout.String())
	}
	var envelope map[string]any
	if decodeErr := json.Unmarshal(stdout.Bytes(), &envelope); decodeErr != nil || envelope["kind"] != "RemoveResult" || envelope["error"] != nil {
		t.Fatalf("explicit remove envelope mismatch: decode=%v output=%s", decodeErr, stdout.String())
	}
}

func TestMachineOutputIsInvariantAcrossLocaleAndTerminalEnvironment(t *testing.T) {
	render := func(t *testing.T, format, locale, columns, color string) string {
		t.Helper()
		t.Setenv("LC_ALL", locale)
		t.Setenv("LANG", locale)
		t.Setenv("COLUMNS", columns)
		t.Setenv("TERM", color)
		t.Setenv("NO_COLOR", "")
		snapshots := &cliSnapshotReader{snapshot: cliFixtureSnapshot()}
		service := newCLIService(t, snapshots, &cliPatroni{}, nil)
		invocations, closes := make([]runtimeInvocation, 0, 1), 0
		var stdout, stderr bytes.Buffer
		root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr,
			cliRuntimeFactory(service, &invocations, &closes),
			func() time.Time { return time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC) },
			func() string { return "environment-invariant" })
		root.SetArgs([]string{"--output", format, "list", "alpha"})
		if err := root.ExecuteContext(context.Background()); err != nil || stderr.String() != "" || closes != 1 {
			t.Fatalf("machine environment fixture failed: err=%v stderr=%q closes=%d output=%s", err, stderr.String(), closes, stdout.String())
		}
		if strings.Contains(stdout.String(), "\x1b[") {
			t.Fatalf("machine output contains ANSI: %q", stdout.String())
		}
		return stdout.String()
	}
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			baseline := render(t, format, "C", "40", "xterm-256color")
			variant := render(t, format, "zh_CN.UTF-8", "240", "dumb")
			if baseline != variant {
				t.Fatalf("machine %s changed with locale/terminal\nbaseline=%s\nvariant=%s", format, baseline, variant)
			}
		})
	}
}

func TestEveryPatronictlCommandHasVersionedMachineUsageEnvelope(t *testing.T) {
	manifest := loadCommandManifest(t)
	if len(manifest.Commands) != 19 {
		t.Fatalf("machine contract expected 19 commands, got %d", len(manifest.Commands))
	}
	for _, contract := range manifest.Commands {
		t.Run(contract.Command, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
				t.Fatal("machine parse contract opened runtime")
				return nil, nil
			}
			root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr, factory,
				func() time.Time { return time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC) },
				func() string { return "all-command-machine-contract" })
			root.SetArgs([]string{"--output", "json", contract.Command, "--definitely-not-a-patronictl-flag"})
			err := root.ExecuteContext(context.Background())
			if err == nil || exitCode(err) != control.ExitCode(control.CategoryUsage) || stderr.String() != "" || !errorWasRendered(err) {
				t.Fatalf("machine usage boundary mismatch: err=%v code=%d stderr=%q output=%s",
					err, exitCode(err), stderr.String(), stdout.String())
			}
			var envelope struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Metadata   struct {
					Warnings []string `json:"warnings"`
				} `json:"metadata"`
				Error struct {
					Category    control.Category   `json:"category"`
					Cause       string             `json:"cause"`
					Evidence    []control.Evidence `json:"evidence"`
					NextActions []string           `json:"nextActions"`
				} `json:"error"`
			}
			if decodeErr := json.Unmarshal(stdout.Bytes(), &envelope); decodeErr != nil ||
				envelope.APIVersion != machineAPIVersion || envelope.Kind != "Error" ||
				envelope.Error.Category != control.CategoryUsage || envelope.Error.Cause != "INVALID_INPUT" ||
				envelope.Metadata.Warnings == nil || envelope.Error.Evidence == nil || envelope.Error.NextActions == nil {
				t.Fatalf("machine envelope mismatch: decode=%v envelope=%#v output=%s", decodeErr, envelope, stdout.String())
			}
			if strings.Contains(stdout.String(), "\x1b[") {
				t.Fatalf("machine envelope contains ANSI: %q", stdout.String())
			}
			normalized := strings.Replace(stdout.String(), `"operation":"`+contract.Command+`"`, `"operation":"<command>"`, 1)
			requireGolden(t, "testdata/machine-command-usage.golden.json", normalized)
		})
	}
}

func TestCLIUnsupportedReadOptInNeverOverridesWriteFailClosed(t *testing.T) {
	unsupported := cliSnapshotWithVersion(t, cliFixtureSnapshot(), "5.0.0")
	snapshots := &cliSnapshotReader{snapshot: unsupported}
	rest := &cliPatroni{reloadResponse: patroni.Response[string]{StatusCode: 200}}
	database := &cliPostgres{}
	service := newCLIService(t, snapshots, rest, database)

	stdout, stderr, err, _, _ := executeCLIForTest(t, "", service, "--output", "json", "list", "alpha")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnsupported) || stderr != "" {
		t.Fatalf("unsupported list was not blocked: err=%v code=%d stderr=%q output=%s", err, exitCode(err), stderr, stdout)
	}
	var blocked struct {
		Error struct {
			Category control.Category `json:"category"`
		} `json:"error"`
	}
	if decodeErr := json.Unmarshal([]byte(stdout), &blocked); decodeErr != nil || blocked.Error.Category != control.CategoryUnsupported {
		t.Fatalf("unsupported list envelope mismatch: decode=%v output=%s", decodeErr, stdout)
	}

	stdout, stderr, err, _, _ = executeCLIForTest(t, "", service, "--allow-unsupported-read", "--output", "json", "list", "alpha")
	if err != nil || stderr != "" {
		t.Fatalf("explicit unsupported read failed: err=%v stderr=%q output=%s", err, stderr, stdout)
	}

	stdout, stderr, err, _, _ = executeCLIForTest(t, "", service,
		"--allow-unsupported-read", "--output", "json", "query", "alpha", "--command", "select 1")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnsupported) || stderr != "" || len(database.requests) != 0 {
		t.Fatalf("read opt-in allowed arbitrary SQL: err=%v code=%d stderr=%q requests=%v output=%s",
			err, exitCode(err), stderr, database.requests, stdout)
	}

	stdout, stderr, err, _, _ = executeCLIForTest(t, "", service,
		"--allow-unsupported-read", "--output", "json", "reload", "alpha", "node-a", "--force")
	if err == nil || exitCode(err) != control.ExitCode(control.CategoryUnsupported) || stderr != "" || len(rest.reloadCalls) != 0 {
		t.Fatalf("read opt-in crossed write gate: err=%v code=%d stderr=%q calls=%v output=%s",
			err, exitCode(err), stderr, rest.reloadCalls, stdout)
	}
}

func cliSnapshotWithVersion(t *testing.T, snapshot dcs.Snapshot, version string) dcs.Snapshot {
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

func requireGolden(t *testing.T, path, actual string) {
	t.Helper()
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	if actual != string(expected) {
		t.Fatalf("golden mismatch %s\nexpected=%s\nactual=%s", path, expected, actual)
	}
}
