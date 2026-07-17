package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestInspectConfigUsesLocalRuntimeAndSeparatesHumanMachineWarnings(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_CLI_INSPECT_PASSWORD__"
	path := filepath.Join(t.TempDir(), "patroni.yaml")
	contents := strings.Join([]string{
		"scope: alpha",
		"namespace: /pg",
		"etcd3:",
		"  host: 203.0.113.10:2379",
		"  password: " + marker,
		"ctl:",
		"  insecure: true",
		"  auth: operator:" + marker,
		"unknown_copy: " + marker,
		"boar:",
		"  network:",
		"    dcs_request_timeout: 13s",
		"    patroni_timeout: 17s",
		"    postgres_timeout: 23s",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DCS_URL", "")
	t.Setenv("BOAR_CONTEXT", "")
	t.Setenv("PATRONICTL_CONFIG_FILE", "")

	var humanOut, humanErr bytes.Buffer
	human := NewRootCommand(&humanOut, &humanErr)
	human.SetArgs([]string{"--config-file", path, "inspect-config"})
	if err := human.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("local configuration inspection failed: %v stderr=%q", err, humanErr.String())
	}
	if strings.Contains(humanOut.String()+humanErr.String(), marker) {
		t.Fatal("human configuration inspection leaked a credential")
	}
	for _, token := range []string{"Target:", "scope: alpha", "Effective configuration:", "Network deadlines:", "etcd3 request/watch lease", "13s", "Patroni REST request", "17s", "PostgreSQL query", "23s", "Configuration sources:", config.Redacted} {
		if !strings.Contains(humanOut.String(), token) {
			t.Errorf("human configuration inspection omitted %q:\n%s", token, humanOut.String())
		}
	}
	if !strings.Contains(humanErr.String(), "WARNING: Patroni REST TLS certificate verification is disabled") {
		t.Fatalf("human insecure warning missing from stderr: %q", humanErr.String())
	}

	var machineOut, machineErr bytes.Buffer
	machine := NewRootCommand(&machineOut, &machineErr)
	machine.SetArgs([]string{"--config-file", path, "--output", "json", "inspect-config"})
	if err := machine.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("machine configuration inspection failed: %v stderr=%q", err, machineErr.String())
	}
	if machineErr.Len() != 0 || strings.Contains(machineOut.String(), marker) {
		t.Fatalf("machine stdout/stderr secret boundary mismatch: stdout=%s stderr=%q", machineOut.String(), machineErr.String())
	}
	var envelope struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Warnings []string `json:"warnings"`
		} `json:"metadata"`
		Data struct {
			Target struct {
				Context   string `json:"context"`
				Namespace string `json:"namespace"`
				Scope     string `json:"scope"`
			} `json:"target"`
			Effective       map[string]any `json:"effective"`
			NetworkTimeouts struct {
				DCSRequestMilliseconds     int64 `json:"dcsRequestMilliseconds"`
				PatroniRequestMilliseconds int64 `json:"patroniRequestMilliseconds"`
				PostgresQueryMilliseconds  int64 `json:"postgresQueryMilliseconds"`
			} `json:"networkTimeouts"`
			Sources []struct {
				Field string `json:"field"`
			} `json:"sources"`
			Warnings []config.Warning `json:"warnings"`
		} `json:"data"`
	}
	if err := json.Unmarshal(machineOut.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.APIVersion != machineAPIVersion || envelope.Kind != "EffectiveConfiguration" ||
		envelope.Data.Target.Context != "default" || envelope.Data.Target.Namespace != "/pg" || envelope.Data.Target.Scope != "alpha" {
		t.Fatalf("machine inspection identity mismatch: %#v", envelope)
	}
	if len(envelope.Metadata.Warnings) != 1 || len(envelope.Data.Warnings) != 1 || envelope.Data.Warnings[0].Code != config.WarningInsecureRESTTLS {
		t.Fatalf("machine inspection warnings mismatch: %#v", envelope)
	}
	if envelope.Data.NetworkTimeouts.DCSRequestMilliseconds != 13_000 || envelope.Data.NetworkTimeouts.PatroniRequestMilliseconds != 17_000 ||
		envelope.Data.NetworkTimeouts.PostgresQueryMilliseconds != 23_000 {
		t.Fatalf("machine inspection deadlines mismatch: %#v", envelope.Data.NetworkTimeouts)
	}
	for index := 1; index < len(envelope.Data.Sources); index++ {
		if envelope.Data.Sources[index-1].Field >= envelope.Data.Sources[index].Field {
			t.Fatalf("machine configuration sources are not sorted: %#v", envelope.Data.Sources)
		}
	}

	var secureOut, secureErr bytes.Buffer
	secure := NewRootCommand(&secureOut, &secureErr)
	secure.SetArgs([]string{"--config-file", path, "--insecure=false", "--output", "json", "inspect-config"})
	if err := secure.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("explicit secure override failed: %v stderr=%q", err, secureErr.String())
	}
	if secureErr.Len() != 0 || strings.Contains(secureOut.String(), "INSECURE_REST_TLS") || strings.Contains(secureOut.String(), "verification is disabled") {
		t.Fatalf("secure override retained insecure warning: stdout=%s stderr=%q", secureOut.String(), secureErr.String())
	}
}

func TestMachineEffectiveConfigurationGolden(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_GOLDEN_INSPECT_PASSWORD__"
	document, err := config.Parse([]byte(strings.Join([]string{
		"scope: alpha",
		"namespace: /pg",
		"etcd3:",
		"  host: etcd.example.invalid",
		"  password: " + marker,
		"ctl:",
		"  insecure: true",
		"unknown_copy: " + marker,
		"boar:",
		"  network:",
		"    dns_timeout: 7s",
		"    dcs_dial_timeout: 8s",
		"    dcs_request_timeout: 9s",
		"    patroni_timeout: 11s",
		"    postgres_timeout: 12s",
		"    postgres_close_timeout: 13s",
		"",
	}, "\n")), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 7, 14, 1, 2, 3, 0, time.UTC)
	service, err := control.NewConfigurationService(control.ConfigurationServiceOptions{
		Clock: func() time.Time { return fixed }, NewOperationID: func() string { return "inspect-operation-1" },
	})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	closed := 0
	factory := func(_ context.Context, invocation runtimeInvocation) (*commandRuntime, error) {
		if invocation.request.operation != config.OperationInspect {
			t.Fatalf("unexpected runtime operation: %#v", invocation)
		}
		warnings := make([]string, 0, len(resolved.Warnings))
		for _, warning := range resolved.Warnings {
			warnings = append(warnings, warning.Message)
		}
		return &commandRuntime{
			service: service, resolved: resolved,
			target:   (model.Target{Context: resolved.Context, Namespace: resolved.Namespace, Scope: resolved.Scope, Group: resolved.Group}).Normalize(),
			warnings: warnings, close: func() error { closed++; return nil },
		}, nil
	}
	root := newRootCommandWithBoundaries(strings.NewReader(""), &stdout, &stderr, factory, func() time.Time { return fixed }, func() string { return "cli-request-unused" })
	root.SetArgs([]string{"--output", "json", "inspect-config"})
	if err := root.ExecuteContext(context.Background()); err != nil || stderr.Len() != 0 || closed != 1 {
		t.Fatalf("machine inspection golden execution failed: err=%v stderr=%q closed=%d", err, stderr.String(), closed)
	}
	if strings.Contains(stdout.String(), marker) {
		t.Fatal("machine inspection golden leaked a credential")
	}
	requireGolden(t, "testdata/machine-effective-configuration.golden.json", stdout.String())
}
