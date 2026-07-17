package control_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestInspectConfigurationIsLocalDeterministicAndSecretSafe(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_INSPECT_PASSWORD__"
	document, err := config.Parse([]byte(strings.Join([]string{
		"scope: alpha",
		"namespace: /pg",
		"etcd3:",
		"  host: 203.0.113.10:2379",
		"  password: " + marker,
		"ctl:",
		"  insecure: true",
		"unknown_copy: " + marker,
		"boar:",
		"  network:",
		"    patroni_timeout: 17s",
		"    postgres_timeout: 23s",
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
	request := control.InspectConfigurationRequest{Resolved: resolved}
	if strings.Contains(request.String(), marker) {
		t.Fatal("inspection request formatter leaked a credential")
	}
	result := service.InspectConfiguration(context.Background(), request)
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != control.Succeeded || result.Path != control.PathLocal || result.Target.Scope != "alpha" || result.Target.Namespace != "/pg" {
		t.Fatalf("inspection result mismatch: %#v", result)
	}
	if len(result.Data.Warnings) != 1 || result.Data.Warnings[0].Code != config.WarningInsecureRESTTLS {
		t.Fatalf("inspection warning mismatch: %#v", result.Data.Warnings)
	}
	if result.Data.NetworkTimeouts.PatroniRequestMilliseconds != 17_000 || result.Data.NetworkTimeouts.PostgresQueryMilliseconds != 23_000 ||
		result.Data.NetworkTimeouts.DCSRequestMilliseconds != 10_000 {
		t.Fatalf("inspection network deadlines mismatch: %#v", result.Data.NetworkTimeouts)
	}
	for index := 1; index < len(result.Data.Sources); index++ {
		if result.Data.Sources[index-1].Field >= result.Data.Sources[index].Field {
			t.Fatalf("configuration sources are not strictly sorted: %#v", result.Data.Sources)
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), marker) || !strings.Contains(string(encoded), config.Redacted) {
		t.Fatalf("inspection result leaked or omitted redaction: %s", encoded)
	}
	if len(result.Evidence) != 1 || result.Evidence[0].ObservedAt != fixed || result.Evidence[0].Source != control.EvidenceLocal {
		t.Fatalf("inspection evidence mismatch: %#v", result.Evidence)
	}
}

func TestConfigurationServiceCancellationAndUnavailableCapabilitiesAreStructured(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := control.NewConfigurationService(control.ConfigurationServiceOptions{
		NewOperationID: func() string { return "inspect-operation" },
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result := service.InspectConfiguration(canceled, control.InspectConfigurationRequest{Resolved: resolved})
	if result.Outcome != control.Failed || result.Error == nil || result.Error.Category != control.CategoryFailed {
		t.Fatalf("canceled inspection mismatch: %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	list := service.List(context.Background(), control.ListRequest{Targets: []model.Target{{Scope: "alpha"}}})
	if list.Outcome != control.Failed || list.Error == nil || list.Error.Category != control.CategoryConfig {
		t.Fatalf("configuration-only service exposed a network capability: %#v", list)
	}
}
