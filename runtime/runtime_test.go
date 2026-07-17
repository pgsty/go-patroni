package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
)

func TestEnvironmentEmbeddingIdentity(t *testing.T) {
	document, err := config.Parse([]byte("scope: demo\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}

	embedded, err := NewEnvironment(context.Background(), EnvironmentOptions{
		Document: document, UserAgent: "pig/2.0", ProductVersion: "pig 2.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if embedded.userAgent != "pig/2.0" || embedded.productVersion != "pig 2.0" {
		t.Fatalf("embedding identity = %q, %q", embedded.userAgent, embedded.productVersion)
	}

	defaults, err := NewEnvironment(context.Background(), EnvironmentOptions{Document: document})
	if err != nil {
		t.Fatal(err)
	}
	if defaults.userAgent == "" || defaults.productVersion == "" {
		t.Fatalf("default identity = %q, %q", defaults.userAgent, defaults.productVersion)
	}
}

func TestEnvironmentCopiesEmbeddingVersionRange(t *testing.T) {
	document, err := config.Parse([]byte("scope: demo\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	patroni4, err := model.NewVersionRange("4.0.0", "5.0.0")
	if err != nil {
		t.Fatal(err)
	}
	environment, err := NewEnvironment(context.Background(), EnvironmentOptions{
		Document: document, SupportedPatroniRange: &patroni4,
	})
	if err != nil {
		t.Fatal(err)
	}
	patroni4.Min = model.Version{Major: 3}
	if environment.supportedRange == nil || environment.supportedRange.Min.Major != 4 {
		t.Fatalf("environment version policy aliases caller memory: %#v", environment.supportedRange)
	}
}

func TestConfigurationRuntimeIsLocalSecretSafeAndCloseable(t *testing.T) {
	document, err := config.Parse([]byte(`
scope: demo
namespace: /service/
ctl:
  authentication:
    username: operator
    password: private-marker
go_patroni:
  default_context: staging
  contexts:
    staging:
      scope: demo-staging
`), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	environment, err := NewEnvironment(context.Background(), EnvironmentOptions{Document: document})
	if err != nil {
		t.Fatal(err)
	}
	if environment.DefaultContext() != "staging" || !reflect.DeepEqual(environment.ContextNames(), []string{"default", "staging"}) {
		t.Fatalf("environment contexts: default=%q names=%v", environment.DefaultContext(), environment.ContextNames())
	}
	resolved, err := environment.Resolve("staging")
	if err != nil || resolved.Scope != "demo-staging" {
		t.Fatalf("resolve staging: scope=%q err=%v", resolved.Scope, err)
	}
	runtime, err := environment.OpenConfiguration(context.Background(), "staging")
	if err != nil {
		t.Fatal(err)
	}
	result := runtime.Service.InspectConfiguration(context.Background(), control.InspectConfigurationRequest{Resolved: runtime.Resolved})
	if result.Outcome != control.Succeeded || result.Data.Target.Scope != "demo-staging" {
		t.Fatalf("configuration inspection: %#v", result)
	}
	if rendered := result.Data.Effective; containsValue(rendered, "private-marker") {
		t.Fatalf("configuration inspection leaked a secret: %#v", rendered)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentAndRuntimeFailureBoundaries(t *testing.T) {
	if _, err := NewEnvironment(nil, EnvironmentOptions{}); err == nil { //nolint:staticcheck // nil-context contract test
		t.Fatal("nil environment context was accepted")
	}
	document, err := config.Parse([]byte("scope: demo\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	widened := model.VersionRange{Min: model.Version{Major: 2}, Max: model.Version{Major: 5}}
	if _, err := NewEnvironment(context.Background(), EnvironmentOptions{Document: document, SupportedPatroniRange: &widened}); err == nil {
		t.Fatal("widened Patroni range was accepted")
	}
	environment, err := NewEnvironment(context.Background(), EnvironmentOptions{Document: document})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.Open(nil, RuntimeOptions{}); err == nil { //nolint:staticcheck // nil-context contract test
		t.Fatal("nil runtime context was accepted")
	}
	if _, err := environment.Open(context.Background(), RuntimeOptions{Operation: config.OperationClusterRead}); err == nil {
		t.Fatal("cluster runtime without etcd3 was accepted")
	}
	var nilEnvironment *Environment
	if nilEnvironment.ContextNames() != nil || nilEnvironment.DefaultContext() != model.DefaultContext {
		t.Fatal("nil environment accessors are not safe")
	}
	if _, err := nilEnvironment.Resolve(""); err == nil {
		t.Fatal("nil environment resolved configuration")
	}
	if _, err := nilEnvironment.OpenConfiguration(context.Background(), ""); err == nil {
		t.Fatal("nil environment opened configuration runtime")
	}
}

func TestRuntimeCloseIsIdempotentAndPreservesError(t *testing.T) {
	marker := errors.New("test-only runtime close failure")
	closed := 0
	runtime := &Runtime{close: func() error {
		closed++
		return marker
	}}
	if err := runtime.Close(); !errors.Is(err, marker) {
		t.Fatalf("first close error: %v", err)
	}
	if err := runtime.Close(); !errors.Is(err, marker) || closed != 1 {
		t.Fatalf("idempotent close: count=%d err=%v", closed, err)
	}
	var nilRuntime *Runtime
	if err := nilRuntime.Close(); err != nil {
		t.Fatalf("nil runtime close: %v", err)
	}
}

func TestResolveEtcdEndpointsForNonSRVLocators(t *testing.T) {
	tests := []struct {
		name      string
		projected config.Etcd3Config
		want      []string
	}{
		{name: "host", projected: config.Etcd3Config{Locator: config.LocatorHost, Endpoints: []string{"node:2379"}}, want: []string{"node:2379"}},
		{name: "hosts", projected: config.Etcd3Config{Locator: config.LocatorHosts, Endpoints: []string{"a:2379", "b:2379"}}, want: []string{"a:2379", "b:2379"}},
		{name: "url", projected: config.Etcd3Config{Locator: config.LocatorURL, URL: "https://node:2379"}, want: []string{"https://node:2379"}},
		{name: "proxy", projected: config.Etcd3Config{Locator: config.LocatorProxy, Proxy: "https://proxy:2379"}, want: []string{"https://proxy:2379"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveEtcdEndpoints(context.Background(), test.projected, time.Second)
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("resolve endpoints: got=%v want=%v err=%v", got, test.want, err)
			}
			if len(got) > 0 {
				got[0] = "mutated"
				if len(test.projected.Endpoints) > 0 && test.projected.Endpoints[0] == "mutated" {
					t.Fatal("resolved endpoints alias configuration memory")
				}
			}
		})
	}
	if _, err := resolveEtcdEndpoints(context.Background(), config.Etcd3Config{}, time.Second); err == nil {
		t.Fatal("missing etcd3 locator was accepted")
	}
	if _, err := lookupEtcdSRV(context.Background(), "_invalid"); err == nil {
		t.Fatal("invalid etcd3 SRV locator was accepted")
	}
}

func containsValue(value any, marker string) bool {
	switch typed := value.(type) {
	case string:
		return typed == marker
	case map[string]any:
		for _, item := range typed {
			if containsValue(item, marker) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsValue(item, marker) {
				return true
			}
		}
	}
	return false
}
