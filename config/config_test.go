package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pgsty/go-patroni/config"
	"go.yaml.in/yaml/v3"
)

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(current), "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func ptr[T any](value T) *T { return &value }

func TestPigstyPatronictlFixtureLoadsTolerantly(t *testing.T) {
	document, err := config.Parse(testdata(t, "pigsty-patronictl.yaml"), "testdata/pigsty-patronictl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Context != "default" || resolved.Namespace != "/pg" || resolved.Scope != "pg-meta" {
		t.Fatalf("identity projection mismatch: %#v", resolved)
	}
	wantEndpoints := []string{"10.0.0.10:2379", "10.0.0.11:2379"}
	if !slices.Equal(resolved.Etcd3.Endpoints, wantEndpoints) || resolved.Etcd3.Protocol != "https" {
		t.Fatalf("etcd3 projection mismatch: %#v", resolved.Etcd3)
	}
	if !resolved.Etcd3.Password.IsSet() || !resolved.REST.Password.IsSet() {
		t.Fatal("secret fields were not retained as protected values")
	}
	if resolved.REST.Username != "test-rest-admin" || resolved.REST.CAFile != "/test/pki/ctl-ca.crt" || resolved.REST.CertFile != "/test/pki/ctl-client.crt" {
		t.Fatalf("ctl to restapi precedence mismatch: %#v", resolved.REST)
	}
	raw := document.RawNode()
	if raw == nil {
		t.Fatal("raw Patroni YAML was not retained")
	}
	rawYAML, err := yaml.Marshal(raw)
	if err != nil || !strings.Contains(string(rawYAML), "unknown_future_section") {
		t.Fatalf("raw Patroni YAML lost unknown fields: %v", err)
	}
	effective, err := resolved.EffectiveYAML()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"__BOAR_TEST_ONLY_ETCD_PASSWORD__", "__BOAR_TEST_ONLY_REST_PASSWORD__", "__BOAR_TEST_ONLY_PG_PASSWORD__"} {
		if strings.Contains(string(effective), forbidden) {
			t.Fatalf("effective configuration leaked fixture credential %q:\n%s", forbidden, effective)
		}
	}
	if !strings.Contains(string(effective), "unknown_future_section") || strings.Count(string(effective), config.Redacted) < 3 {
		t.Fatalf("effective configuration did not preserve unknown fields/redact secrets:\n%s", effective)
	}
}

func TestRedactMapIsDeepNonMutatingAndConservative(t *testing.T) {
	original := map[string]any{
		"password": "top-level-secret",
		"nested": map[string]any{
			"auth": "nested-secret", "api-key": "api-key-material", "cookie": "cookie-material",
			"items": []any{map[string]any{"private-key": "key-material", "safe": "visible"}},
		},
	}
	redacted := config.RedactMap(original)
	if redacted["password"] != config.Redacted ||
		redacted["nested"].(map[string]any)["auth"] != config.Redacted ||
		redacted["nested"].(map[string]any)["api-key"] != config.Redacted ||
		redacted["nested"].(map[string]any)["cookie"] != config.Redacted ||
		redacted["nested"].(map[string]any)["items"].([]any)[0].(map[string]any)["private-key"] != config.Redacted {
		t.Fatalf("nested secret redaction mismatch: %#v", redacted)
	}
	if redacted["nested"].(map[string]any)["items"].([]any)[0].(map[string]any)["safe"] != "visible" {
		t.Fatalf("safe value was redacted: %#v", redacted)
	}
	if original["password"] != "top-level-secret" || original["nested"].(map[string]any)["auth"] != "nested-secret" {
		t.Fatalf("redaction mutated input: %#v", original)
	}
}

func TestEffectiveMapRedactsKnownValuesAndCredentialBearingStrings(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_EFFECTIVE_PASSWORD__"
	document, err := config.Parse([]byte(strings.Join([]string{
		"scope: alpha",
		"ctl:",
		"  auth: operator:" + marker,
		"unknown_copy: " + marker,
		"unknown_uri: postgres://operator:" + marker + "@db.example/app",
		"unknown_dsn: host=db.example password=" + marker,
		"",
	}, "\n")), "memory")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	effective := resolved.Effective()
	encoded, err := json.Marshal(effective)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), marker) || strings.Count(string(encoded), config.Redacted) < 4 {
		t.Fatalf("effective map leaked a credential shape: %s", encoded)
	}
	effective["scope"] = "mutated"
	if next := resolved.Effective()["scope"]; next != "alpha" {
		t.Fatalf("effective map was not a deep copy: %#v", next)
	}
}

func TestExplicitInsecureRESTTLSIsObservableAndReversible(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\nctl:\n  insecure: true\n"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.REST.Insecure || len(resolved.Warnings) != 1 || resolved.Warnings[0].Code != config.WarningInsecureRESTTLS || resolved.Warnings[0].Field != "ctl.insecure" {
		t.Fatalf("explicit insecure REST TLS was not observable: %#v", resolved)
	}
	disabled := false
	secure, err := document.Resolve(config.ResolveRequest{Overrides: config.Overrides{Insecure: &disabled}})
	if err != nil {
		t.Fatal(err)
	}
	if secure.REST.Insecure || len(secure.Warnings) != 0 {
		t.Fatalf("explicit secure override retained insecure state/warning: %#v", secure)
	}
	if source, ok := secure.Source("ctl.insecure"); !ok || source.Layer != config.LayerFlag || source.Name != "--insecure" {
		t.Fatalf("secure override source mismatch: %#v %t", source, ok)
	}
}

func TestPinnedPatroniFixtureLoadsBeforeOperationValidation(t *testing.T) {
	document, err := config.Parse(testdata(t, "patroni-postgres0.yaml"), "testdata/patroni-postgres0.yaml")
	if err != nil {
		t.Fatalf("valid Patroni fixture rejected: %v", err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatalf("valid Patroni fixture projection rejected: %v", err)
	}
	if resolved.Scope != "batman" || resolved.Namespace != "/service" || resolved.Etcd3.Configured {
		t.Fatalf("unexpected Patroni fixture projection: %#v", resolved)
	}
	if err := resolved.Validate(config.OperationLocalVersion, ""); err != nil {
		t.Fatalf("unrelated local operation rejected: %v", err)
	}
	var validationErr *config.ValidationError
	if err := resolved.Validate(config.OperationClusterRead, ""); !errors.As(err, &validationErr) || validationErr.Field != "etcd3" {
		t.Fatalf("non-MVP DCS did not produce precise operation error: %v", err)
	}
	effective, err := resolved.EffectiveYAML()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"__BOAR_TEST_ONLY_REPLICATION_PASSWORD__", "__BOAR_TEST_ONLY_SUPERUSER_PASSWORD__", "__BOAR_TEST_ONLY_REWIND_PASSWORD__"} {
		if strings.Contains(string(effective), forbidden) {
			t.Fatal("Patroni fixture credential leaked from effective configuration")
		}
	}
}

func TestCitusPresenceIsProjectedWithoutRequiringGroup(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\ncitus:\n  database: postgres\netcd3:\n  host: 127.0.0.1\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Citus || resolved.Group != nil {
		t.Fatalf("Citus presence/group projection mismatch: %#v", resolved)
	}

	ordinary, err := config.Parse([]byte("scope: alpha\netcd3:\n  host: 127.0.0.1\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryResolved, err := ordinary.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ordinaryResolved.Citus || ordinaryResolved.Group != nil {
		t.Fatalf("ordinary cluster was projected as Citus: %#v", ordinaryResolved)
	}

	group := 7
	withOverride, err := ordinary.Resolve(config.ResolveRequest{Overrides: config.Overrides{Group: &group}})
	if err != nil {
		t.Fatal(err)
	}
	if !withOverride.Citus || withOverride.Group == nil || *withOverride.Group != group {
		t.Fatalf("group override did not project Citus identity: %#v", withOverride)
	}
}

func TestConfigValuesAreSafeToFormatAndMarshal(t *testing.T) {
	document, err := config.Parse(testdata(t, "pigsty-patronictl.yaml"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}
	outputs := []string{fmt.Sprint(document), fmt.Sprintf("%#v", document), fmt.Sprint(resolved), fmt.Sprintf("%#v", resolved), string(encoded)}
	for _, output := range outputs {
		for _, forbidden := range []string{"__BOAR_TEST_ONLY_ETCD_PASSWORD__", "__BOAR_TEST_ONLY_REST_PASSWORD__", "__BOAR_TEST_ONLY_PG_PASSWORD__"} {
			if strings.Contains(output, forbidden) {
				t.Fatalf("configuration formatter leaked a fixture credential")
			}
		}
	}
}

func TestCredentialBearingDCSURLsAreRejected(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	_, err = document.Resolve(config.ResolveRequest{Environment: config.MapEnvironment{"DCS_URL": "etcd3://user:__BOAR_TEST_ONLY_URL_PASSWORD__@host:2379/service"}})
	var configErr *config.Error
	if !errors.As(err, &configErr) || configErr.Field != "DCS_URL" || strings.Contains(err.Error(), "__BOAR_TEST_ONLY_URL_PASSWORD__") {
		t.Fatalf("credential-bearing DCS URL error was unsafe or imprecise: %v", err)
	}
}

func TestUnknownTaggedFieldsRemainTolerated(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\nfuture: !future tagged-value\n"), "memory")
	if err != nil {
		t.Fatalf("unknown YAML tag rejected: %v", err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	effective, err := resolved.EffectiveYAML()
	if err != nil || !strings.Contains(string(effective), "tagged-value") {
		t.Fatalf("unknown tagged field not retained: %v", err)
	}
}

func TestNamedContextMergeNullAndListRules(t *testing.T) {
	document, err := config.Parse(testdata(t, "multi-context.yaml"), "testdata/multi-context.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := document.ContextNames(); !slices.Equal(got, []string{"default", "isolated", "staging"}) {
		t.Fatalf("context names are not deterministic: %v", got)
	}
	staging, err := document.Resolve(config.ResolveRequest{Context: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	if staging.Namespace != "/stage" || staging.Scope != "stage-scope" || !slices.Equal(staging.Etcd3.Endpoints, []string{"stage-only:2379"}) {
		t.Fatalf("staging merge mismatch: %#v", staging)
	}
	if staging.Etcd3.Password.IsSet() {
		t.Fatal("explicit null did not clear inherited secret")
	}
	effective, err := staging.EffectiveYAML()
	if err != nil {
		t.Fatal(err)
	}
	text := string(effective)
	if strings.Contains(text, "root-a") || !strings.Contains(text, "stage-only") || !strings.Contains(text, "preserved: true") {
		t.Fatalf("map/list merge contract mismatch:\n%s", text)
	}
	if !strings.Contains(text, "password: null") {
		t.Fatalf("explicit null was not preserved in diagnostics:\n%s", text)
	}
	isolated, err := document.Resolve(config.ResolveRequest{Context: "isolated"})
	if err != nil {
		t.Fatal(err)
	}
	if isolated.Etcd3.Protocol != "http" || !slices.Equal(isolated.Etcd3.Endpoints, []string{"isolated:2379"}) {
		t.Fatalf("isolated context defaults mismatch: %#v", isolated.Etcd3)
	}
	isolatedYAML, _ := isolated.EffectiveYAML()
	if strings.Contains(string(isolatedYAML), "preserved") || strings.Contains(string(isolatedYAML), "root-a") {
		t.Fatalf("context without extends inherited root fields:\n%s", isolatedYAML)
	}
}

func TestEffectiveConfigGolden(t *testing.T) {
	document, err := config.Parse(testdata(t, "multi-context.yaml"), "testdata/multi-context.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{Context: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := resolved.EffectiveYAML()
	if err != nil {
		t.Fatal(err)
	}
	_, current, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(current), "testdata", "staging-effective.golden.yaml")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (use UPDATE_GOLDEN=1 for an intentional update): %v", err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("effective config differs from golden; use UPDATE_GOLDEN=1 only after contract review")
	}
}

func TestPrecedenceAndSourceTracking(t *testing.T) {
	document, err := config.Parse(testdata(t, "multi-context.yaml"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{
		Context: "staging",
		Environment: config.MapEnvironment{
			"DCS_URL": "env-etcd:22379/env-ns",
		},
		Overrides: config.Overrides{
			Scope:     ptr("flag-scope"),
			Namespace: ptr("/flag-ns/"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Scope != "flag-scope" || resolved.Namespace != "/flag-ns" || !slices.Equal(resolved.Etcd3.Endpoints, []string{"env-etcd:22379"}) {
		t.Fatalf("precedence mismatch: %#v", resolved)
	}
	if len(resolved.Warnings) != 1 || resolved.Warnings[0].Code != config.WarningSchemeLessDCSURL {
		t.Fatalf("scheme-less compatibility warning missing: %#v", resolved.Warnings)
	}
	if source, ok := resolved.Source("scope"); !ok || source.Layer != config.LayerFlag || source.Name != "--scope" {
		t.Fatalf("scope source mismatch: %#v %v", source, ok)
	}
	if source, ok := resolved.Source("etcd3.host"); !ok || source.Layer != config.LayerEnvironment || source.Name != "DCS_URL" {
		t.Fatalf("DCS source mismatch: %#v %v", source, ok)
	}
}

func TestDCSURLRejectsUnsupportedBackend(t *testing.T) {
	document, err := config.Parse([]byte("scope: alpha\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	_, err = document.Resolve(config.ResolveRequest{Environment: config.MapEnvironment{"DCS_URL": "consul://127.0.0.1:8500/service"}})
	var configErr *config.Error
	if !errors.As(err, &configErr) || configErr.Kind != config.ErrorUnsupported || configErr.Field != "DCS_URL" {
		t.Fatalf("unsupported DCS error mismatch: %#v", err)
	}
}

func TestNamedContextErrorsArePrecise(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		context string
		field   string
	}{
		{
			name:    "cycle",
			yaml:    "boar:\n  contexts:\n    first:\n      extends: second\n    second:\n      extends: first\n",
			context: "first",
			field:   "boar.contexts.first.extends",
		},
		{
			name:    "missing parent",
			yaml:    "boar:\n  contexts:\n    child:\n      extends: absent\n",
			context: "child",
			field:   "boar.contexts.absent",
		},
		{
			name:    "missing selected context",
			yaml:    "scope: root\n",
			context: "absent",
			field:   "go_patroni.contexts.absent",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document, err := config.Parse([]byte(test.yaml), "memory")
			if err != nil {
				t.Fatal(err)
			}
			_, err = document.Resolve(config.ResolveRequest{Context: test.context})
			var configErr *config.Error
			if !errors.As(err, &configErr) || configErr.Kind != config.ErrorContext || configErr.Field != test.field {
				t.Fatalf("context error mismatch: %#v", err)
			}
		})
	}
}

func TestPublicConfigExtensionAndLegacyContextEnvironment(t *testing.T) {
	document, err := config.Parse([]byte(`scope: root
go_patroni:
  default_context: staging
  contexts:
    staging:
      scope: staging
    production:
      scope: production
  network:
    patroni_timeout: 17s
`), "public.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if document.DefaultContext() != "staging" {
		t.Fatalf("default context=%q", document.DefaultContext())
	}
	resolved, err := document.Resolve(config.ResolveRequest{Environment: config.MapEnvironment{
		"GO_PATRONI_CONTEXT": "production",
		"BOAR_CONTEXT":       "staging",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Context != "production" || resolved.Scope != "production" || resolved.Network.PatroniTimeout.String() != "17s" {
		t.Fatalf("public extension projection mismatch: %#v", resolved)
	}

	_, err = config.Parse([]byte("go_patroni: {}\nboar: {}\n"), "ambiguous.yaml")
	var configErr *config.Error
	if !errors.As(err, &configErr) || configErr.Field != "go_patroni" {
		t.Fatalf("ambiguous extension error=%#v", err)
	}
}

func TestEtcd3LocatorProjectionAndEndpointDefaults(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		locator   config.LocatorKind
		endpoints []string
	}{
		{"host default port", "etcd3:\n  host: etcd.example\n", config.LocatorHost, []string{"etcd.example:2379"}},
		{"comma separated hosts", "etcd3:\n  hosts: one,two:22379\n", config.LocatorHosts, []string{"one:2379", "two:22379"}},
		{"URL host and IPv6", "etcd3:\n  hosts: [https://three.example, '[::1]:22379']\n", config.LocatorHosts, []string{"three.example:2379", "[::1]:22379"}},
		{"url wins", "etcd3:\n  url: https://proxy.example:4001\n  host: ignored.example\n", config.LocatorURL, nil},
		{"proxy wins", "etcd3:\n  proxy: https://proxy.example:4001\n  hosts: [ignored.example]\n", config.LocatorProxy, nil},
		{"srv wins", "etcd3:\n  srv: _etcd-client-ssl._tcp.example\n  host: ignored.example\n", config.LocatorSRV, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document, err := config.Parse([]byte(test.yaml), "memory")
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := document.Resolve(config.ResolveRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if !resolved.Etcd3.Configured || resolved.Etcd3.Locator != test.locator || !slices.Equal(resolved.Etcd3.Endpoints, test.endpoints) {
				t.Fatalf("locator projection mismatch: %#v", resolved.Etcd3)
			}
		})
	}
}

func TestEtcd3EndpointsRejectUnsafeOrInvalidValues(t *testing.T) {
	for _, value := range []string{
		"https://user:__BOAR_TEST_ONLY_ENDPOINT_PASSWORD__@host:2379",
		"host:not-a-port",
		"host:0",
		"host:65536",
	} {
		document, err := config.Parse([]byte("etcd3:\n  host: \""+value+"\"\n"), "memory")
		if err != nil {
			t.Fatal(err)
		}
		_, err = document.Resolve(config.ResolveRequest{})
		var configErr *config.Error
		if !errors.As(err, &configErr) || configErr.Field != "etcd3.host" || strings.Contains(err.Error(), "__BOAR_TEST_ONLY_ENDPOINT_PASSWORD__") {
			t.Fatalf("unsafe or invalid endpoint was not rejected safely")
		}
	}
}

func TestOperationSpecificValidation(t *testing.T) {
	document, err := config.Parse([]byte("unknown: retained\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolved.Validate(config.OperationLocalVersion, ""); err != nil {
		t.Fatalf("local version should not require DCS or scope: %v", err)
	}
	var validationErr *config.ValidationError
	if err := resolved.Validate(config.OperationDiscover, ""); !errors.As(err, &validationErr) || validationErr.Field != "etcd3" {
		t.Fatalf("discover missing-DCS error mismatch: %#v", err)
	}
	withDCS, err := config.Parse([]byte("etcd3:\n  host: 127.0.0.1\n"), "memory")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err = withDCS.Resolve(config.ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolved.Validate(config.OperationDiscover, ""); err != nil {
		t.Fatalf("discover should not require scope: %v", err)
	}
	if err := resolved.Validate(config.OperationClusterRead, ""); !errors.As(err, &validationErr) || validationErr.Field != "scope" {
		t.Fatalf("implicit cluster error mismatch: %#v", err)
	}
	if err := resolved.Validate(config.OperationClusterRead, "explicit-scope"); err != nil {
		t.Fatalf("explicit scope should satisfy cluster operation: %v", err)
	}
}

func TestLoadHonorsConfigFileSelectorAndCancellation(t *testing.T) {
	temp := t.TempDir()
	path := filepath.Join(temp, "patronictl.yaml")
	if err := os.WriteFile(path, []byte("scope: from-file\netcd3:\n  host: 127.0.0.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	document, err := config.Load(context.Background(), config.LoadRequest{Environment: config.MapEnvironment{"PATRONICTL_CONFIG_FILE": path}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := document.Resolve(config.ResolveRequest{})
	if err != nil || resolved.Scope != "from-file" {
		t.Fatalf("selected config was not loaded: resolved=%#v err=%v", resolved, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = config.Load(cancelled, config.LoadRequest{Path: path})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled load returned %v", err)
	}
}

func TestResolveIsSafeForConcurrentReaders(t *testing.T) {
	document, err := config.Parse(testdata(t, "multi-context.yaml"), "fixture.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	errorsFound := make(chan error, 64)
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			resolved, err := document.Resolve(config.ResolveRequest{Context: "staging"})
			if err == nil {
				_, err = resolved.EffectiveYAML()
			}
			if err != nil {
				errorsFound <- err
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent resolve failed: %v", err)
	}
}

func TestParseRejectsMultipleDocumentsAndNonMappingRoot(t *testing.T) {
	tests := []struct {
		name string
		data string
		kind config.ErrorKind
	}{
		{"multiple", "scope: one\n---\nscope: two\n", config.ErrorMultipleDocuments},
		{"sequence", "- one\n- two\n", config.ErrorRootType},
		{"malformed", "scope: [\n", config.ErrorSyntax},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Parse([]byte(test.data), "memory")
			var configErr *config.Error
			if !errors.As(err, &configErr) || configErr.Kind != test.kind {
				t.Fatalf("error mismatch: %#v", err)
			}
		})
	}
}

func FuzzParseNeverPanics(f *testing.F) {
	f.Add([]byte("scope: alpha\netcd3:\n  hosts: [a:2379]\n"))
	f.Add([]byte("unknown: !future tagged\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		document, err := config.Parse(data, "fuzz")
		if err != nil {
			return
		}
		_, _ = document.Resolve(config.ResolveRequest{})
	})
}
