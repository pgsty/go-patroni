package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNetworkConfigDefaultsAndCustomProjection(t *testing.T) {
	defaultsDocument, err := Parse([]byte("scope: alpha\n"), "defaults.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := defaultsDocument.NetworkConfig()
	if err != nil {
		t.Fatal(err)
	}
	wantDefaults := NetworkConfig{
		DNSLookupTimeout: 5 * time.Second, DCSDialTimeout: 5 * time.Second,
		DCSRequestTimeout: 10 * time.Second, PatroniTimeout: 10 * time.Second,
		PostgresTimeout: 30 * time.Second, PostgresCloseTimeout: 5 * time.Second,
	}
	if !reflect.DeepEqual(defaults, wantDefaults) {
		t.Fatalf("network defaults=%#v want=%#v", defaults, wantDefaults)
	}
	resolvedDefaults, err := defaultsDocument.Resolve(ResolveRequest{})
	if err != nil || !reflect.DeepEqual(resolvedDefaults.Network, wantDefaults) {
		t.Fatalf("resolved network defaults=%#v error=%v", resolvedDefaults.Network, err)
	}

	document, err := Parse([]byte(`scope: alpha
boar:
  network:
    dns_timeout: 1s
    dcs_dial_timeout: 2s
    dcs_request_timeout: 3s
    patroni_timeout: 4s
    postgres_timeout: 5s
    postgres_close_timeout: 6s
    future_timeout_policy: tolerated
`), "custom.yaml")
	if err != nil {
		t.Fatal(err)
	}
	want := NetworkConfig{
		DNSLookupTimeout: time.Second, DCSDialTimeout: 2 * time.Second,
		DCSRequestTimeout: 3 * time.Second, PatroniTimeout: 4 * time.Second,
		PostgresTimeout: 5 * time.Second, PostgresCloseTimeout: 6 * time.Second,
	}
	resolved, err := document.Resolve(ResolveRequest{})
	if err != nil || !reflect.DeepEqual(resolved.Network, want) {
		t.Fatalf("resolved network=%#v want=%#v error=%v", resolved.Network, want, err)
	}
	if !strings.Contains(resolved.Network.String(), "dcsRequest:3s") {
		t.Fatalf("network formatter does not expose safe effective values: %s", resolved.Network.String())
	}
}

func TestNetworkConfigRejectsOnlyKnownInvalidFields(t *testing.T) {
	tests := []struct {
		name  string
		yaml  string
		field string
	}{
		{name: "mapping", yaml: "text", field: "boar.network"},
		{name: "type", yaml: "{patroni_timeout: 10}", field: "boar.network.patroni_timeout"},
		{name: "empty", yaml: "{dns_timeout: ''}", field: "boar.network.dns_timeout"},
		{name: "zero", yaml: "{dcs_request_timeout: 0s}", field: "boar.network.dcs_request_timeout"},
		{name: "negative", yaml: "{postgres_timeout: -1s}", field: "boar.network.postgres_timeout"},
		{name: "invalid", yaml: "{postgres_close_timeout: forever}", field: "boar.network.postgres_close_timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document, err := Parse([]byte("scope: alpha\nboar:\n  network: "+test.yaml+"\n"), "invalid.yaml")
			if err != nil {
				t.Fatalf("raw tolerant parse rejected network extension: %v", err)
			}
			_, err = document.Resolve(ResolveRequest{})
			configuration, ok := err.(*Error)
			if !ok || configuration.Kind != ErrorProjection || configuration.Field != test.field || configuration.Source != "invalid.yaml" {
				t.Fatalf("error=%#v want projection field %q", err, test.field)
			}
		})
	}
}
