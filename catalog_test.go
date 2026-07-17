package patroni_test

import (
	"errors"
	"testing"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/model"
)

func TestVersionedEndpointCatalog(t *testing.T) {
	tests := []struct {
		version       string
		endpoints     int
		healthAliases int
		mpp           bool
		quorum        bool
	}{
		{version: "3.0.0", endpoints: 68, healthAliases: 16},
		{version: "3.2.2", endpoints: 68, healthAliases: 16},
		{version: "3.3.0", endpoints: 69, healthAliases: 16, mpp: true},
		{version: "3.3.8", endpoints: 69, healthAliases: 16, mpp: true},
		{version: "4.0.0", endpoints: 75, healthAliases: 18, mpp: true, quorum: true},
		{version: "4.1.4", endpoints: 75, healthAliases: 18, mpp: true, quorum: true},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			endpoints, err := patroni.EndpointCatalogFor(test.version)
			if err != nil {
				t.Fatal(err)
			}
			if len(endpoints) != test.endpoints {
				t.Fatalf("endpoint count=%d want=%d", len(endpoints), test.endpoints)
			}
			aliases, err := patroni.HealthAliasesFor(test.version)
			if err != nil {
				t.Fatal(err)
			}
			if len(aliases) != test.healthAliases {
				t.Fatalf("health alias count=%d want=%d", len(aliases), test.healthAliases)
			}
			assertEndpointAvailability(t, endpoints, "post-mpp", test.mpp)
			assertEndpointAvailability(t, endpoints, "get-quorum", test.quorum)
		})
	}
}

func TestFeatureAvailability(t *testing.T) {
	features := patroni.FeatureCatalog()
	if len(features) != 7 {
		t.Fatalf("feature count=%d want=7", len(features))
	}
	tests := []struct {
		version string
		feature patroni.Feature
		want    bool
	}{
		{version: "3.0.0", feature: patroni.FeatureCoreRESTAPI, want: true},
		{version: "3.2.2", feature: patroni.FeatureMPPEndpoint},
		{version: "3.3.0", feature: patroni.FeatureMPPEndpoint, want: true},
		{version: "3.3.8", feature: patroni.FeatureQuorumStatus},
		{version: "4.0.0", feature: patroni.FeatureQuorumStatus, want: true},
		{version: "4.0.7", feature: patroni.FeatureReadinessLagMode},
		{version: "4.1.0", feature: patroni.FeatureReadinessLagMode, want: true},
		{version: "4.1.0-rc1", feature: patroni.FeatureReadinessLagMode},
		{version: "4.1.4", feature: patroni.FeatureReinitializeFromLeader, want: true},
	}
	for _, test := range tests {
		t.Run(test.version+"/"+string(test.feature), func(t *testing.T) {
			got, err := patroni.SupportsFeature(test.version, test.feature)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("SupportsFeature=%v want=%v", got, test.want)
			}
		})
	}
}

func TestVersionedCatalogRejectsUnparseableAndUnsupportedVersions(t *testing.T) {
	for _, version := range []string{"", "future", "2.9.9", "5.0.0"} {
		if _, err := patroni.EndpointCatalogFor(version); err == nil {
			t.Errorf("EndpointCatalogFor(%q) accepted", version)
		}
	}
	if _, err := patroni.SupportsFeature("5.0.0", patroni.FeatureCoreRESTAPI); !errors.Is(err, model.ErrUnsupportedPatroniVersion) {
		t.Fatalf("unsupported version error=%v", err)
	}
	if _, err := patroni.SupportsFeature("4.1.4", patroni.Feature("unknown")); err == nil {
		t.Fatal("unknown feature accepted")
	}
}

func assertEndpointAvailability(t *testing.T, endpoints []patroni.Endpoint, id string, want bool) {
	t.Helper()
	found := false
	for _, endpoint := range endpoints {
		if endpoint.ID == id {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("endpoint %s availability=%v want=%v", id, found, want)
	}
}
