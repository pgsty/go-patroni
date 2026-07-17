package patroni

import (
	"net/http"
	"strings"

	"github.com/pgsty/go-patroni/model"
)

const (
	PatroniV3       = "3.0.0"
	PatroniV3MPP    = "3.3.0"
	PatroniV4       = "4.0.0"
	PatroniV4Point1 = "4.1.0"
)

type Risk string

const (
	RiskRead                  Risk = "read"
	RiskAdminWrite            Risk = "admin-write"
	RiskAvailabilityWrite     Risk = "availability-write"
	RiskPeerInternalRead      Risk = "peer-internal-read"
	RiskPeerInternal          Risk = "peer-internal"
	RiskTestPlatformDangerous Risk = "test-platform-dangerous"
)

type Endpoint struct {
	ID       string
	Method   string
	Path     string
	Risk     Risk
	Request  string
	Response string
	Since    string
}

// AvailableIn reports whether this method/path contract exists in a supported
// Patroni version. Version-specific semantics are described by FeatureCatalog.
func (endpoint Endpoint) AvailableIn(version model.Version) bool {
	since, err := model.ParseVersion(endpoint.Since)
	return err == nil && version.Compare(since) >= 0 && model.SupportedPatroniRange.Contains(version)
}

type Feature string

const (
	FeatureCoreRESTAPI            Feature = "core-rest-api"
	FeatureMPPEndpoint            Feature = "mpp-endpoint"
	FeatureQuorumStatus           Feature = "quorum-status"
	FeatureFailsafeLSNHeader      Feature = "failsafe-lsn-header"
	FeatureReadinessLagMode       Feature = "readiness-lag-mode"
	FeatureReinitializeFromLeader Feature = "reinitialize-from-leader"
	FeatureStandbyClusterCLI      Feature = "standby-cluster-cli"
)

type FeatureAvailability struct {
	Feature     Feature
	Since       string
	Description string
}

var featureCatalog = []FeatureAvailability{
	{Feature: FeatureCoreRESTAPI, Since: PatroniV3, Description: "core health, monitoring, configuration, restart, failover, and Citus REST endpoints"},
	{Feature: FeatureMPPEndpoint, Since: PatroniV3MPP, Description: "generic POST /mpp alias in addition to POST /citus"},
	{Feature: FeatureQuorumStatus, Since: PatroniV4, Description: "quorum health aliases, status field, and Prometheus metric"},
	{Feature: FeatureFailsafeLSNHeader, Since: PatroniV4, Description: "POST /failsafe returns the standby LSN header"},
	{Feature: FeatureReadinessLagMode, Since: PatroniV4Point1, Description: "readiness lag threshold and apply/write mode query semantics"},
	{Feature: FeatureReinitializeFromLeader, Since: PatroniV4Point1, Description: "POST /reinitialize from_leader request option"},
	{Feature: FeatureStandbyClusterCLI, Since: PatroniV4Point1, Description: "patronictl demote-cluster and promote-cluster commands"},
}

// FeatureCatalog returns a copy of the audited Patroni version capability
// matrix. The catalog is pinned against upstream 3.0.0 through 4.1.3.
func FeatureCatalog() []FeatureAvailability {
	return append([]FeatureAvailability(nil), featureCatalog...)
}

// SupportsFeature validates version and reports whether Patroni implements a
// named versioned capability.
func SupportsFeature(versionText string, feature Feature) (bool, error) {
	version, err := model.ParseVersion(versionText)
	if err != nil {
		return false, err
	}
	if err := model.CheckPatroniVersion(versionText); err != nil {
		return false, err
	}
	for _, candidate := range featureCatalog {
		if candidate.Feature != feature {
			continue
		}
		since, err := model.ParseVersion(candidate.Since)
		return err == nil && version.Compare(since) >= 0, err
	}
	return false, &wireContractError{message: "unknown Patroni feature"}
}

type HealthAlias string

const (
	HealthRoot                HealthAlias = "/"
	HealthPrimary             HealthAlias = "/primary"
	HealthMaster              HealthAlias = "/master"
	HealthReadWrite           HealthAlias = "/read-write"
	HealthLeader              HealthAlias = "/leader"
	HealthStandbyLeader       HealthAlias = "/standby-leader"
	HealthStandbyLeaderLegacy HealthAlias = "/standby_leader"
	HealthReplica             HealthAlias = "/replica"
	HealthReadOnly            HealthAlias = "/read-only"
	HealthQuorum              HealthAlias = "/quorum"
	HealthReadOnlyQuorum      HealthAlias = "/read-only-quorum"
	HealthSync                HealthAlias = "/sync"
	HealthSynchronous         HealthAlias = "/synchronous"
	HealthReadOnlySync        HealthAlias = "/read-only-sync"
	HealthReadOnlySynchronous HealthAlias = "/read-only-synchronous"
	HealthAsync               HealthAlias = "/async"
	HealthAsynchronous        HealthAlias = "/asynchronous"
	HealthAny                 HealthAlias = "/health"
)

var healthAliases = []HealthAlias{
	HealthRoot, HealthPrimary, HealthMaster, HealthReadWrite, HealthLeader,
	HealthStandbyLeader, HealthStandbyLeaderLegacy, HealthReplica, HealthReadOnly,
	HealthQuorum, HealthReadOnlyQuorum, HealthSync, HealthSynchronous,
	HealthReadOnlySync, HealthReadOnlySynchronous, HealthAsync,
	HealthAsynchronous, HealthAny,
}

func HealthAliases() []HealthAlias { return append([]HealthAlias(nil), healthAliases...) }

// HealthAliasesFor returns the health aliases implemented by the requested
// supported Patroni version.
func HealthAliasesFor(versionText string) ([]HealthAlias, error) {
	version, err := model.ParseVersion(versionText)
	if err != nil {
		return nil, err
	}
	if err := model.CheckPatroniVersion(versionText); err != nil {
		return nil, err
	}
	aliases := make([]HealthAlias, 0, len(healthAliases))
	for _, alias := range healthAliases {
		if healthAliasAvailableIn(alias, version) {
			aliases = append(aliases, alias)
		}
	}
	return aliases, nil
}

func healthAliasAvailableIn(alias HealthAlias, version model.Version) bool {
	if alias != HealthQuorum && alias != HealthReadOnlyQuorum {
		return true
	}
	since, _ := model.ParseVersion(PatroniV4)
	return version.Compare(since) >= 0
}

func healthAliasSince(alias HealthAlias) string {
	if alias == HealthQuorum || alias == HealthReadOnlyQuorum {
		return PatroniV4
	}
	return PatroniV3
}

func validHealthAlias(alias HealthAlias) bool {
	for _, candidate := range healthAliases {
		if alias == candidate {
			return true
		}
	}
	return false
}

// EndpointCatalog returns all 75 method/path rows in pinned Patroni source
// order. Callers receive a copy and cannot mutate the package contract.
func EndpointCatalog() []Endpoint {
	output := make([]Endpoint, 0, 75)
	for _, alias := range healthAliases {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
			response := "status-only"
			if method == http.MethodGet {
				response = "status-json"
			}
			output = append(output, Endpoint{
				ID: endpointID(method, string(alias)), Method: method, Path: string(alias), Risk: RiskRead,
				Request: "none", Response: response, Since: healthAliasSince(alias),
			})
		}
	}
	output = append(output,
		Endpoint{ID: "get-liveness", Method: http.MethodGet, Path: "/liveness", Risk: RiskRead, Request: "none", Response: "status-only"},
		Endpoint{ID: "get-readiness", Method: http.MethodGet, Path: "/readiness", Risk: RiskRead, Request: "none", Response: "status-only"},
		Endpoint{ID: "get-patroni", Method: http.MethodGet, Path: "/patroni", Risk: RiskRead, Request: "none", Response: "status-json"},
		Endpoint{ID: "get-cluster", Method: http.MethodGet, Path: "/cluster", Risk: RiskRead, Request: "none", Response: "cluster-json"},
		Endpoint{ID: "get-history", Method: http.MethodGet, Path: "/history", Risk: RiskRead, Request: "none", Response: "history-json"},
		Endpoint{ID: "get-config", Method: http.MethodGet, Path: "/config", Risk: RiskRead, Request: "none", Response: "config-json"},
		Endpoint{ID: "get-metrics", Method: http.MethodGet, Path: "/metrics", Risk: RiskRead, Request: "none", Response: "prometheus-text"},
		Endpoint{ID: "get-failsafe", Method: http.MethodGet, Path: "/failsafe", Risk: RiskPeerInternalRead, Request: "none", Response: "failsafe-json"},
		Endpoint{ID: "patch-config", Method: http.MethodPatch, Path: "/config", Risk: RiskAdminWrite, Request: "config-patch-json", Response: "config-json"},
		Endpoint{ID: "put-config", Method: http.MethodPut, Path: "/config", Risk: RiskAdminWrite, Request: "config-json", Response: "config-json"},
		Endpoint{ID: "post-reload", Method: http.MethodPost, Path: "/reload", Risk: RiskAdminWrite, Request: "none", Response: "text"},
		Endpoint{ID: "post-failsafe", Method: http.MethodPost, Path: "/failsafe", Risk: RiskPeerInternal, Request: "failsafe-peer-json", Response: "text"},
		Endpoint{ID: "post-sigterm", Method: http.MethodPost, Path: "/sigterm", Risk: RiskTestPlatformDangerous, Request: "none", Response: "text"},
		Endpoint{ID: "post-restart", Method: http.MethodPost, Path: "/restart", Risk: RiskAdminWrite, Request: "restart-json", Response: "text"},
		Endpoint{ID: "delete-restart", Method: http.MethodDelete, Path: "/restart", Risk: RiskAdminWrite, Request: "none", Response: "text"},
		Endpoint{ID: "delete-switchover", Method: http.MethodDelete, Path: "/switchover", Risk: RiskAdminWrite, Request: "none", Response: "text"},
		Endpoint{ID: "post-reinitialize", Method: http.MethodPost, Path: "/reinitialize", Risk: RiskAdminWrite, Request: "reinitialize-json", Response: "text"},
		Endpoint{ID: "post-failover", Method: http.MethodPost, Path: "/failover", Risk: RiskAvailabilityWrite, Request: "failover-json", Response: "text"},
		Endpoint{ID: "post-switchover", Method: http.MethodPost, Path: "/switchover", Risk: RiskAvailabilityWrite, Request: "switchover-json", Response: "text"},
		Endpoint{ID: "post-citus", Method: http.MethodPost, Path: "/citus", Risk: RiskPeerInternal, Request: "mpp-event-json", Response: "text"},
		Endpoint{ID: "post-mpp", Method: http.MethodPost, Path: "/mpp", Risk: RiskPeerInternal, Request: "mpp-event-json", Response: "text"},
	)
	for index := range output {
		if output[index].Since == "" {
			output[index].Since = PatroniV3
		}
		if output[index].ID == "post-mpp" {
			output[index].Since = PatroniV3MPP
		}
	}
	return output
}

// EndpointCatalogFor returns only endpoints implemented by the requested
// supported Patroni version, preserving upstream source order.
func EndpointCatalogFor(versionText string) ([]Endpoint, error) {
	version, err := model.ParseVersion(versionText)
	if err != nil {
		return nil, err
	}
	if err := model.CheckPatroniVersion(versionText); err != nil {
		return nil, err
	}
	all := EndpointCatalog()
	output := make([]Endpoint, 0, len(all))
	for _, endpoint := range all {
		if endpoint.AvailableIn(version) {
			output = append(output, endpoint)
		}
	}
	return output, nil
}

func endpointID(method, endpointPath string) string {
	name := strings.Trim(endpointPath, "/")
	if name == "" {
		name = "root"
	}
	return strings.ToLower(method) + "-" + strings.ReplaceAll(name, "/", "-")
}
