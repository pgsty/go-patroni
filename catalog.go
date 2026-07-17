package patroni

import (
	"net/http"
	"strings"
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
				Request: "none", Response: response,
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
	return output
}

func endpointID(method, endpointPath string) string {
	name := strings.Trim(endpointPath, "/")
	if name == "" {
		name = "root"
	}
	return strings.ToLower(method) + "-" + strings.ReplaceAll(name, "/", "-")
}
