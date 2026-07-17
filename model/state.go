package model

// DiscoveryState, ManagementState, ReachabilityState, and HealthState remain
// orthogonal. Callers must never infer one axis from another.
type DiscoveryState string
type ManagementState string
type ReachabilityState string
type HealthState string

const (
	DiscoveryDiscovered     DiscoveryState = "DISCOVERED"
	DiscoveryConfiguredOnly DiscoveryState = "CONFIGURED_ONLY"
	DiscoveryAbsent         DiscoveryState = "ABSENT"

	ManagementExplicit    ManagementState = "EXPLICIT"
	ManagementAllSelected ManagementState = "ALL_SELECTED"
	ManagementUnmanaged   ManagementState = "UNMANAGED"

	ReachabilityReachable          ReachabilityState = "REACHABLE"
	ReachabilityPartiallyReachable ReachabilityState = "PARTIALLY_REACHABLE"
	ReachabilityUnreachable        ReachabilityState = "UNREACHABLE"
	ReachabilityUnknown            ReachabilityState = "UNKNOWN"

	HealthHealthy   HealthState = "HEALTHY"
	HealthDegraded  HealthState = "DEGRADED"
	HealthUnhealthy HealthState = "UNHEALTHY"
	HealthUnknown   HealthState = "UNKNOWN"
)
