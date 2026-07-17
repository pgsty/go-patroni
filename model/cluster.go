package model

// MemberRole is BOAR's normalized Patroni role. It is deliberately separate
// from both Patroni REST wire DTOs and adapter-specific rendering values.
type MemberRole string

const (
	RoleLeader        MemberRole = "leader"
	RoleStandbyLeader MemberRole = "standby_leader"
	RoleSyncStandby   MemberRole = "sync_standby"
	RoleQuorumStandby MemberRole = "quorum_standby"
	RoleReplica       MemberRole = "replica"
)

type ScheduledRestart struct {
	Schedule        string `json:"schedule" yaml:"schedule"`
	PostgresVersion string `json:"postgresVersion,omitempty" yaml:"postgresVersion,omitempty"`
}

type Member struct {
	Target               Target            `json:"target" yaml:"target"`
	Name                 string            `json:"name" yaml:"name"`
	APIURL               string            `json:"apiUrl,omitempty" yaml:"apiUrl,omitempty"`
	Host                 string            `json:"host,omitempty" yaml:"host,omitempty"`
	Port                 uint16            `json:"port,omitempty" yaml:"port,omitempty"`
	Role                 MemberRole        `json:"role" yaml:"role"`
	State                string            `json:"state,omitempty" yaml:"state,omitempty"`
	Timeline             *int              `json:"timeline,omitempty" yaml:"timeline,omitempty"`
	ReceiveLSN           string            `json:"receiveLsn,omitempty" yaml:"receiveLsn,omitempty"`
	ReceiveLagBytes      int64             `json:"receiveLagBytes,omitempty" yaml:"receiveLagBytes,omitempty"`
	ReplayLSN            string            `json:"replayLsn,omitempty" yaml:"replayLsn,omitempty"`
	ReplayLagBytes       int64             `json:"replayLagBytes,omitempty" yaml:"replayLagBytes,omitempty"`
	PendingRestart       bool              `json:"pendingRestart,omitempty" yaml:"pendingRestart,omitempty"`
	PendingRestartReason map[string]any    `json:"pendingRestartReason,omitempty" yaml:"pendingRestartReason,omitempty"`
	ScheduledRestart     *ScheduledRestart `json:"scheduledRestart,omitempty" yaml:"scheduledRestart,omitempty"`
	Tags                 map[string]any    `json:"tags,omitempty" yaml:"tags,omitempty"`
	PatroniVersion       string            `json:"patroniVersion,omitempty" yaml:"patroniVersion,omitempty"`
}

type ScheduledSwitchover struct {
	At   string `json:"at" yaml:"at"`
	From string `json:"from,omitempty" yaml:"from,omitempty"`
	To   string `json:"to,omitempty" yaml:"to,omitempty"`
}

type Cluster struct {
	Target              Target               `json:"target" yaml:"target"`
	DiscoveryState      DiscoveryState       `json:"discoveryState" yaml:"discoveryState"`
	ManagementState     ManagementState      `json:"managementState" yaml:"managementState"`
	ReachabilityState   ReachabilityState    `json:"reachabilityState" yaml:"reachabilityState"`
	HealthState         HealthState          `json:"healthState" yaml:"healthState"`
	Revision            int64                `json:"revision" yaml:"revision"`
	Initialize          string               `json:"initialize,omitempty" yaml:"initialize,omitempty"`
	Leader              string               `json:"leader,omitempty" yaml:"leader,omitempty"`
	Paused              bool                 `json:"paused" yaml:"paused"`
	ScheduledSwitchover *ScheduledSwitchover `json:"scheduledSwitchover,omitempty" yaml:"scheduledSwitchover,omitempty"`
	Members             []Member             `json:"members" yaml:"members"`
}

// ClusterSummary is the normalized, adapter-neutral projection returned by a
// bounded namespace discovery. Reachability and health remain unknown until a
// caller performs an endpoint health probe; DCS membership alone cannot prove
// either axis.
type ClusterSummary struct {
	Target            Target            `json:"target" yaml:"target"`
	DiscoveryState    DiscoveryState    `json:"discoveryState" yaml:"discoveryState"`
	ManagementState   ManagementState   `json:"managementState" yaml:"managementState"`
	ReachabilityState ReachabilityState `json:"reachabilityState" yaml:"reachabilityState"`
	HealthState       HealthState       `json:"healthState" yaml:"healthState"`
	Revision          int64             `json:"revision" yaml:"revision"`
	MemberCount       int               `json:"memberCount" yaml:"memberCount"`
	Leader            string            `json:"leader,omitempty" yaml:"leader,omitempty"`
}
