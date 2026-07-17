package control

import (
	"context"
	"fmt"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
)

// PatroniStatusReader is the minimal REST capability needed by the version
// use case. *patroni.Client satisfies this interface.
type PatroniStatusReader interface {
	GetPatroni(context.Context, string) (patroni.Response[patroni.Status], error)
}

// PatroniControlClient is the exact REST capability set required by the M3
// command algorithms. It remains a typed HTTP-wire port; fallback and outcome
// logic stay in Service.
type PatroniControlClient interface {
	PatroniStatusReader
	PostReload(context.Context, string) (patroni.Response[string], error)
	PostRestart(context.Context, string, patroni.RestartRequest) (patroni.Response[string], error)
	DeleteRestart(context.Context, string) (patroni.Response[string], error)
	PostReinitialize(context.Context, string, patroni.ReinitializeRequest) (patroni.Response[string], error)
	PostFailover(context.Context, string, patroni.FailoverRequest) (patroni.Response[string], error)
	PostSwitchover(context.Context, string, patroni.FailoverRequest) (patroni.Response[string], error)
	DeleteSwitchover(context.Context, string) (patroni.Response[string], error)
	PatchConfig(context.Context, string, patroni.DynamicConfig) (patroni.Response[patroni.DynamicConfig], error)
}

// PostgresQueryExecutor is the one-shot query capability used by control.
// *postgres.Client satisfies it and guarantees the optional role check and
// user SQL run on the same connection.
type PostgresQueryExecutor interface {
	QueryChecked(context.Context, postgres.ConnectionOptions, postgres.RecoveryExpectation, postgres.QueryRequest) (postgres.QueryResult, error)
}

type Role string

const (
	RoleLeader        Role = "leader"
	RolePrimary       Role = "primary"
	RoleStandbyLeader Role = "standby-leader"
	RoleReplica       Role = "replica"
	RoleStandby       Role = "standby"
	RoleAny           Role = "any"
)

func (role Role) validOrEmpty() bool {
	switch role {
	case "", RoleLeader, RolePrimary, RoleStandbyLeader, RoleReplica, RoleStandby, RoleAny:
		return true
	default:
		return false
	}
}

type ListRequest struct {
	Targets              []model.Target `json:"targets" yaml:"targets"`
	Citus                bool           `json:"citus" yaml:"citus"`
	AllowUnsupportedRead bool           `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type ListData struct {
	Clusters []model.Cluster `json:"clusters" yaml:"clusters"`
}

// DiscoverRequest selects one named context and one Patroni namespace. The
// service performs a bounded namespace scan and excludes unrelated/orphan
// keys through the DCS discovery contract.
type DiscoverRequest struct {
	Context              string `json:"context" yaml:"context"`
	Namespace            string `json:"namespace" yaml:"namespace"`
	AllowUnsupportedRead bool   `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type DiscoverData struct {
	Clusters []model.ClusterSummary `json:"clusters" yaml:"clusters"`
}

type ListAllRequest struct {
	Context              string `json:"context" yaml:"context"`
	Namespace            string `json:"namespace" yaml:"namespace"`
	AllowUnsupportedRead bool   `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type DSNRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	Role                 Role         `json:"role,omitempty" yaml:"role,omitempty"`
	Member               string       `json:"member,omitempty" yaml:"member,omitempty"`
	Citus                bool         `json:"citus" yaml:"citus"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type DSNData struct {
	Target model.Target `json:"target" yaml:"target"`
	Member string       `json:"member" yaml:"member"`
	Role   Role         `json:"role" yaml:"role"`
	Host   string       `json:"host" yaml:"host"`
	Port   uint16       `json:"port" yaml:"port"`
}

func (data DSNData) String() string {
	return fmt.Sprintf("host=%s port=%d", data.Host, data.Port)
}

func (data DSNData) GoString() string { return data.String() }

type TopologyRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type TopologyMember struct {
	Member model.Member `json:"member" yaml:"member"`
	Parent string       `json:"parent,omitempty" yaml:"parent,omitempty"`
	Depth  int          `json:"depth" yaml:"depth"`
}

type TopologyData struct {
	Cluster model.Cluster    `json:"cluster" yaml:"cluster"`
	Members []TopologyMember `json:"members" yaml:"members"`
}

// TopologyGroupsRequest selects every Patroni Citus group beneath one exact
// scope. It is separate from TopologyAll, which intentionally spans scopes.
type TopologyGroupsRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type TopologyAllRequest struct {
	Context              string `json:"context" yaml:"context"`
	Namespace            string `json:"namespace" yaml:"namespace"`
	AllowUnsupportedRead bool   `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type TopologyListData struct {
	Topologies []TopologyData `json:"topologies" yaml:"topologies"`
}

type ShowConfigRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type ConfigData struct {
	Target   model.Target   `json:"target" yaml:"target"`
	Revision int64          `json:"revision" yaml:"revision"`
	Config   map[string]any `json:"config" yaml:"config"`
}

type HistoryRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type HistoryEntry struct {
	Timeline  int64  `json:"timeline" yaml:"timeline"`
	LSN       int64  `json:"lsn" yaml:"lsn"`
	Reason    string `json:"reason" yaml:"reason"`
	Timestamp string `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	NewLeader string `json:"newLeader,omitempty" yaml:"newLeader,omitempty"`
}

type HistoryData struct {
	Target  model.Target   `json:"target" yaml:"target"`
	Entries []HistoryEntry `json:"entries" yaml:"entries"`
}

type QueryRequest struct {
	Target     model.Target               `json:"target" yaml:"target"`
	Role       Role                       `json:"role,omitempty" yaml:"role,omitempty"`
	Member     string                     `json:"member,omitempty" yaml:"member,omitempty"`
	Citus      bool                       `json:"citus" yaml:"citus"`
	Connection postgres.ConnectionOptions `json:"-" yaml:"-"`
	SQL        string                     `json:"-" yaml:"-"`
	Limits     postgres.Limits            `json:"limits" yaml:"limits"`
}

func (request QueryRequest) String() string {
	return fmt.Sprintf("control.QueryRequest{target:%s,role:%q,member:%q,citus:%t,connection:%s,sql:[REDACTED],maxRows:%d,maxBytes:%d,unlimited:%t}",
		request.Target.Normalize().ClusterID(), request.Role, request.Member, request.Citus, request.Connection.String(),
		request.Limits.MaxRows, request.Limits.MaxBytes, request.Limits.Unlimited)
}

func (request QueryRequest) GoString() string { return request.String() }

type QueryErrorKind string

const (
	QueryErrorNoConnection QueryErrorKind = "NO_CONNECTION"
	QueryErrorDatabase     QueryErrorKind = "DATABASE"
	QueryErrorRoleMismatch QueryErrorKind = "ROLE_MISMATCH"
)

type QueryError struct {
	Kind     QueryErrorKind `json:"kind" yaml:"kind"`
	SQLState string         `json:"sqlState,omitempty" yaml:"sqlState,omitempty"`
	Message  string         `json:"message" yaml:"message"`
}

type QueryData struct {
	Target model.Target         `json:"target" yaml:"target"`
	Member string               `json:"member,omitempty" yaml:"member,omitempty"`
	Result postgres.QueryResult `json:"result" yaml:"result"`
	Error  *QueryError          `json:"error,omitempty" yaml:"error,omitempty"`
}

type VersionRequest struct {
	Target               model.Target `json:"target" yaml:"target"`
	Members              []string     `json:"members,omitempty" yaml:"members,omitempty"`
	Citus                bool         `json:"citus" yaml:"citus"`
	AllowUnsupportedRead bool         `json:"allowUnsupportedRead" yaml:"allowUnsupportedRead"`
}

type MemberVersion struct {
	Target          model.Target `json:"target" yaml:"target"`
	PatroniVersion  string       `json:"patroniVersion,omitempty" yaml:"patroniVersion,omitempty"`
	PostgresVersion string       `json:"postgresVersion,omitempty" yaml:"postgresVersion,omitempty"`
	HTTPStatus      int          `json:"httpStatus,omitempty" yaml:"httpStatus,omitempty"`
	Error           *Error       `json:"error,omitempty" yaml:"error,omitempty"`
}

type VersionData struct {
	ProductVersion string          `json:"productVersion" yaml:"productVersion"`
	Members        []MemberVersion `json:"members" yaml:"members"`
}
