package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/postgres"
)

func (service *Service) List(ctx context.Context, request ListRequest) Result[ListData] {
	operationID := service.operationID()
	target := aggregateTarget(request.Targets)
	if !validContext(ctx) {
		return failedRead[ListData](service, operationID, "list", target, PathDCS, CategoryUsage, false, "list requires a context", nil)
	}
	if len(request.Targets) == 0 {
		return failedRead[ListData](service, operationID, "list", target, PathDCS, CategoryUsage, false, "list requires at least one target", nil)
	}
	data := ListData{Clusters: make([]model.Cluster, 0, len(request.Targets))}
	evidence := make([]Evidence, 0, len(request.Targets))
	for _, rawTarget := range request.Targets {
		clusterTarget := rawTarget.Normalize()
		if err := clusterTarget.Validate(true); err != nil {
			return failedReadWithEvidence[ListData](service, operationID, "list", clusterTarget, PathDCS, CategoryUsage, false, "list target is invalid", err, evidence)
		}
		if request.Citus && clusterTarget.Group == nil {
			snapshots, groupEvidence, failure := service.citusGroupSnapshots(ctx, "list", clusterTarget, request.AllowUnsupportedRead)
			evidence = append(evidence, groupEvidence...)
			if failure != nil {
				return failedReadWithEvidence[ListData](service, operationID, "list", clusterTarget, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
			}
			for _, snapshot := range snapshots {
				cluster := projectCluster(snapshot, snapshot.Target)
				cluster.DiscoveryState = model.DiscoveryDiscovered
				cluster.ManagementState = model.ManagementExplicit
				data.Clusters = append(data.Clusters, cluster)
			}
			continue
		}
		snapshot, err := service.snapshots.Snapshot(ctx, clusterTarget)
		if err != nil {
			category, retryable := classifyReadError(err)
			return failedReadWithEvidence[ListData](service, operationID, "list", clusterTarget, PathDCS, category, retryable, "cluster snapshot failed", err, evidence)
		}
		if versionError := checkSnapshotPatroniVersion(snapshot, request.AllowUnsupportedRead); versionError != nil {
			return unsupportedVersionResult[ListData](service, operationID, "list", clusterTarget, PathDCS, snapshot, versionError)
		}
		cluster := projectCluster(snapshot, clusterTarget)
		data.Clusters = append(data.Clusters, cluster)
		evidence = append(evidence, snapshotEvidence(service, snapshot, "cluster snapshot read"))
	}
	return Result[ListData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS,
		Data: data, Evidence: evidence,
	}
}

func (service *Service) DSN(ctx context.Context, request DSNRequest) Result[DSNData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	memberName := strings.TrimSpace(request.Member)
	if memberName == "" {
		memberName = target.Member
	} else if target.Member != "" && target.Member != memberName {
		return failedRead[DSNData](service, operationID, "dsn", target, PathDCS, CategoryUsage, false, "DSN member selectors conflict", nil)
	}
	target.Member = ""
	if !validContext(ctx) {
		return failedRead[DSNData](service, operationID, "dsn", target, PathDCS, CategoryUsage, false, "DSN requires a context", nil)
	}
	if err := target.Validate(true); err != nil {
		return failedRead[DSNData](service, operationID, "dsn", target, PathDCS, CategoryUsage, false, "DSN target is invalid", err)
	}
	if !request.Role.validOrEmpty() {
		return failedRead[DSNData](service, operationID, "dsn", target, PathDCS, CategoryUsage, false, "DSN role is invalid", nil)
	}
	if request.Role != "" && memberName != "" {
		return failedRead[DSNData](service, operationID, "dsn", target, PathDCS, CategoryUsage, false, "DSN role and member are mutually exclusive", nil)
	}
	snapshots, evidence, failure := service.operationSnapshots(ctx, "dsn", target, request.Citus, request.AllowUnsupportedRead)
	if failure != nil {
		return failedReadWithEvidence[DSNData](service, operationID, "dsn", target, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	role := request.Role
	if role == "" && memberName == "" {
		role = RoleLeader
	}
	selectedSnapshot, member, ok := selectMemberAcrossSnapshots(snapshots, role, memberName)
	if !ok {
		return failedReadWithEvidence[DSNData](service, operationID, "dsn", target, PathDCS, CategoryNotFound, false, "cannot find a suitable member", nil, evidence)
	}
	host, port, err := postgresAddress(member.Data.ConnURL)
	if err != nil {
		return failedReadWithEvidence[DSNData](service, operationID, "dsn", target, PathDCS, CategoryFailed, false, "selected member has no usable PostgreSQL address", err, evidence)
	}
	selectedTarget := selectedSnapshot.Target.Normalize()
	selectedTarget.Member = member.Name
	data := DSNData{Target: selectedTarget, Member: member.Name, Role: role, Host: host, Port: port}
	return Result[DSNData]{
		OperationID: operationID, Outcome: Succeeded, Target: selectedTarget, Path: PathDCS, Data: data,
		Evidence: evidence,
	}
}

func (service *Service) Topology(ctx context.Context, request TopologyRequest) Result[TopologyData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	if !validContext(ctx) {
		return failedRead[TopologyData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "topology requires a context", nil)
	}
	if err := target.Validate(true); err != nil {
		return failedRead[TopologyData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "topology target is invalid", err)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[TopologyData](service, operationID, "topology", target, PathDCS, category, retryable, "cluster snapshot failed", err)
	}
	if versionError := checkSnapshotPatroniVersion(snapshot, request.AllowUnsupportedRead); versionError != nil {
		return unsupportedVersionResult[TopologyData](service, operationID, "topology", target, PathDCS, snapshot, versionError)
	}
	cluster := projectCluster(snapshot, target)
	data := TopologyData{Cluster: cluster, Members: topologyMembers(cluster)}
	return Result[TopologyData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS, Data: data,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, "cluster topology projected")},
	}
}

// TopologyGroups projects all concrete Citus group roots for one scope from a
// bounded namespace discovery. Adapters use this for patronictl's group-less
// Citus topology command instead of assembling DCS results themselves.
func (service *Service) TopologyGroups(ctx context.Context, request TopologyGroupsRequest) Result[TopologyListData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	if !validContext(ctx) {
		return failedRead[TopologyListData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "Citus topology requires a context", nil)
	}
	if err := target.Validate(true); err != nil || target.Group != nil || target.Member != "" {
		if err == nil {
			err = errors.New("Citus topology group expansion requires a group-less cluster target")
		}
		return failedRead[TopologyListData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "Citus topology target is invalid", err)
	}
	snapshots, evidence, failure := service.operationSnapshots(ctx, "topology", target, true, request.AllowUnsupportedRead)
	if failure != nil {
		return failedReadWithEvidence[TopologyListData](service, operationID, "topology", target, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	topologies := make([]TopologyData, 0, len(snapshots))
	for _, snapshot := range snapshots {
		cluster := projectCluster(snapshot, snapshot.Target)
		cluster.DiscoveryState = model.DiscoveryDiscovered
		cluster.ManagementState = model.ManagementExplicit
		topologies = append(topologies, TopologyData{Cluster: cluster, Members: topologyMembers(cluster)})
	}
	return Result[TopologyListData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS,
		Data: TopologyListData{Topologies: topologies}, Evidence: evidence,
	}
}

func (service *Service) ShowConfig(ctx context.Context, request ShowConfigRequest) Result[ConfigData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	if !validContext(ctx) {
		return failedRead[ConfigData](service, operationID, "show-config", target, PathDCS, CategoryUsage, false, "show-config requires a context", nil)
	}
	if err := target.Validate(true); err != nil {
		return failedRead[ConfigData](service, operationID, "show-config", target, PathDCS, CategoryUsage, false, "show-config target is invalid", err)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[ConfigData](service, operationID, "show-config", target, PathDCS, category, retryable, "cluster snapshot failed", err)
	}
	if versionError := checkSnapshotPatroniVersion(snapshot, request.AllowUnsupportedRead); versionError != nil {
		return unsupportedVersionResult[ConfigData](service, operationID, "show-config", target, PathDCS, snapshot, versionError)
	}
	revision := int64(0)
	if entry, ok := snapshot.Entry("config"); ok {
		revision = entry.ModRevision
	}
	data := ConfigData{Target: target, Revision: revision, Config: cloneMap(snapshot.Cluster.Config)}
	return Result[ConfigData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS, Data: data,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, "dynamic configuration read")},
	}
}

func (service *Service) History(ctx context.Context, request HistoryRequest) Result[HistoryData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	if !validContext(ctx) {
		return failedRead[HistoryData](service, operationID, "history", target, PathDCS, CategoryUsage, false, "history requires a context", nil)
	}
	if err := target.Validate(true); err != nil {
		return failedRead[HistoryData](service, operationID, "history", target, PathDCS, CategoryUsage, false, "history target is invalid", err)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[HistoryData](service, operationID, "history", target, PathDCS, category, retryable, "cluster snapshot failed", err)
	}
	if versionError := checkSnapshotPatroniVersion(snapshot, request.AllowUnsupportedRead); versionError != nil {
		return unsupportedVersionResult[HistoryData](service, operationID, "history", target, PathDCS, snapshot, versionError)
	}
	entries := make([]HistoryEntry, 0, len(snapshot.Cluster.History))
	for _, raw := range snapshot.Cluster.History {
		entry, err := normalizeHistoryEntry(raw)
		if err != nil {
			return failedRead[HistoryData](service, operationID, "history", target, PathDCS, CategoryFailed, false, "cluster history is malformed", err, snapshotEvidence(service, snapshot, "cluster history read"))
		}
		entries = append(entries, entry)
	}
	data := HistoryData{Target: target, Entries: entries}
	return Result[HistoryData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS, Data: data,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, "cluster history read")},
	}
}

// Query implements patronictl member/role selection while delegating all
// PostgreSQL wire behavior to the native pgx-backed postgres package.
func (service *Service) Query(ctx context.Context, request QueryRequest) Result[QueryData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	memberName := strings.TrimSpace(request.Member)
	if memberName == "" {
		memberName = target.Member
	} else if target.Member != "" && target.Member != memberName {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryUsage, false, "query member selectors conflict", nil)
	}
	target.Member = ""
	if !validContext(ctx) {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryUsage, false, "query requires a context", nil)
	}
	if err := target.Validate(true); err != nil {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryUsage, false, "query target is invalid", err)
	}
	if !request.Role.validOrEmpty() {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryUsage, false, "query role is invalid", nil)
	}
	if request.Role != "" && memberName != "" {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryUsage, false, "query role and member are mutually exclusive", nil)
	}
	if service.postgres == nil {
		return failedRead[QueryData](service, operationID, "query", target, PathPostgres, CategoryConfig, false, "query requires a PostgreSQL executor", nil)
	}
	// Query may execute arbitrary SQL and is therefore never a best-effort
	// read, even when an adapter exposes --allow-unsupported-read.
	snapshots, evidence, failure := service.operationSnapshots(ctx, "query", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[QueryData](service, operationID, "query", target, PathPostgres, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	role := request.Role
	if role == "" && memberName == "" {
		role = RoleLeader
	}
	selectedSnapshot, member, ok := selectMemberAcrossSnapshots(snapshots, role, memberName)
	if !ok {
		message := "No connection is available"
		if memberName != "" {
			message = "No connection to member " + memberName + " is available"
		} else if request.Role != "" {
			message = "No connection to role " + string(request.Role) + " is available"
		}
		return compatibleQueryError(service, operationID, target, "", QueryErrorNoConnection, "", message, evidence)
	}
	host, port, addressErr := postgresAddress(member.Data.ConnURL)
	if addressErr != nil {
		return compatibleQueryError(service, operationID, target, member.Name, QueryErrorNoConnection, "", "No connection to member "+member.Name+" is available", evidence)
	}
	selectedTarget := selectedSnapshot.Target.Normalize()
	selectedTarget.Member = member.Name
	connection := request.Connection
	connection.Host = host
	connection.Port = port
	if connection.ApplicationName == "" {
		connection.ApplicationName = "Patroni ctl"
	}
	expectation := postgres.RecoveryAny
	if memberName == "" {
		switch role {
		case RolePrimary:
			expectation = postgres.RecoveryPrimary
		case RoleReplica, RoleStandby, RoleStandbyLeader:
			expectation = postgres.RecoveryStandby
		}
	}
	queryResult, queryErr := service.postgres.QueryChecked(ctx, connection, expectation, postgres.QueryRequest{SQL: request.SQL, Limits: request.Limits})
	evidence = append(evidence, Evidence{Source: EvidencePostgres, ObservedAt: service.now(), Summary: "PostgreSQL query attempt completed", Path: string(PathPostgres)})
	if queryErr != nil {
		var typed *postgres.Error
		if errors.As(queryErr, &typed) {
			switch typed.Kind {
			case postgres.ErrorCanceled:
				return failedRead[QueryData](service, operationID, "query", selectedTarget, PathPostgres, CategoryFailed, false, "query canceled", queryErr, evidence...)
			case postgres.ErrorDeadline:
				return failedRead[QueryData](service, operationID, "query", selectedTarget, PathPostgres, CategoryUnreachable, true, "query deadline exceeded", queryErr, evidence...)
			case postgres.ErrorConfiguration, postgres.ErrorInvariant, postgres.ErrorSink, postgres.ErrorLimit:
				return failedRead[QueryData](service, operationID, "query", selectedTarget, PathPostgres, CategoryConfig, false, "query execution is invalid", queryErr, evidence...)
			case postgres.ErrorRoleMismatch:
				return compatibleQueryError(service, operationID, selectedTarget, member.Name, QueryErrorRoleMismatch, "", "No connection to role "+string(role)+" is available", evidence)
			default:
				message := "ERROR, SQLSTATE: " + typed.SQLState
				if typed.SQLState == "" {
					message = "ERROR, SQLSTATE: DATABASE"
				}
				return compatibleQueryError(service, operationID, selectedTarget, member.Name, QueryErrorDatabase, typed.SQLState, message, evidence)
			}
		}
		return compatibleQueryError(service, operationID, selectedTarget, member.Name, QueryErrorDatabase, "", "ERROR, SQLSTATE: DATABASE", evidence)
	}
	data := QueryData{Target: selectedTarget, Member: member.Name, Result: queryResult}
	return Result[QueryData]{OperationID: operationID, Outcome: Succeeded, Target: selectedTarget, Path: PathPostgres, Data: data, Evidence: evidence}
}

func compatibleQueryError(
	service *Service,
	operationID string,
	target model.Target,
	member string,
	kind QueryErrorKind,
	sqlState string,
	message string,
	evidence []Evidence,
) Result[QueryData] {
	if member != "" {
		target.Member = member
	}
	evidence = append(evidence, Evidence{Source: EvidencePostgres, ObservedAt: service.now(), Summary: "compatible query error row produced", Path: string(PathPostgres)})
	data := QueryData{Target: target, Member: member, Error: &QueryError{Kind: kind, SQLState: sqlState, Message: message}}
	return Result[QueryData]{OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathPostgres, Data: data, Evidence: evidence}
}

func (service *Service) Version(ctx context.Context, request VersionRequest) Result[VersionData] {
	operationID := service.operationID()
	target := request.Target.Normalize()
	data := VersionData{ProductVersion: service.productVersion, Members: make([]MemberVersion, 0)}
	if !validContext(ctx) {
		return failedRead[VersionData](service, operationID, "version", target, PathLocal, CategoryUsage, false, "version requires a context", nil)
	}
	localEvidence := Evidence{Source: EvidenceLocal, ObservedAt: service.now(), Summary: "local BOAR version read", Path: string(PathLocal)}
	if target.Scope == "" {
		return Result[VersionData]{OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathLocal, Data: data, Evidence: []Evidence{localEvidence}}
	}
	if err := target.Validate(true); err != nil {
		return failedRead[VersionData](service, operationID, "version", target, PathREST, CategoryUsage, false, "version target is invalid", err, localEvidence)
	}
	if service.patroni == nil {
		return failedRead[VersionData](service, operationID, "version", target, PathREST, CategoryConfig, false, "version requires a Patroni REST reader", nil, localEvidence)
	}
	snapshots, snapshotEvidence, failure := service.versionSnapshots(ctx, "version", target, request.Citus, request.AllowUnsupportedRead)
	evidence := append([]Evidence{localEvidence}, snapshotEvidence...)
	if failure != nil {
		return failedReadWithEvidence[VersionData](service, operationID, "version", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	filter := stringSet(request.Members)
	for _, snapshot := range snapshots {
		for _, member := range snapshot.Cluster.Members {
			if len(filter) > 0 {
				if _, ok := filter[member.Name]; !ok {
					continue
				}
			}
			if strings.TrimSpace(member.Data.APIURL) == "" {
				continue
			}
			memberTarget := snapshot.Target.Normalize()
			memberTarget.Member = member.Name
			item := MemberVersion{Target: memberTarget}
			baseURL, baseErr := patroniBaseURL(member.Data.APIURL)
			if baseErr != nil {
				item.Error = NewError(CategoryConfig, "version", memberTarget, false, "member Patroni address is invalid", baseErr)
				data.Members = append(data.Members, item)
				continue
			}
			response, callErr := service.patroni.GetPatroni(ctx, baseURL)
			item.HTTPStatus = response.StatusCode
			if callErr != nil {
				category, retryable := classifyReadError(callErr)
				item.Error = NewError(category, "version", memberTarget, retryable, "failed to get member version", callErr)
			} else if response.StatusCode < 200 || response.StatusCode >= 300 {
				category, retryable := classifyHTTPStatus(response.StatusCode)
				item.Error = NewError(category, "version", memberTarget, retryable, "failed to get member version", nil)
			} else {
				item.PatroniVersion = response.Data.Patroni.Version
				if !request.AllowUnsupportedRead {
					if versionError := checkPatroniVersion(item.PatroniVersion); versionError != nil {
						return unsupportedVersionResult[VersionData](service, operationID, "version", target, PathREST, snapshot, versionError)
					}
				}
				if response.Data.ServerVersion != nil {
					item.PostgresVersion = formatPostgresVersion(*response.Data.ServerVersion)
				}
			}
			data.Members = append(data.Members, item)
		}
	}
	return Result[VersionData]{OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathREST, Data: data, Evidence: evidence}
}

func aggregateTarget(targets []model.Target) model.Target {
	if len(targets) == 0 {
		return (model.Target{}).Normalize()
	}
	first := targets[0].Normalize()
	if len(targets) == 1 {
		first.Member = ""
		return first
	}
	aggregate := model.Target{Context: first.Context, Namespace: first.Namespace}.Normalize()
	for _, raw := range targets[1:] {
		target := raw.Normalize()
		if target.Context != aggregate.Context || target.Namespace != aggregate.Namespace {
			return (model.Target{}).Normalize()
		}
	}
	return aggregate
}

func projectCluster(snapshot dcs.Snapshot, requested model.Target) model.Cluster {
	target := snapshot.Target.Normalize()
	if target.Scope == "" {
		target = requested.Normalize()
	}
	cluster := model.Cluster{
		Target: target, Revision: snapshot.Revision, Initialize: snapshot.Cluster.Initialize,
		DiscoveryState: discoveryStateForSnapshot(snapshot), ManagementState: model.ManagementExplicit,
		ReachabilityState: model.ReachabilityUnknown, HealthState: model.HealthUnknown,
		Members: make([]model.Member, 0, len(snapshot.Cluster.Members)),
	}
	if snapshot.Cluster.Leader != nil {
		cluster.Leader = snapshot.Cluster.Leader.Name
	}
	cluster.Paused = boolConfig(snapshot.Cluster.Config["pause"])
	standby := standbyCluster(snapshot.Cluster.Config)
	synchronous := !standby && synchronousMode(snapshot.Cluster.Config)
	quorum := strings.EqualFold(fmt.Sprint(snapshot.Cluster.Config["synchronous_mode"]), "quorum")
	syncMembers := stringSet(snapshot.Cluster.Sync.Standbys)
	for _, member := range snapshot.Cluster.Members {
		memberTarget := target
		memberTarget.Member = member.Name
		role := model.RoleReplica
		if member.Name == cluster.Leader {
			role = model.RoleLeader
			if standby {
				role = model.RoleStandbyLeader
			}
		} else if synchronous {
			if _, ok := syncMembers[member.Name]; ok {
				role = model.RoleSyncStandby
				if quorum {
					role = model.RoleQuorumStandby
				}
			}
		}
		host, port, _ := postgresAddress(member.Data.ConnURL)
		state := member.Data.State
		if member.Name != cluster.Leader && member.Data.ReplicationState != "" {
			state = member.Data.ReplicationState
		}
		projected := model.Member{
			Target: memberTarget, Name: member.Name, APIURL: member.Data.APIURL, Host: host, Port: port,
			Role: role, State: state, Timeline: cloneInt(member.Data.Timeline),
			PendingRestart:       member.Data.PendingRestart != nil && *member.Data.PendingRestart,
			PendingRestartReason: cloneMap(member.Data.PendingRestartReason),
			Tags:                 cloneMap(member.Data.Tags), PatroniVersion: member.Data.PatroniVersion,
		}
		if member.Name != cluster.Leader {
			projectLSN(&projected, member.Data, snapshot.Cluster.Status.LastLSN)
		}
		if scheduled := decodeScheduledRestart(member.Data.ScheduledRestart); scheduled != nil {
			projected.ScheduledRestart = scheduled
		}
		cluster.Members = append(cluster.Members, projected)
	}
	sort.SliceStable(cluster.Members, func(left, right int) bool { return cluster.Members[left].Name < cluster.Members[right].Name })
	if failover := snapshot.Cluster.Failover; failover != nil && failover.ScheduledAt != "" {
		cluster.ScheduledSwitchover = &model.ScheduledSwitchover{At: failover.ScheduledAt, From: failover.Leader, To: failover.Candidate}
	}
	return cluster
}

func discoveryStateForSnapshot(snapshot dcs.Snapshot) model.DiscoveryState {
	for _, entry := range snapshot.Entries {
		if entry.Kind != dcs.KeyUnknown {
			return model.DiscoveryDiscovered
		}
	}
	return model.DiscoveryAbsent
}

func projectLSN(member *model.Member, data dcs.MemberData, clusterLSN int64) {
	receive, replay := data.ReceiveLSN, data.ReplayLSN
	if receive == nil && replay == nil && data.XLogLocation != nil {
		if member.State == "streaming" {
			receive = data.XLogLocation
		} else {
			replay = data.XLogLocation
		}
	}
	if receive != nil {
		member.ReceiveLSN = formatLSN(*receive)
		member.ReceiveLagBytes = nonnegativeLag(clusterLSN, *receive)
	}
	if replay != nil {
		member.ReplayLSN = formatLSN(*replay)
		member.ReplayLagBytes = nonnegativeLag(clusterLSN, *replay)
	}
}

func formatLSN(value int64) string {
	return fmt.Sprintf("%X/%X", uint64(value)>>32, uint64(value)&0xffffffff)
}

func nonnegativeLag(cluster, member int64) int64 {
	if cluster <= member {
		return 0
	}
	return cluster - member
}

func selectMember(cluster dcs.ClusterState, role Role, memberName string) (dcs.Member, bool) {
	leaderName := ""
	if cluster.Leader != nil {
		leaderName = cluster.Leader.Name
	}
	if memberName != "" {
		for _, member := range cluster.Members {
			if member.Name == memberName {
				return member, true
			}
		}
		return dcs.Member{}, false
	}
	if role == "" {
		role = RoleLeader
	}
	for _, member := range cluster.Members {
		isLeader := member.Name == leaderName
		wireRole := strings.ReplaceAll(strings.ToLower(member.Data.Role), "-", "_")
		matched := false
		switch role {
		case RoleAny:
			matched = true
		case RoleLeader:
			matched = isLeader
		case RolePrimary:
			matched = isLeader && wireRole != "standby_leader"
		case RoleStandbyLeader:
			matched = isLeader && wireRole != "primary" && wireRole != "master"
		case RoleReplica, RoleStandby:
			matched = !isLeader
		}
		if matched {
			return member, true
		}
	}
	return dcs.Member{}, false
}

func postgresAddress(connectionURL string) (string, uint16, error) {
	parsed, err := url.Parse(strings.TrimSpace(connectionURL))
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" {
		return "", 0, errors.New("invalid PostgreSQL connection URL")
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", 0, errors.New("unsupported PostgreSQL connection URL scheme")
	}
	port := uint64(5432)
	if parsed.Port() != "" {
		port, err = strconv.ParseUint(parsed.Port(), 10, 16)
		if err != nil || port == 0 {
			return "", 0, errors.New("invalid PostgreSQL connection URL port")
		}
	}
	return parsed.Hostname(), uint16(port), nil
}

func topologyMembers(cluster model.Cluster) []TopologyMember {
	members := make(map[string]model.Member, len(cluster.Members))
	replicas := make(map[string]struct{}, len(cluster.Members))
	for _, member := range cluster.Members {
		members[member.Name] = member
		if member.Name != cluster.Leader {
			replicas[member.Name] = struct{}{}
		}
	}
	children := make(map[string][]string)
	parents := make(map[string]string)
	for _, member := range cluster.Members {
		if member.Name == cluster.Leader {
			continue
		}
		parent := cluster.Leader
		if candidate, ok := member.Tags["replicatefrom"].(string); ok && candidate != member.Name {
			if _, exists := replicas[candidate]; exists {
				parent = candidate
			}
		}
		parents[member.Name] = parent
		children[parent] = append(children[parent], member.Name)
	}
	for parent := range children {
		sort.Strings(children[parent])
	}
	result := make([]TopologyMember, 0, len(cluster.Members))
	visited := make(map[string]bool, len(cluster.Members))
	var walk func(string, string, int)
	walk = func(name, parent string, depth int) {
		if name == "" || visited[name] {
			return
		}
		member, ok := members[name]
		if !ok {
			return
		}
		visited[name] = true
		result = append(result, TopologyMember{Member: member, Parent: parent, Depth: depth})
		for _, child := range children[name] {
			walk(child, name, depth+1)
		}
	}
	walk(cluster.Leader, "", 0)
	for _, member := range cluster.Members {
		if !visited[member.Name] {
			walk(member.Name, parents[member.Name], 0)
		}
	}
	return result
}

func normalizeHistoryEntry(raw json.RawMessage) (HistoryEntry, error) {
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return HistoryEntry{}, err
	}
	if len(fields) < 3 {
		return HistoryEntry{}, errors.New("history entry has fewer than three fields")
	}
	entry := HistoryEntry{}
	if err := json.Unmarshal(fields[0], &entry.Timeline); err != nil {
		return HistoryEntry{}, err
	}
	if err := json.Unmarshal(fields[1], &entry.LSN); err != nil {
		return HistoryEntry{}, err
	}
	if err := json.Unmarshal(fields[2], &entry.Reason); err != nil {
		return HistoryEntry{}, err
	}
	if len(fields) > 3 && string(fields[3]) != "null" {
		if err := json.Unmarshal(fields[3], &entry.Timestamp); err != nil {
			return HistoryEntry{}, err
		}
	}
	if len(fields) > 4 && string(fields[4]) != "null" {
		if err := json.Unmarshal(fields[4], &entry.NewLeader); err != nil {
			return HistoryEntry{}, err
		}
	}
	return entry, nil
}

func snapshotEvidence(service *Service, snapshot dcs.Snapshot, summary string) Evidence {
	return Evidence{
		Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary,
		Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix,
	}
}

func failedRead[T any](service *Service, operationID, operation string, target model.Target, path Path, category Category, retryable bool, message string, cause error, evidence ...Evidence) Result[T] {
	return failedReadWithEvidence[T](service, operationID, operation, target, path, category, retryable, message, cause, evidence)
}

func failedReadWithEvidence[T any](service *Service, operationID, operation string, target model.Target, path Path, category Category, retryable bool, message string, cause error, evidence []Evidence) Result[T] {
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: evidenceSourceForPath(path), ObservedAt: service.now(), Summary: message, Path: string(path)}}
	}
	return Result[T]{
		OperationID: operationID, Outcome: Failed, Target: target.Normalize(), Path: path, Evidence: evidence,
		Error: NewError(category, operation, target, retryable, message, cause, evidence...),
	}
}

func evidenceSourceForPath(path Path) EvidenceSource {
	switch path {
	case PathREST:
		return EvidencePatroni
	case PathPostgres:
		return EvidencePostgres
	case PathLocal:
		return EvidenceLocal
	default:
		return EvidenceDCS
	}
}

func classifyReadError(err error) (Category, bool) {
	if errors.Is(err, context.Canceled) {
		return CategoryFailed, false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return CategoryUnreachable, true
	}
	var dcsError *dcs.Error
	if errors.As(err, &dcsError) {
		switch dcsError.Kind {
		case dcs.ErrorConfiguration:
			return CategoryConfig, false
		case dcs.ErrorAuthentication:
			return CategoryAuth, false
		case dcs.ErrorConflict:
			return CategoryConflict, false
		case dcs.ErrorCanceled:
			return CategoryFailed, false
		case dcs.ErrorDeadline, dcs.ErrorTransport:
			return CategoryUnreachable, true
		default:
			return CategoryFailed, false
		}
	}
	var patroniError *patroni.Error
	if errors.As(err, &patroniError) {
		switch patroniError.Kind {
		case patroni.ErrorAuthentication:
			return CategoryAuth, false
		case patroni.ErrorRequest:
			return CategoryConfig, false
		case patroni.ErrorTransport:
			return CategoryUnreachable, true
		default:
			return CategoryFailed, false
		}
	}
	return CategoryUnreachable, true
}

func classifyHTTPStatus(status int) (Category, bool) {
	switch {
	case status == 401 || status == 403:
		return CategoryAuth, false
	case status == 404:
		return CategoryNotFound, false
	case status == 409 || status == 412:
		return CategoryConflict, false
	case status >= 500:
		return CategoryUnreachable, true
	default:
		return CategoryFailed, false
	}
}

func patroniBaseURL(apiURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(apiURL))
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("invalid Patroni API URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("Patroni API URL contains query or fragment")
	}
	parsed.Path = strings.TrimSuffix(strings.TrimRight(parsed.Path, "/"), "/patroni")
	parsed.RawPath = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func formatPostgresVersion(version int) string {
	if version < 100000 {
		return fmt.Sprintf("%d.%d.%d", version/10000, version/100%100, version%100)
	}
	return fmt.Sprintf("%d.%d", version/10000, version%100)
}

func boolConfig(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		}
	case json.Number:
		return typed.String() == "1"
	case float64:
		return typed == 1
	}
	return false
}

func synchronousMode(configuration map[string]any) bool {
	value := configuration["synchronous_mode"]
	return boolConfig(value) || strings.EqualFold(fmt.Sprint(value), "quorum")
}

func standbyCluster(configuration map[string]any) bool {
	raw, ok := configuration["standby_cluster"].(map[string]any)
	if !ok {
		return false
	}
	for _, key := range []string{"host", "port", "restore_command"} {
		if value, exists := raw[key]; exists && strings.TrimSpace(fmt.Sprint(value)) != "" && fmt.Sprint(value) != "0" {
			return true
		}
	}
	return false
}

func decodeScheduledRestart(raw json.RawMessage) *model.ScheduledRestart {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var value struct {
		Schedule        string `json:"schedule"`
		PostgresVersion string `json:"postgres_version"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Schedule == "" {
		return nil
	}
	return &model.ScheduledRestart{Schedule: value.Schedule, PostgresVersion: value.PostgresVersion}
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return map[string]any{}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	output := make(map[string]any)
	if decoder.Decode(&output) != nil {
		return map[string]any{}
	}
	return output
}

func cloneInt(input *int) *int {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}
