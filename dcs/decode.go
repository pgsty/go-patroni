package dcs

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/model"
)

func BuildSnapshot(target model.Target, prefix string, revision int64, entries []Entry) Snapshot {
	target = target.Normalize()
	cloned := make([]Entry, len(entries))
	for index, entry := range entries {
		cloned[index] = entry.Clone()
		if cloned[index].RelativePath == "" {
			cloned[index].RelativePath = strings.TrimPrefix(strings.TrimPrefix(entry.Key, strings.TrimRight(prefix, "/")), "/")
		}
		cloned[index].Kind = ClassifyRelativePath(cloned[index].RelativePath)
	}
	SortEntries(cloned)
	snapshot := Snapshot{Target: target, Prefix: strings.TrimRight(prefix, "/"), Revision: revision, Entries: cloned}
	snapshot.decodeCluster()
	return snapshot
}

func (snapshot *Snapshot) issue(relative, field, reason string) {
	snapshot.Issues = append(snapshot.Issues, DecodeIssue{RelativePath: relative, Field: field, Reason: reason})
}

func (snapshot *Snapshot) decodeCluster() {
	entries := make(map[string]Entry, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		entries[entry.RelativePath] = entry
		switch entry.Kind {
		case KeyInitialize:
			snapshot.Cluster.Initialize = string(entry.Value)
		case KeyConfig:
			var configuration map[string]any
			if err := decodeJSON(entry.Value, &configuration); err != nil || configuration == nil {
				snapshot.issue(entry.RelativePath, "value", "dynamic configuration is not a JSON object")
				configuration = map[string]any{}
			}
			snapshot.Cluster.Config = configuration
		case KeyMember:
			snapshot.Cluster.Members = append(snapshot.Cluster.Members, snapshot.decodeMember(entry))
		case KeyLeader:
			snapshot.Cluster.Leader = &Leader{Name: string(entry.Value), ModRevision: entry.ModRevision, Lease: entry.Lease}
		case KeyFailover:
			snapshot.Cluster.Failover = snapshot.decodeFailover(entry)
		case KeyHistory:
			var history []json.RawMessage
			if err := json.Unmarshal(entry.Value, &history); err != nil {
				snapshot.issue(entry.RelativePath, "value", "history is not a JSON array")
			} else {
				snapshot.Cluster.History = history
			}
		case KeySync:
			snapshot.Cluster.Sync = snapshot.decodeSync(entry)
		case KeyFailsafe:
			var topology map[string]string
			if err := json.Unmarshal(entry.Value, &topology); err != nil {
				snapshot.issue(entry.RelativePath, "value", "failsafe topology is not a string map")
			} else {
				snapshot.Cluster.Failsafe = topology
			}
		}
	}
	status, hasStatus := entries["status"]
	if hasStatus {
		snapshot.Cluster.Status = snapshot.decodeStatus(status)
	} else if legacy, ok := entries["optime/leader"]; ok {
		snapshot.Cluster.Status = snapshot.decodeStatus(legacy)
	}
	sortMembers(snapshot.Cluster.Members)
}

func (snapshot *Snapshot) decodeMember(entry Entry) Member {
	name := strings.TrimPrefix(entry.RelativePath, "members/")
	member := Member{Name: name, ModRevision: entry.ModRevision, Lease: entry.Lease}
	if strings.HasPrefix(string(entry.Value), "postgres") {
		member.Data.ConnURL, member.Data.APIURL = parseLegacyConnectionString(string(entry.Value))
		return member
	}
	var data struct {
		APIURL               string                 `json:"api_url"`
		ConnURL              string                 `json:"conn_url"`
		State                string                 `json:"state"`
		Role                 string                 `json:"role"`
		ReplicationState     string                 `json:"replication_state"`
		Timeline             *int                   `json:"timeline"`
		PendingRestart       *bool                  `json:"pending_restart"`
		PendingRestartReason map[string]any         `json:"pending_restart_reason"`
		ScheduledRestart     json.RawMessage        `json:"scheduled_restart"`
		Tags                 map[string]any         `json:"tags"`
		Version              string                 `json:"version"`
		Pause                *bool                  `json:"pause"`
		XLogLocation         *int64                 `json:"xlog_location"`
		ReceiveLSN           *int64                 `json:"receive_lsn"`
		ReplayLSN            *int64                 `json:"replay_lsn"`
		Slots                map[string]json.Number `json:"slots"`
	}
	if err := decodeJSON(entry.Value, &data); err != nil {
		snapshot.issue(entry.RelativePath, "value", "member value is not a JSON object or legacy PostgreSQL URL")
		return member
	}
	member.Data = MemberData{
		APIURL: data.APIURL, ConnURL: data.ConnURL, State: data.State, Role: data.Role,
		ReplicationState: data.ReplicationState, Timeline: data.Timeline, PendingRestart: data.PendingRestart,
		PendingRestartReason: data.PendingRestartReason,
		ScheduledRestart:     append(json.RawMessage(nil), data.ScheduledRestart...), Tags: data.Tags,
		PatroniVersion: data.Version, Pause: data.Pause, XLogLocation: data.XLogLocation,
		ReceiveLSN: data.ReceiveLSN, ReplayLSN: data.ReplayLSN, Slots: data.Slots,
	}
	return member
}

func (snapshot *Snapshot) decodeFailover(entry Entry) *Failover {
	failover := &Failover{ModRevision: entry.ModRevision}
	var data struct {
		Leader      string `json:"leader"`
		Member      string `json:"member"`
		ScheduledAt string `json:"scheduled_at"`
	}
	if err := decodeJSON(entry.Value, &data); err == nil {
		failover.Leader = data.Leader
		failover.Candidate = data.Member
		failover.ScheduledAt = data.ScheduledAt
		return failover
	}
	parts := strings.Split(string(entry.Value), ":")
	if len(parts) > 0 {
		failover.Leader = strings.TrimSpace(parts[0])
	}
	if len(parts) > 1 {
		failover.Candidate = strings.TrimSpace(parts[1])
	}
	if failover.Leader == "" && failover.Candidate == "" {
		snapshot.issue(entry.RelativePath, "value", "failover value is neither JSON nor legacy leader:candidate")
	}
	return failover
}

func (snapshot *Snapshot) decodeSync(entry Entry) SyncState {
	state := SyncState{ModRevision: entry.ModRevision}
	var data struct {
		Leader      string `json:"leader"`
		SyncStandby string `json:"sync_standby"`
		Quorum      any    `json:"quorum"`
	}
	if err := decodeJSON(entry.Value, &data); err != nil {
		snapshot.issue(entry.RelativePath, "value", "sync state is not a JSON object")
		return state
	}
	state.Leader = data.Leader
	if state.Leader == "" {
		return state
	}
	for _, standby := range strings.Split(data.SyncStandby, ",") {
		if standby = strings.TrimSpace(standby); standby != "" {
			state.Standbys = append(state.Standbys, standby)
		}
	}
	if quorum, ok := integerValue(data.Quorum); ok {
		state.Quorum = int(quorum)
	}
	return state
}

func (snapshot *Snapshot) decodeStatus(entry Entry) Status {
	status := Status{Source: entry.Kind}
	if entry.Kind == KeyLeaderOptime {
		value, err := strconv.ParseInt(strings.TrimSpace(string(entry.Value)), 10, 64)
		if err != nil {
			snapshot.issue(entry.RelativePath, "value", "legacy leader optime is not an integer")
		} else {
			status.LastLSN = value
		}
		return status
	}
	var root any
	if err := decodeJSON(entry.Value, &root); err != nil {
		snapshot.issue(entry.RelativePath, "value", "status value is invalid")
		return status
	}
	if value, ok := integerValue(root); ok {
		status.LastLSN = value
		return status
	}
	data, ok := root.(map[string]any)
	if !ok {
		snapshot.issue(entry.RelativePath, "value", "status value is invalid")
		return status
	}
	if value, ok := integerValue(data["optime"]); ok {
		status.LastLSN = value
	}
	slots := data["slots"]
	if encoded, ok := slots.(string); ok {
		var decoded any
		if decodeJSON([]byte(encoded), &decoded) == nil {
			slots = decoded
		} else {
			slots = nil
		}
	}
	if values, ok := slots.(map[string]any); ok {
		status.Slots = make(map[string]int64, len(values))
		for name, raw := range values {
			if value, ok := integerValue(raw); ok {
				status.Slots[name] = value
			} else {
				status.Slots[name] = 0
			}
		}
	}
	retainSlots := data["retain_slots"]
	if encoded, ok := retainSlots.(string); ok {
		var decoded any
		if decodeJSON([]byte(encoded), &decoded) == nil {
			retainSlots = decoded
		} else {
			retainSlots = nil
		}
	}
	switch values := retainSlots.(type) {
	case []any:
		for _, raw := range values {
			if value, ok := raw.(string); ok {
				status.RetainSlots = append(status.RetainSlots, value)
			}
		}
	}
	return status
}

func decodeJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func parseLegacyConnectionString(value string) (string, string) {
	parsed, err := url.Parse(value)
	if err != nil {
		return value, ""
	}
	apiURL := parsed.Query().Get("application_name")
	parsed.RawQuery = ""
	return parsed.String(), apiURL
}

func integerValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		integer, err := typed.Int64()
		return integer, err == nil
	case float64:
		integer := int64(typed)
		return integer, float64(integer) == typed
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		integer, err := strconv.ParseInt(typed, 10, 64)
		return integer, err == nil
	default:
		return 0, false
	}
}

func sortMembers(members []Member) {
	sort.Slice(members, func(left, right int) bool { return members[left].Name < members[right].Name })
}

func DiscoverFromEntries(request DiscoveryRequest, revision int64, entries []Entry) []DiscoveredCluster {
	contextName := strings.TrimSpace(request.Context)
	if contextName == "" {
		contextName = model.DefaultContext
	}
	namespace := (model.Target{Namespace: request.Namespace}).Normalize().Namespace
	prefix := NamespacePrefix(namespace)
	type evidence struct {
		target model.Target
		keys   map[string]Entry
	}
	found := map[string]*evidence{}
	for _, entry := range entries {
		relative := strings.TrimPrefix(entry.Key, prefix)
		if relative == entry.Key || relative == "" {
			continue
		}
		parts := strings.Split(relative, "/")
		if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
			continue
		}
		target := model.Target{Context: contextName, Namespace: namespace, Scope: parts[0]}.Normalize()
		keyStart := 1
		if len(parts) >= 3 {
			if group, err := strconv.Atoi(parts[1]); err == nil && group >= 0 && ClassifyRelativePath(strings.Join(parts[2:], "/")) != KeyUnknown {
				target.Group = &group
				keyStart = 2
			}
		}
		kind := ClassifyRelativePath(strings.Join(parts[keyStart:], "/"))
		if kind == KeyUnknown {
			continue
		}
		id := target.ClusterID()
		item := found[id]
		if item == nil {
			item = &evidence{target: target, keys: map[string]Entry{}}
			found[id] = item
		}
		clusterEntry := entry.Clone()
		clusterEntry.RelativePath = strings.Join(parts[keyStart:], "/")
		clusterEntry.Kind = kind
		item.keys[entry.Key] = clusterEntry
	}
	output := make([]DiscoveredCluster, 0, len(found))
	for _, item := range found {
		keys := make([]string, 0, len(item.keys))
		clusterEntries := make([]Entry, 0, len(item.keys))
		for key, entry := range item.keys {
			keys = append(keys, key)
			clusterEntries = append(clusterEntries, entry)
		}
		sort.Strings(keys)
		SortEntries(clusterEntries)
		clusterPrefix, err := ClusterPrefix(item.target)
		if err != nil {
			// Targets were normalized from a validated namespace prefix and a
			// non-empty scope; retain the closed failure behavior if that contract
			// changes instead of fabricating a root.
			continue
		}
		snapshot := BuildSnapshot(item.target, clusterPrefix, revision, clusterEntries)
		output = append(output, DiscoveredCluster{
			Target: item.target, Revision: revision, EvidenceKeys: keys, Snapshot: &snapshot,
		})
	}
	sort.Slice(output, func(left, right int) bool {
		if output[left].Target.Scope != output[right].Target.Scope {
			return output[left].Target.Scope < output[right].Target.Scope
		}
		leftGroup, rightGroup := -1, -1
		if output[left].Target.Group != nil {
			leftGroup = *output[left].Target.Group
		}
		if output[right].Target.Group != nil {
			rightGroup = *output[right].Target.Group
		}
		return leftGroup < rightGroup
	})
	return output
}
