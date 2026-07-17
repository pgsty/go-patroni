package dcs

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/model"
)

type KeyKind string

const (
	KeyUnknown      KeyKind = "unknown"
	KeyInitialize   KeyKind = "initialize"
	KeyConfig       KeyKind = "config"
	KeyMember       KeyKind = "member"
	KeyLeader       KeyKind = "leader"
	KeyFailover     KeyKind = "failover"
	KeyHistory      KeyKind = "history"
	KeyStatus       KeyKind = "status"
	KeyLeaderOptime KeyKind = "leader-optime"
	KeySync         KeyKind = "sync"
	KeyFailsafe     KeyKind = "failsafe"
)

type Entry struct {
	Key            string  `json:"key"`
	RelativePath   string  `json:"relative_path"`
	Kind           KeyKind `json:"kind"`
	CreateRevision int64   `json:"create_revision"`
	ModRevision    int64   `json:"mod_revision"`
	Version        int64   `json:"version"`
	Lease          int64   `json:"lease"`
	Value          []byte  `json:"-"`
}

func (entry Entry) Clone() Entry {
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}

func (entry Entry) String() string {
	return fmt.Sprintf("dcs.Entry{key:%q,relative:%q,kind:%q,modRevision:%d,value:[REDACTED]}",
		entry.Key, entry.RelativePath, entry.Kind, entry.ModRevision)
}

func (entry Entry) GoString() string { return entry.String() }

type DecodeIssue struct {
	RelativePath string `json:"relative_path"`
	Field        string `json:"field"`
	Reason       string `json:"reason"`
}

type MemberData struct {
	APIURL               string                 `json:"api_url,omitempty"`
	ConnURL              string                 `json:"-"`
	State                string                 `json:"state,omitempty"`
	Role                 string                 `json:"role,omitempty"`
	ReplicationState     string                 `json:"replication_state,omitempty"`
	Timeline             *int                   `json:"timeline,omitempty"`
	PendingRestart       *bool                  `json:"pending_restart,omitempty"`
	PendingRestartReason map[string]any         `json:"pending_restart_reason,omitempty"`
	ScheduledRestart     json.RawMessage        `json:"scheduled_restart,omitempty"`
	Tags                 map[string]any         `json:"tags,omitempty"`
	PatroniVersion       string                 `json:"version,omitempty"`
	Pause                *bool                  `json:"pause,omitempty"`
	XLogLocation         *int64                 `json:"xlog_location,omitempty"`
	ReceiveLSN           *int64                 `json:"receive_lsn,omitempty"`
	ReplayLSN            *int64                 `json:"replay_lsn,omitempty"`
	Slots                map[string]json.Number `json:"slots,omitempty"`
}

type Member struct {
	Name        string     `json:"name"`
	ModRevision int64      `json:"mod_revision"`
	Lease       int64      `json:"lease"`
	Data        MemberData `json:"data"`
}

func (member Member) String() string {
	return fmt.Sprintf("dcs.Member{name:%q,modRevision:%d,lease:%d,apiURL:%q,connURL:[REDACTED]}",
		member.Name, member.ModRevision, member.Lease, member.Data.APIURL)
}

func (member Member) GoString() string { return member.String() }

type Leader struct {
	Name        string `json:"name"`
	ModRevision int64  `json:"mod_revision"`
	Lease       int64  `json:"lease"`
}

type Failover struct {
	ModRevision int64  `json:"mod_revision"`
	Leader      string `json:"leader,omitempty"`
	Candidate   string `json:"candidate,omitempty"`
	ScheduledAt string `json:"scheduled_at,omitempty"`
}

type SyncState struct {
	ModRevision int64    `json:"mod_revision"`
	Leader      string   `json:"leader,omitempty"`
	Standbys    []string `json:"standbys,omitempty"`
	Quorum      int      `json:"quorum"`
}

type Status struct {
	Source      KeyKind          `json:"source"`
	LastLSN     int64            `json:"last_lsn"`
	Slots       map[string]int64 `json:"slots,omitempty"`
	RetainSlots []string         `json:"retain_slots,omitempty"`
}

type ClusterState struct {
	Initialize string            `json:"initialize,omitempty"`
	Config     map[string]any    `json:"-"`
	Leader     *Leader           `json:"leader,omitempty"`
	Members    []Member          `json:"members"`
	Failover   *Failover         `json:"failover,omitempty"`
	History    []json.RawMessage `json:"history,omitempty"`
	Status     Status            `json:"status"`
	Sync       SyncState         `json:"sync"`
	Failsafe   map[string]string `json:"failsafe,omitempty"`
}

type Snapshot struct {
	Target   model.Target  `json:"target"`
	Prefix   string        `json:"prefix"`
	Revision int64         `json:"revision"`
	Entries  []Entry       `json:"entries"`
	Cluster  ClusterState  `json:"cluster"`
	Issues   []DecodeIssue `json:"issues,omitempty"`
}

func (snapshot Snapshot) String() string {
	return fmt.Sprintf("dcs.Snapshot{target:%s,prefix:%q,revision:%d,entries:%d,issues:%d}",
		snapshot.Target.ClusterID(), snapshot.Prefix, snapshot.Revision, len(snapshot.Entries), len(snapshot.Issues))
}

func (snapshot Snapshot) GoString() string { return snapshot.String() }

func (snapshot Snapshot) Entry(relativePath string) (Entry, bool) {
	for _, entry := range snapshot.Entries {
		if entry.RelativePath == relativePath {
			return entry.Clone(), true
		}
	}
	return Entry{}, false
}

type DiscoveryRequest struct {
	Context   string
	Namespace string
}

type DiscoveredCluster struct {
	Target       model.Target `json:"target"`
	Revision     int64        `json:"revision"`
	EvidenceKeys []string     `json:"evidence_keys"`
	// Snapshot is assembled from the same bounded namespace read as discovery.
	// It is excluded from serialization because it contains low-level DCS raw
	// values; control projects it into normalized, secret-safe models. A nil
	// value is accepted for older/custom Discoverer implementations, which may
	// require control to perform a compatibility snapshot read per target.
	Snapshot *Snapshot `json:"-" yaml:"-"`
}

type WriteResult struct {
	Applied  bool   `json:"applied"`
	Revision int64  `json:"revision"`
	Previous *Entry `json:"previous,omitempty"`
	Current  *Entry `json:"current,omitempty"`
}

type RemoveResult struct {
	Deleted  int64 `json:"deleted"`
	Revision int64 `json:"revision"`
}

type WatchEventType string

const (
	WatchPut    WatchEventType = "PUT"
	WatchDelete WatchEventType = "DELETE"
	WatchResync WatchEventType = "RESYNC"
)

type WatchEvent struct {
	Type     WatchEventType `json:"type"`
	Revision int64          `json:"revision"`
	Entry    *Entry         `json:"entry,omitempty"`
	Snapshot *Snapshot      `json:"snapshot,omitempty"`
	At       time.Time      `json:"at"`
}

type WatchStream struct {
	Events <-chan WatchEvent
	Errors <-chan error
}

func ClassifyRelativePath(relative string) KeyKind {
	relative = strings.Trim(relative, "/")
	switch relative {
	case "initialize":
		return KeyInitialize
	case "config":
		return KeyConfig
	case "leader":
		return KeyLeader
	case "failover":
		return KeyFailover
	case "history":
		return KeyHistory
	case "status":
		return KeyStatus
	case "optime/leader":
		return KeyLeaderOptime
	case "sync":
		return KeySync
	case "failsafe":
		return KeyFailsafe
	}
	if strings.HasPrefix(relative, "members/") && strings.Count(relative, "/") == 1 && strings.TrimPrefix(relative, "members/") != "" {
		return KeyMember
	}
	return KeyUnknown
}

func SortEntries(entries []Entry) {
	sort.Slice(entries, func(left, right int) bool { return entries[left].Key < entries[right].Key })
}
