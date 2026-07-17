package patroni

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type Response[T any] struct {
	StatusCode int         `json:"status_code"`
	Header     http.Header `json:"header"`
	Data       T           `json:"data"`
	Raw        []byte      `json:"raw"`
}

type Empty struct{}

type PatroniIdentity struct {
	Version string `json:"version"`
	Scope   string `json:"scope"`
	Name    string `json:"name"`
}

type XLogStatus struct {
	Location          *int64 `json:"location,omitempty"`
	ReceivedLocation  *int64 `json:"received_location,omitempty"`
	ReplayedLocation  *int64 `json:"replayed_location,omitempty"`
	ReplayedTimestamp any    `json:"replayed_timestamp,omitempty"`
	Paused            *bool  `json:"paused,omitempty"`
}

type ReplicationStatus struct {
	Username        string `json:"usename,omitempty"`
	ApplicationName string `json:"application_name,omitempty"`
	ClientAddress   string `json:"client_addr,omitempty"`
	State           string `json:"state,omitempty"`
	SyncState       string `json:"sync_state,omitempty"`
	SyncPriority    *int   `json:"sync_priority,omitempty"`
}

type PendingRestartChange struct {
	OldValue any `json:"old_value,omitempty"`
	NewValue any `json:"new_value,omitempty"`
}

type ScheduledRestart struct {
	Schedule        string `json:"schedule,omitempty"`
	Role            string `json:"role,omitempty"`
	PostgresVersion string `json:"postgres_version,omitempty"`
	// Timeout is either a JSON number of seconds or a Patroni duration string,
	// such as "30" or "30s". Patroni accepts and may publish both forms.
	Timeout        any   `json:"timeout,omitempty"`
	RestartPending *bool `json:"restart_pending,omitempty"`
}

// Status is the wire DTO returned by health aliases and GET /patroni. Unknown
// fields are tolerated and remain available in Response.Raw.
type Status struct {
	State                    string                          `json:"state,omitempty"`
	PostmasterStartTime      string                          `json:"postmaster_start_time,omitempty"`
	Role                     string                          `json:"role,omitempty"`
	ServerVersion            *int                            `json:"server_version,omitempty"`
	XLog                     XLogStatus                      `json:"xlog,omitempty"`
	Timeline                 *int                            `json:"timeline,omitempty"`
	Replication              []ReplicationStatus             `json:"replication,omitempty"`
	ReplicationState         string                          `json:"replication_state,omitempty"`
	ClusterUnlocked          *bool                           `json:"cluster_unlocked,omitempty"`
	FailsafeModeIsActive     *bool                           `json:"failsafe_mode_is_active,omitempty"`
	Pause                    *bool                           `json:"pause,omitempty"`
	DCSLastSeen              *int64                          `json:"dcs_last_seen,omitempty"`
	Tags                     map[string]any                  `json:"tags,omitempty"`
	DatabaseSystemIdentifier string                          `json:"database_system_identifier,omitempty"`
	PendingRestart           *bool                           `json:"pending_restart,omitempty"`
	PendingRestartReason     map[string]PendingRestartChange `json:"pending_restart_reason,omitempty"`
	ScheduledRestart         *ScheduledRestart               `json:"scheduled_restart,omitempty"`
	WatchdogFailed           *bool                           `json:"watchdog_failed,omitempty"`
	LoggerQueueSize          *int                            `json:"logger_queue_size,omitempty"`
	LoggerRecordsLost        *int                            `json:"logger_records_lost,omitempty"`
	SyncStandby              *bool                           `json:"sync_standby,omitempty"`
	QuorumStandby            *bool                           `json:"quorum_standby,omitempty"`
	Patroni                  PatroniIdentity                 `json:"patroni"`
}

type ClusterMember struct {
	Name                 string                          `json:"name"`
	Role                 string                          `json:"role"`
	State                string                          `json:"state"`
	APIURL               string                          `json:"api_url,omitempty"`
	Host                 string                          `json:"host,omitempty"`
	Port                 *int                            `json:"port,omitempty"`
	Timeline             *int                            `json:"timeline,omitempty"`
	PendingRestart       *bool                           `json:"pending_restart,omitempty"`
	PendingRestartReason map[string]PendingRestartChange `json:"pending_restart_reason,omitempty"`
	ScheduledRestart     *ScheduledRestart               `json:"scheduled_restart,omitempty"`
	Tags                 map[string]any                  `json:"tags,omitempty"`
	LSN                  json.RawMessage                 `json:"lsn,omitempty"`
	Lag                  json.RawMessage                 `json:"lag,omitempty"`
	ReceiveLSN           json.RawMessage                 `json:"receive_lsn,omitempty"`
	ReceiveLag           json.RawMessage                 `json:"receive_lag,omitempty"`
	ReplayLSN            json.RawMessage                 `json:"replay_lsn,omitempty"`
	ReplayLag            json.RawMessage                 `json:"replay_lag,omitempty"`
}

type ScheduledSwitchover struct {
	At   string `json:"at,omitempty"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type Cluster struct {
	Members             []ClusterMember      `json:"members"`
	Scope               string               `json:"scope,omitempty"`
	Pause               *bool                `json:"pause,omitempty"`
	ScheduledSwitchover *ScheduledSwitchover `json:"scheduled_switchover,omitempty"`
}

type HistoryEntry struct {
	Timeline  int64
	LSN       int64
	Reason    string
	Timestamp string
	Member    string
	Extra     []json.RawMessage
}

func (entry *HistoryEntry) UnmarshalJSON(data []byte) error {
	var fields []json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("history entry must be an array: %w", err)
	}
	if len(fields) < 3 {
		return fmt.Errorf("history entry requires at least timeline, lsn, and reason")
	}
	if err := json.Unmarshal(fields[0], &entry.Timeline); err != nil {
		return fmt.Errorf("decode history timeline: %w", err)
	}
	if err := json.Unmarshal(fields[1], &entry.LSN); err != nil {
		return fmt.Errorf("decode history lsn: %w", err)
	}
	if err := json.Unmarshal(fields[2], &entry.Reason); err != nil {
		return fmt.Errorf("decode history reason: %w", err)
	}
	if len(fields) > 3 {
		if err := json.Unmarshal(fields[3], &entry.Timestamp); err != nil {
			return fmt.Errorf("decode history timestamp: %w", err)
		}
	}
	if len(fields) > 4 {
		if err := json.Unmarshal(fields[4], &entry.Member); err != nil {
			return fmt.Errorf("decode history member: %w", err)
		}
	}
	if len(fields) > 5 {
		entry.Extra = append([]json.RawMessage(nil), fields[5:]...)
	}
	return nil
}

func (entry HistoryEntry) MarshalJSON() ([]byte, error) {
	fields := []any{entry.Timeline, entry.LSN, entry.Reason}
	if entry.Timestamp != "" || entry.Member != "" || len(entry.Extra) > 0 {
		fields = append(fields, entry.Timestamp)
	}
	if entry.Member != "" || len(entry.Extra) > 0 {
		fields = append(fields, entry.Member)
	}
	for _, extra := range entry.Extra {
		fields = append(fields, extra)
	}
	return json.Marshal(fields)
}

type History []HistoryEntry

type DynamicConfig map[string]any

type FailsafeTopology map[string]string

type RestartRequest struct {
	Schedule        string `json:"schedule,omitempty"`
	Role            string `json:"role,omitempty"`
	PostgresVersion string `json:"postgres_version,omitempty"`
	// Timeout accepts either a JSON number of seconds or a Patroni duration
	// string. Callers should use a numeric Go value or a string such as "30s".
	Timeout        any   `json:"timeout,omitempty"`
	RestartPending *bool `json:"restart_pending,omitempty"`
}

type ReinitializeRequest struct {
	Force      bool `json:"force"`
	FromLeader bool `json:"from_leader"`
}

type FailoverRequest struct {
	Leader      string `json:"leader,omitempty"`
	Candidate   string `json:"candidate,omitempty"`
	Member      string `json:"member,omitempty"`
	ScheduledAt string `json:"scheduled_at,omitempty"`
}

type FailsafePeerRequest struct {
	Name    string           `json:"name"`
	ConnURL string           `json:"conn_url"`
	APIURL  string           `json:"api_url"`
	Slots   map[string]int64 `json:"slots,omitempty"`
}

type MPPEvent struct {
	Type     string  `json:"type"`
	Group    int     `json:"group"`
	Leader   string  `json:"leader"`
	Timeout  float64 `json:"timeout"`
	Cooldown float64 `json:"cooldown"`
}

func decodeJSON[T any](raw []byte, output *T) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return fmt.Errorf("multiple JSON values in response")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}
