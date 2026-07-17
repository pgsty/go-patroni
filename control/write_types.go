package control

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/model"
)

type ReloadRequest struct {
	Target  model.Target `json:"target" yaml:"target"`
	Members []string     `json:"members,omitempty" yaml:"members,omitempty"`
	Role    Role         `json:"role,omitempty" yaml:"role,omitempty"`
	Citus   bool         `json:"citus" yaml:"citus"`
}

type RestartRequest struct {
	Target          model.Target `json:"target" yaml:"target"`
	Members         []string     `json:"members,omitempty" yaml:"members,omitempty"`
	Role            Role         `json:"role,omitempty" yaml:"role,omitempty"`
	Any             bool         `json:"any" yaml:"any"`
	ScheduledAt     *time.Time   `json:"scheduledAt,omitempty" yaml:"scheduledAt,omitempty"`
	PostgresVersion string       `json:"postgresVersion,omitempty" yaml:"postgresVersion,omitempty"`
	Pending         bool         `json:"pending" yaml:"pending"`
	Timeout         string       `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Force           bool         `json:"force" yaml:"force"`
	Citus           bool         `json:"citus" yaml:"citus"`
}

type ReinitializeRequest struct {
	Target     model.Target `json:"target" yaml:"target"`
	Members    []string     `json:"members,omitempty" yaml:"members,omitempty"`
	Force      bool         `json:"force" yaml:"force"`
	FromLeader bool         `json:"fromLeader" yaml:"fromLeader"`
	Wait       bool         `json:"wait" yaml:"wait"`
	Citus      bool         `json:"citus" yaml:"citus"`
}

type FailoverRequest struct {
	Target    model.Target `json:"target" yaml:"target"`
	Candidate string       `json:"candidate,omitempty" yaml:"candidate,omitempty"`
	Force     bool         `json:"force" yaml:"force"`
	Citus     bool         `json:"citus" yaml:"citus"`
}

func (request FailoverRequest) String() string {
	return fmt.Sprintf("control.FailoverRequest{target:%s,candidate:%q,force:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), strings.TrimSpace(request.Candidate), request.Force, request.Citus)
}

func (request FailoverRequest) GoString() string { return request.String() }

type SwitchoverRequest struct {
	Target      model.Target `json:"target" yaml:"target"`
	Leader      string       `json:"leader,omitempty" yaml:"leader,omitempty"`
	Candidate   string       `json:"candidate,omitempty" yaml:"candidate,omitempty"`
	ScheduledAt *time.Time   `json:"scheduledAt,omitempty" yaml:"scheduledAt,omitempty"`
	Force       bool         `json:"force" yaml:"force"`
	Citus       bool         `json:"citus" yaml:"citus"`
}

func (request SwitchoverRequest) String() string {
	schedule := ""
	if request.ScheduledAt != nil {
		schedule = request.ScheduledAt.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("control.SwitchoverRequest{target:%s,leader:%q,candidate:%q,scheduled:%q,force:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), strings.TrimSpace(request.Leader), strings.TrimSpace(request.Candidate), schedule, request.Force, request.Citus)
}

func (request SwitchoverRequest) GoString() string { return request.String() }

type FlushEvent string

const (
	FlushRestart    FlushEvent = "restart"
	FlushSwitchover FlushEvent = "switchover"
)

func (event FlushEvent) valid() bool {
	return event == FlushRestart || event == FlushSwitchover
}

type FlushRequest struct {
	Target  model.Target `json:"target" yaml:"target"`
	Event   FlushEvent   `json:"event" yaml:"event"`
	Members []string     `json:"members,omitempty" yaml:"members,omitempty"`
	Role    Role         `json:"role,omitempty" yaml:"role,omitempty"`
	Force   bool         `json:"force" yaml:"force"`
	Citus   bool         `json:"citus" yaml:"citus"`
}

func (request FlushRequest) String() string {
	return fmt.Sprintf("control.FlushRequest{target:%s,event:%q,role:%q,members:%q,force:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), request.Event, request.Role, normalizeMemberNames(request.Members), request.Force, request.Citus)
}

func (request FlushRequest) GoString() string { return request.String() }

type PauseRequest struct {
	Target model.Target `json:"target" yaml:"target"`
	Wait   bool         `json:"wait" yaml:"wait"`
	Citus  bool         `json:"citus" yaml:"citus"`
}

func (request PauseRequest) String() string {
	return fmt.Sprintf("control.PauseRequest{target:%s,wait:%t,citus:%t}", request.Target.Normalize().ClusterID(), request.Wait, request.Citus)
}

func (request PauseRequest) GoString() string { return request.String() }

func (request ReinitializeRequest) String() string {
	return fmt.Sprintf("control.ReinitializeRequest{target:%s,members:%q,force:%t,fromLeader:%t,wait:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), normalizeMemberNames(request.Members), request.Force, request.FromLeader, request.Wait, request.Citus)
}

func (request ReinitializeRequest) GoString() string { return request.String() }

func (request RestartRequest) String() string {
	schedule := ""
	if request.ScheduledAt != nil {
		schedule = request.ScheduledAt.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("control.RestartRequest{target:%s,role:%q,members:%q,any:%t,scheduled:%q,postgresVersion:%q,pending:%t,timeout:%q,force:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), request.Role, normalizeMemberNames(request.Members), request.Any,
		schedule, request.PostgresVersion, request.Pending, request.Timeout, request.Force, request.Citus)
}

func (request RestartRequest) GoString() string { return request.String() }

func (request ReloadRequest) String() string {
	members := normalizeMemberNames(request.Members)
	return fmt.Sprintf("control.ReloadRequest{target:%s,role:%q,members:%q,citus:%t}",
		request.Target.Normalize().ClusterID(), request.Role, members, request.Citus)
}

func (request ReloadRequest) GoString() string { return request.String() }

// MemberWriteResult is a normalized command result, not a Patroni wire DTO.
// Response bodies and member endpoints are deliberately excluded.
type MemberWriteResult struct {
	Target       model.Target `json:"target" yaml:"target"`
	Outcome      Outcome      `json:"outcome" yaml:"outcome"`
	SendState    SendState    `json:"sendState" yaml:"sendState"`
	Verification Verification `json:"verification" yaml:"verification"`
	HTTPStatus   int          `json:"httpStatus,omitempty" yaml:"httpStatus,omitempty"`
	Summary      string       `json:"summary" yaml:"summary"`
	Evidence     []Evidence   `json:"evidence" yaml:"evidence"`
	Error        *Error       `json:"error,omitempty" yaml:"error,omitempty"`
}

func (result MemberWriteResult) Validate() error {
	if err := result.Target.Validate(true); err != nil || result.Target.Member == "" {
		return errorsOrMemberTarget(err)
	}
	if !result.SendState.validOrEmpty() || result.SendState == "" {
		return fmt.Errorf("member write send state %q is invalid", result.SendState)
	}
	if result.Verification != Unverified && result.Verification != VerifiedSucceeded && result.Verification != VerifiedFailed {
		return fmt.Errorf("member write verification %q is invalid", result.Verification)
	}
	if result.Summary == "" || len(result.Evidence) == 0 {
		return fmt.Errorf("member write summary and evidence are required")
	}
	for _, evidence := range result.Evidence {
		if err := evidence.Validate(); err != nil {
			return err
		}
	}
	switch result.Outcome {
	case Succeeded:
		if result.Error != nil || result.Verification != VerifiedSucceeded {
			return fmt.Errorf("successful member write requires verified success without error")
		}
	case Failed:
		if result.Error == nil || result.Error.Category == CategoryUnknown || result.Verification != VerifiedFailed {
			return fmt.Errorf("failed member write requires a definite error")
		}
	case Unknown:
		if result.Error == nil || result.Error.Category != CategoryUnknown {
			return fmt.Errorf("unknown member write requires an UNKNOWN error")
		}
	default:
		return fmt.Errorf("member write outcome %q is invalid", result.Outcome)
	}
	return nil
}

func errorsOrMemberTarget(err error) error {
	if err != nil {
		return fmt.Errorf("member write target: %w", err)
	}
	return fmt.Errorf("member write target requires a member")
}

type BatchWriteData struct {
	Members []MemberWriteResult `json:"members" yaml:"members"`
}

// FlushData retains every REST attempt because scheduled-switchover flush
// probes members leader-first before its command-defined DCS delete fallback.
// Response bodies and endpoints remain transport-only data.
type FlushData struct {
	Event         FlushEvent          `json:"event" yaml:"event"`
	Results       []MemberWriteResult `json:"results,omitempty" yaml:"results,omitempty"`
	RESTSendState SendState           `json:"restSendState" yaml:"restSendState"`
	DCSSendState  SendState           `json:"dcsSendState,omitempty" yaml:"dcsSendState,omitempty"`
	Verification  Verification        `json:"verification" yaml:"verification"`
	DCSRevision   int64               `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	Noop          bool                `json:"noop" yaml:"noop"`
}

func (data FlushData) Validate() error {
	if !data.Event.valid() {
		return fmt.Errorf("flush event %q is invalid", data.Event)
	}
	if !data.RESTSendState.validOrEmpty() || data.RESTSendState == "" {
		return fmt.Errorf("flush REST send state %q is invalid", data.RESTSendState)
	}
	if !data.DCSSendState.validOrEmpty() {
		return fmt.Errorf("flush DCS send state %q is invalid", data.DCSSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("flush verification %q is invalid", data.Verification)
	}
	if data.DCSRevision < 0 {
		return fmt.Errorf("flush DCS revision must be non-negative")
	}
	if data.Noop && data.Verification != VerifiedSucceeded {
		return fmt.Errorf("flush no-op requires verified success")
	}
	for _, result := range data.Results {
		if err := result.Validate(); err != nil {
			return fmt.Errorf("flush REST result: %w", err)
		}
	}
	return nil
}

type PauseData struct {
	Paused         bool                `json:"paused" yaml:"paused"`
	Wait           bool                `json:"wait" yaml:"wait"`
	Results        []MemberWriteResult `json:"results,omitempty" yaml:"results,omitempty"`
	RESTSendState  SendState           `json:"restSendState" yaml:"restSendState"`
	Verification   Verification        `json:"verification" yaml:"verification"`
	DCSRevision    int64               `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	PendingMembers []string            `json:"pendingMembers,omitempty" yaml:"pendingMembers,omitempty"`
	Noop           bool                `json:"noop" yaml:"noop"`
}

func (data PauseData) Validate() error {
	if !data.RESTSendState.validOrEmpty() || data.RESTSendState == "" {
		return fmt.Errorf("pause REST send state %q is invalid", data.RESTSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("pause verification %q is invalid", data.Verification)
	}
	if data.DCSRevision < 0 {
		return fmt.Errorf("pause DCS revision must be non-negative")
	}
	if data.Noop && data.Verification != VerifiedSucceeded {
		return fmt.Errorf("pause no-op requires verified success")
	}
	for _, result := range data.Results {
		if err := result.Validate(); err != nil {
			return fmt.Errorf("pause REST result: %w", err)
		}
	}
	if !sort.StringsAreSorted(data.PendingMembers) {
		return fmt.Errorf("pause pending members must be deterministic")
	}
	return nil
}

// ClusterWriteData is the normalized outcome projection for cluster-level
// availability writes. Patroni response bodies and raw DCS values stay in
// their transport layers and are not exposed here.
type ClusterWriteData struct {
	Leader         string       `json:"leader,omitempty" yaml:"leader,omitempty"`
	Candidate      string       `json:"candidate,omitempty" yaml:"candidate,omitempty"`
	ScheduledAt    string       `json:"scheduledAt,omitempty" yaml:"scheduledAt,omitempty"`
	RESTSendState  SendState    `json:"restSendState" yaml:"restSendState"`
	DCSSendState   SendState    `json:"dcsSendState,omitempty" yaml:"dcsSendState,omitempty"`
	Verification   Verification `json:"verification" yaml:"verification"`
	HTTPStatus     int          `json:"httpStatus,omitempty" yaml:"httpStatus,omitempty"`
	DCSRevision    int64        `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	LegacyEndpoint bool         `json:"legacyEndpoint" yaml:"legacyEndpoint"`
}

func (data ClusterWriteData) Validate() error {
	if !data.RESTSendState.validOrEmpty() || data.RESTSendState == "" {
		return fmt.Errorf("cluster write REST send state %q is invalid", data.RESTSendState)
	}
	if !data.DCSSendState.validOrEmpty() {
		return fmt.Errorf("cluster write DCS send state %q is invalid", data.DCSSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("cluster write verification %q is invalid", data.Verification)
	}
	if data.HTTPStatus < 0 || data.DCSRevision < 0 {
		return fmt.Errorf("cluster write status and revision must be non-negative")
	}
	return nil
}

func normalizeMemberNames(input []string) []string {
	set := make(map[string]struct{}, len(input))
	for _, name := range input {
		if name = strings.TrimSpace(name); name != "" {
			set[name] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}
