package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

const restartVerificationInterval = 250 * time.Millisecond

func (service *Service) PrepareRestart(ctx context.Context, request RestartRequest) Result[Plan] {
	operationID := service.operationID()
	target, requestedMembers, err := normalizeRestartRequest(request)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "restart", target, PathREST, CategoryUsage, false, "restart requires a context", nil)
	}
	if err != nil {
		return failedRead[Plan](service, operationID, "restart", target, PathREST, CategoryUsage, false, "restart request is invalid", err)
	}
	snapshots, evidence, failure := service.operationSnapshots(ctx, "restart", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[Plan](service, operationID, "restart", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	selected := make([]resolvedPlannedMember, 0)
	for _, snapshot := range snapshots {
		if request.ScheduledAt != nil && boolConfig(snapshot.Cluster.Config["pause"]) {
			pausedEvidence := append(evidence, snapshotEvidence(service, snapshot, "paused cluster state observed"))
			return failedReadWithEvidence[Plan](service, operationID, "restart", target, PathREST, CategoryConflict, false, "cannot schedule restart while the cluster is paused", nil, pausedEvidence)
		}
		members := selectMembers(snapshot.Cluster, normalizedWriteRole(request.Role), requestedMembers, true)
		if request.Pending && request.ScheduledAt == nil {
			members = pendingRestartMembers(members)
		}
		for _, member := range members {
			memberTarget := snapshot.Target.Normalize()
			memberTarget.Member = member.Name
			selected = append(selected, resolvedPlannedMember{target: memberTarget, member: member, snapshot: snapshot})
		}
	}
	if request.Any && len(selected) > 0 {
		index, randomError := service.randomIndex(len(selected))
		if randomError != nil || index < 0 || index >= len(selected) {
			return failedReadWithEvidence[Plan](service, operationID, "restart", target, PathREST, CategoryInternal, false, "restart random member selection failed", randomError, evidence)
		}
		selected = []resolvedPlannedMember{selected[index]}
	}
	if len(selected) == 0 {
		return failedReadWithEvidence[Plan](service, operationID, "restart", target, PathREST, CategoryNotFound, false, "restart found no matching members", nil, evidence)
	}
	targets := make([]model.Target, 0, len(selected))
	for _, member := range selected {
		targets = append(targets, member.target)
	}
	preconditions := restartPreconditions(snapshots[0].Revision, request, requestedMembers, snapshotGroupIDs(snapshots))
	plan := Plan{
		OperationID: operationID, Operation: "restart", Target: target, Targets: targets,
		Path: PathREST, Risk: RiskAvailability, RetrySafety: UnsafeAfterSend,
		Summary:       fmt.Sprintf("restart %d selected member(s): %s", len(targets), strings.Join(memberNamesFromTargets(targets), ", ")),
		Preconditions: preconditions,
	}
	if err := plan.Validate(); err != nil {
		return failedReadWithEvidence[Plan](service, operationID, "restart", target, PathREST, CategoryInternal, false, "restart plan construction failed", err, evidence)
	}
	return Result[Plan]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathREST, Data: plan,
		Evidence: evidence,
	}
}

func (service *Service) ExecuteRestart(ctx context.Context, request RestartRequest, plan Plan) Result[BatchWriteData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	target, requestedMembers, requestError := normalizeRestartRequest(request)
	if !validContext(ctx) {
		return failedRead[BatchWriteData](service, operationID, "restart", target, PathREST, CategoryUsage, false, "restart requires a context", nil)
	}
	if requestError != nil {
		return failedRead[BatchWriteData](service, operationID, "restart", target, PathREST, CategoryUsage, false, "restart request is invalid", requestError)
	}
	if err := validateRestartPlan(plan, target, request, requestedMembers); err != nil {
		return failedRead[BatchWriteData](service, operationID, "restart", target, PathREST, CategoryUsage, false, "restart plan does not match the request", err)
	}
	if service.patroni == nil {
		return failedRead[BatchWriteData](service, operationID, "restart", target, PathREST, CategoryConfig, false, "restart requires a Patroni REST client", nil)
	}
	snapshots, baseEvidence, failure := service.operationSnapshots(ctx, "restart", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "restart", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, baseEvidence)
	}
	plannedGroups, _ := expectedPrecondition(plan, "citus.groups")
	if snapshotGroupIDs(snapshots) != plannedGroups {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "restart", target, PathREST, CategoryConflict, false,
			"restart was not sent because the Citus group inventory changed", nil, baseEvidence)
	}
	resolved, conflicts := resolvePlannedMembersAcrossSnapshots(service, "restart", snapshots, plan.Targets, normalizedWriteRole(request.Role))
	if request.Pending && request.ScheduledAt == nil {
		resolved, conflicts = requirePendingRestart(service, resolved, conflicts)
	}
	if len(conflicts) > 0 {
		data := abortBatchForTargetChange(service, "restart", plan.Targets, conflicts)
		return finalizeBatchWrite(service, operationID, "restart", target, PathREST, data, baseEvidence)
	}

	results := make([]MemberWriteResult, 0, len(resolved))
	for _, resolvedMember := range resolved {
		member := resolvedMember.member
		memberTarget := resolvedMember.target
		snapshot := resolvedMember.snapshot
		if err := ctx.Err(); err != nil {
			results = append(results, skippedWriteAfterCancellation(service, "restart", memberTarget, err))
			continue
		}
		baseURL, urlError := patroniBaseURL(member.Data.APIURL)
		if urlError != nil {
			results = append(results, invalidRESTAddressWrite(service, "restart", memberTarget, snapshot, urlError))
			continue
		}
		prefixEvidence := make([]Evidence, 0, 1)
		if request.ScheduledAt != nil && request.Force && decodeScheduledRestart(member.Data.ScheduledRestart) != nil {
			flushResponse, flushError := service.patroni.DeleteRestart(ctx, baseURL)
			flushResult, continueRestart := classifyForcedRestartFlush(service, memberTarget, flushResponse, flushError)
			if !continueRestart {
				results = append(results, flushResult)
				continue
			}
			prefixEvidence = append(prefixEvidence, flushResult.Evidence...)
		}
		payload := patroni.RestartRequest{
			PostgresVersion: request.PostgresVersion,
			Timeout:         request.Timeout,
		}
		if request.Pending {
			value := true
			payload.RestartPending = &value
		}
		if request.ScheduledAt != nil {
			payload.Schedule = formatPatroniTimestamp(*request.ScheduledAt)
		}
		response, callError := service.patroni.PostRestart(ctx, baseURL, payload)
		memberResult := classifyRestartResponse(service, memberTarget, response, callError)
		if len(prefixEvidence) > 0 {
			memberResult.Evidence = append(prefixEvidence, memberResult.Evidence...)
		}
		if request.ScheduledAt != nil && (response.StatusCode == 202 || memberResult.SendState == SendMaybeSent || memberResult.SendState == SendAccepted && response.StatusCode == 0) {
			verified, verificationEvidence := service.verifyScheduledRestart(ctx, memberTarget, member.Name, *request.ScheduledAt)
			memberResult.Evidence = append(memberResult.Evidence, verificationEvidence...)
			if verified {
				memberResult.Outcome = Succeeded
				memberResult.Verification = VerifiedSucceeded
				memberResult.Error = nil
				memberResult.Summary = "scheduled restart verified in DCS"
			} else if memberResult.Outcome != Failed {
				memberResult.Outcome = Unknown
				memberResult.Verification = Unverified
				memberResult.Summary = "scheduled restart was not verified in DCS"
				memberResult.Error = NewError(CategoryUnknown, "restart", memberTarget, false,
					"scheduled restart outcome is unknown; verify before retrying", callError, memberResult.Evidence...)
			}
		}
		results = append(results, memberResult)
	}
	return finalizeBatchWrite(service, operationID, "restart", target, PathREST, BatchWriteData{Members: results}, baseEvidence)
}

func normalizeRestartRequest(request RestartRequest) (model.Target, []string, error) {
	target := request.Target.Normalize()
	members := normalizeMemberNames(request.Members)
	if target.Member != "" {
		if len(members) > 0 && (len(members) != 1 || members[0] != target.Member) {
			return target, members, errors.New("target member and member list conflict")
		}
		members = []string{target.Member}
		target.Member = ""
	}
	if err := target.Validate(true); err != nil {
		return target, members, err
	}
	if !request.Role.validOrEmpty() {
		return target, members, errors.New("invalid role")
	}
	if request.ScheduledAt != nil && request.ScheduledAt.IsZero() {
		return target, members, errors.New("scheduled restart time is zero")
	}
	if request.PostgresVersion != "" {
		if err := validatePostgresVersion(request.PostgresVersion); err != nil {
			return target, members, err
		}
	}
	if request.Timeout != "" {
		if err := validateRestartTimeout(request.Timeout); err != nil {
			return target, members, err
		}
	}
	return target, members, nil
}

func validatePostgresVersion(value string) error {
	parts := strings.Split(value, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return errors.New("PostgreSQL version must use X.Y or X.Y.Z")
	}
	components := make([]int, len(parts))
	for index, part := range parts {
		component, err := strconv.Atoi(part)
		if err != nil || component < 0 {
			return errors.New("PostgreSQL version contains a non-numeric component")
		}
		components[index] = component
	}
	if len(components) == 2 && components[0] < 10 {
		return errors.New("pre-10 PostgreSQL versions require X.Y.Z")
	}
	return nil
}

func validateRestartTimeout(value string) error {
	trimmed := strings.TrimSpace(value)
	if seconds, err := strconv.ParseFloat(trimmed, 64); err == nil {
		if seconds > 0 {
			return nil
		}
		return errors.New("restart timeout must be positive")
	}
	compact := strings.ReplaceAll(trimmed, " ", "")
	compact = strings.ReplaceAll(compact, "min", "m")
	if strings.HasSuffix(compact, "d") {
		days, parseError := strconv.ParseFloat(strings.TrimSuffix(compact, "d"), 64)
		if parseError == nil && days > 0 {
			return nil
		}
	}
	duration, err := time.ParseDuration(compact)
	if err != nil || duration <= 0 {
		return errors.New("restart timeout must be a positive duration")
	}
	return nil
}

func pendingRestartMembers(input []dcs.Member) []dcs.Member {
	result := make([]dcs.Member, 0, len(input))
	for _, member := range input {
		if member.Data.PendingRestart != nil && *member.Data.PendingRestart {
			result = append(result, member)
		}
	}
	return result
}

func restartPreconditions(revision int64, request RestartRequest, members []string, groups string) []Precondition {
	schedule := ""
	if request.ScheduledAt != nil {
		schedule = request.ScheduledAt.Format(time.RFC3339Nano)
	}
	return []Precondition{
		{Field: "dcs.revision", Expected: strconv.FormatInt(revision, 10), Source: EvidenceDCS},
		{Field: "selector.role", Expected: string(normalizedWriteRole(request.Role)), Source: EvidenceControl},
		{Field: "selector.members", Expected: strings.Join(members, ","), Source: EvidenceControl},
		{Field: "selector.citus", Expected: strconv.FormatBool(request.Citus), Source: EvidenceControl},
		{Field: "citus.groups", Expected: groups, Source: EvidenceDCS},
		{Field: "restart.any", Expected: strconv.FormatBool(request.Any), Source: EvidenceControl},
		{Field: "restart.schedule", Expected: schedule, Source: EvidenceControl},
		{Field: "restart.postgresVersion", Expected: request.PostgresVersion, Source: EvidenceControl},
		{Field: "restart.pending", Expected: strconv.FormatBool(request.Pending), Source: EvidenceControl},
		{Field: "restart.timeout", Expected: request.Timeout, Source: EvidenceControl},
		{Field: "restart.force", Expected: strconv.FormatBool(request.Force), Source: EvidenceControl},
	}
}

func validateRestartPlan(plan Plan, target model.Target, request RestartRequest, members []string) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "restart" || plan.Path != PathREST || plan.Risk != RiskAvailability || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from restart")
	}
	if plan.Target.Normalize().ClusterID() != target.Normalize().ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	expected := restartPreconditions(0, request, members, "")
	for _, precondition := range expected[1:] {
		value, exists := expectedPrecondition(plan, precondition.Field)
		if !exists || precondition.Field != "citus.groups" && value != precondition.Expected {
			return fmt.Errorf("plan %s differs from request", precondition.Field)
		}
	}
	return nil
}

func requirePendingRestart(service *Service, resolved []resolvedPlannedMember, conflicts []MemberWriteResult) ([]resolvedPlannedMember, []MemberWriteResult) {
	remaining := make([]resolvedPlannedMember, 0, len(resolved))
	for _, resolvedMember := range resolved {
		member := resolvedMember.member
		if member.Data.PendingRestart != nil && *member.Data.PendingRestart {
			remaining = append(remaining, resolvedMember)
			continue
		}
		target := resolvedMember.target
		evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "confirmed member no longer has pending restart", Revision: strconv.FormatInt(resolvedMember.snapshot.Revision, 10), Path: resolvedMember.snapshot.Prefix, SendState: SendNotSent}
		errorValue := NewError(CategoryConflict, "restart", target, false, "restart was not sent because pending-restart state changed", nil, evidence)
		conflicts = append(conflicts, MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
	}
	return remaining, conflicts
}

func abortBatchForTargetChange(service *Service, operation string, targets []model.Target, conflicts []MemberWriteResult) BatchWriteData {
	conflictByMember := make(map[string]MemberWriteResult, len(conflicts))
	for _, result := range conflicts {
		conflictByMember[result.Target.Normalize().MemberID()] = result
	}
	members := make([]MemberWriteResult, 0, len(targets))
	for _, target := range targets {
		if result, ok := conflictByMember[target.Normalize().MemberID()]; ok {
			members = append(members, result)
			continue
		}
		evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: operation + " skipped because the confirmed target set changed", Path: string(PathREST), SendState: SendNotSent}
		errorValue := NewError(CategoryConflict, operation, target, false, operation+" was not sent because the confirmed target set changed", nil, evidence)
		members = append(members, MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
	}
	return BatchWriteData{Members: members}
}

func skippedWriteAfterCancellation(service *Service, operation string, target model.Target, cause error) MemberWriteResult {
	evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: operation + " skipped after cancellation", Path: "/" + operation, SendState: SendNotSent}
	errorValue := NewError(CategoryFailed, operation, target, false, operation+" was not sent because the operation was canceled", cause, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func invalidRESTAddressWrite(service *Service, operation string, target model.Target, snapshot dcs.Snapshot, cause error) MemberWriteResult {
	evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "member has no usable Patroni REST address", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent}
	errorValue := NewError(CategoryConfig, operation, target, false, operation+" was not sent because the member REST address is invalid", cause, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func classifyForcedRestartFlush(service *Service, target model.Target, response patroni.Response[string], callError error) (MemberWriteResult, bool) {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Summary: "scheduled restart replacement flush completed", Path: "/restart", SendState: send}
	if response.StatusCode != 0 {
		outcome := Succeeded
		verification := VerifiedSucceeded
		var errorValue *Error
		if response.StatusCode >= 400 {
			outcome = Failed
			verification = VerifiedFailed
			category, retryable := classifyHTTPStatus(response.StatusCode)
			errorValue = NewError(category, "restart", target, retryable, "Patroni rejected scheduled restart flush", callError, evidence)
		}
		return MemberWriteResult{Target: target, Outcome: outcome, SendState: send, Verification: verification, HTTPStatus: response.StatusCode, Summary: evidence.Summary, Evidence: []Evidence{evidence}, Error: errorValue}, true
	}
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "restart", target, false, "scheduled restart flush may have been sent; verify before retrying", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}, false
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, "restart", target, retryable, "scheduled restart flush was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}, false
}

func classifyRestartResponse(service *Service, target model.Target, response patroni.Response[string], callError error) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/restart", SendState: send}
	if response.StatusCode == 200 || response.StatusCode == 202 {
		evidence.Summary = "Patroni accepted restart request"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded, HTTPStatus: response.StatusCode, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if response.StatusCode != 0 {
		evidence.Summary = "Patroni rejected restart request"
		category, retryable := classifyHTTPStatus(response.StatusCode)
		errorValue := NewError(category, "restart", target, retryable, "Patroni rejected restart request", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed, HTTPStatus: response.StatusCode, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = "restart transport ended without an HTTP response"
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "restart", target, false, "restart may have been sent; verify before retrying", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, "restart", target, retryable, "restart was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) verifyScheduledRestart(ctx context.Context, target model.Target, memberName string, scheduledAt time.Time) (bool, []Evidence) {
	target.Member = ""
	evidence := make([]Evidence, 0, service.verificationAttempts)
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "scheduled restart verification canceled", Path: string(PathDCS)})
				return false, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "scheduled restart verification snapshot failed", Path: string(PathDCS)})
			continue
		}
		matched := false
		for _, member := range snapshot.Cluster.Members {
			if member.Name == memberName && scheduledRestartMatches(member.Data.ScheduledRestart, scheduledAt) {
				matched = true
				break
			}
		}
		summary := "scheduled restart not yet observed in DCS"
		if matched {
			summary = "scheduled restart observed in DCS"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary, Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if matched {
			return true, evidence
		}
	}
	return false, evidence
}

func scheduledRestartMatches(raw json.RawMessage, expected time.Time) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}
	var value struct {
		Schedule string `json:"schedule"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Schedule == "" {
		return false
	}
	observed, err := time.Parse(time.RFC3339Nano, value.Schedule)
	if err != nil {
		return value.Schedule == expected.Format(time.RFC3339Nano) || value.Schedule == expected.Format(time.RFC3339)
	}
	return observed.Equal(expected)
}
