package control

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

type flushIntent struct {
	target  model.Target
	event   FlushEvent
	members []string
	role    Role
	force   bool
	citus   bool
}

func (service *Service) PrepareFlush(ctx context.Context, request FlushRequest) Result[Plan] {
	operationID := service.operationID()
	intent, err := normalizeFlushRequest(request)
	path := flushPath(intent.event)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "flush", intent.target, path, CategoryUsage, false, "flush requires a context", nil)
	}
	if err != nil {
		return failedRead[Plan](service, operationID, "flush", intent.target, path, CategoryUsage, false, "flush request is invalid", err)
	}
	snapshots, evidence, failure := service.flushOperationSnapshots(ctx, intent)
	if failure != nil {
		return failedReadWithEvidence[Plan](service, operationID, "flush", intent.target, path, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	plan, err := buildFlushPlan(operationID, intent, snapshots)
	if err != nil {
		return failedReadWithEvidence[Plan](service, operationID, "flush", intent.target, path, CategoryNotFound, false, "flush target selection failed", err, evidence)
	}
	if err := plan.Validate(); err != nil {
		return failedReadWithEvidence[Plan](service, operationID, "flush", intent.target, path, CategoryInternal, false, "flush plan construction failed", err, evidence)
	}
	return Result[Plan]{
		OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: path, Data: plan,
		Evidence: evidence,
	}
}

func (service *Service) ExecuteFlush(ctx context.Context, request FlushRequest, plan Plan) Result[FlushData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	intent, requestError := normalizeFlushRequest(request)
	path := flushPath(intent.event)
	if !validContext(ctx) {
		return failedRead[FlushData](service, operationID, "flush", intent.target, path, CategoryUsage, false, "flush requires a context", nil)
	}
	if requestError != nil {
		return failedRead[FlushData](service, operationID, "flush", intent.target, path, CategoryUsage, false, "flush request is invalid", requestError)
	}
	if err := validateFlushPlan(plan, intent); err != nil {
		return failedRead[FlushData](service, operationID, "flush", intent.target, path, CategoryUsage, false, "flush plan does not match the request", err)
	}
	snapshots, evidence, failure := service.flushOperationSnapshots(ctx, intent)
	if failure != nil {
		return failedReadWithEvidence[FlushData](service, operationID, "flush", intent.target, path, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	plannedGroups, _ := expectedPrecondition(plan, "citus.groups")
	if snapshotGroupIDs(snapshots) != plannedGroups {
		return failedReadWithEvidence[FlushData](service, operationID, "flush", intent.target, path, CategoryConflict, false,
			"flush was not sent because the Citus group inventory changed", nil, evidence)
	}
	if intent.event == FlushRestart {
		return service.executeRestartFlush(ctx, operationID, intent, plan, snapshots, evidence)
	}
	return service.executeSwitchoverFlush(ctx, operationID, intent, plan, snapshots[0], evidence)
}

func (service *Service) flushOperationSnapshots(ctx context.Context, intent flushIntent) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	if intent.event != FlushSwitchover || !intent.citus || intent.target.Group != nil {
		return service.operationSnapshots(ctx, "flush", intent.target, intent.citus, false)
	}
	return service.citusCoordinatorSnapshot(ctx, "flush", intent.target, false)
}

func normalizeFlushRequest(request FlushRequest) (flushIntent, error) {
	intent := flushIntent{
		target: request.Target.Normalize(), event: request.Event, members: normalizeMemberNames(request.Members),
		role: normalizedWriteRole(request.Role), force: request.Force, citus: request.Citus,
	}
	if intent.target.Member != "" {
		if len(intent.members) > 0 && (len(intent.members) != 1 || intent.members[0] != intent.target.Member) {
			return intent, errors.New("target member and member list conflict")
		}
		intent.members = []string{intent.target.Member}
		intent.target.Member = ""
	}
	if err := intent.target.Validate(true); err != nil {
		return intent, err
	}
	if !intent.event.valid() {
		return intent, errors.New("flush event must be restart or switchover")
	}
	if !request.Role.validOrEmpty() {
		return intent, errors.New("invalid role")
	}
	return intent, nil
}

func flushPath(event FlushEvent) Path {
	if event == FlushSwitchover {
		return PathRESTToDCS
	}
	return PathREST
}

func buildFlushPlan(operationID string, intent flushIntent, snapshots []dcs.Snapshot) (Plan, error) {
	if len(snapshots) == 0 {
		return Plan{}, errors.New("flush requires at least one cluster snapshot")
	}
	preconditions := flushRequestPreconditions(snapshots[0].Revision, intent, snapshotGroupIDs(snapshots))
	plan := Plan{
		OperationID: operationID, Operation: "flush", Target: intent.target, Path: flushPath(intent.event),
		Risk: RiskAdminWrite, RetrySafety: UnsafeAfterSend,
	}
	if intent.event == FlushRestart {
		selected := make([]resolvedPlannedMember, 0)
		for _, snapshot := range snapshots {
			for _, member := range selectMembers(snapshot.Cluster, intent.role, intent.members, true) {
				target := snapshot.Target.Normalize()
				target.Member = member.Name
				selected = append(selected, resolvedPlannedMember{target: target, member: member, snapshot: snapshot})
			}
		}
		if len(selected) == 0 {
			return Plan{}, errors.New("flush found no matching members")
		}
		plan.Targets = make([]model.Target, 0, len(selected))
		for _, resolved := range selected {
			plan.Targets = append(plan.Targets, resolved.target)
		}
		plan.Summary = fmt.Sprintf("flush scheduled restart for %d selected member(s): %s", len(selected), strings.Join(memberNamesFromTargets(plan.Targets), ", "))
		preconditions = append(preconditions, Precondition{Field: "rest.members", Expected: strings.Join(memberNamesFromTargets(plan.Targets), ","), Source: EvidenceDCS})
		preconditions = append(preconditions, Precondition{Field: "rest.targets", Expected: strings.Join(memberIDsFromTargets(plan.Targets), ","), Source: EvidenceDCS})
		for _, resolved := range selected {
			preconditions = append(preconditions, Precondition{
				Field: scheduledRestartPreconditionField(resolved.target), Expected: strconv.FormatBool(hasScheduledRestart(resolved.member)), Source: EvidenceDCS,
			})
		}
	} else {
		snapshot := snapshots[0]
		members := leaderFirstRESTMembers(snapshot.Cluster)
		failover := snapshot.Cluster.Failover
		if failover != nil && failover.ScheduledAt != "" {
			plan.Targets = memberTargets(snapshot.Target, members)
			plan.Summary = "flush scheduled switchover at " + failover.ScheduledAt
			preconditions = append(preconditions, failoverFlushPreconditions(failover, plan.Targets)...)
		} else {
			plan.Summary = "flush scheduled switchover (no pending event observed)"
			preconditions = append(preconditions, emptyFailoverFlushPreconditions()...)
		}
	}
	plan.Preconditions = preconditions
	return plan, nil
}

func flushRequestPreconditions(revision int64, intent flushIntent, groups string) []Precondition {
	return []Precondition{
		{Field: "dcs.revision", Expected: strconv.FormatInt(revision, 10), Source: EvidenceDCS},
		{Field: "flush.event", Expected: string(intent.event), Source: EvidenceControl},
		{Field: "selector.role", Expected: string(intent.role), Source: EvidenceControl},
		{Field: "selector.members", Expected: strings.Join(intent.members, ","), Source: EvidenceControl},
		{Field: "request.force", Expected: strconv.FormatBool(intent.force), Source: EvidenceControl},
		{Field: "request.citus", Expected: strconv.FormatBool(intent.citus), Source: EvidenceControl},
		{Field: "citus.groups", Expected: groups, Source: EvidenceDCS},
	}
}

func failoverFlushPreconditions(failover *dcs.Failover, targets []model.Target) []Precondition {
	return []Precondition{
		{Field: "failover.modRevision", Expected: strconv.FormatInt(failover.ModRevision, 10), Source: EvidenceDCS},
		{Field: "failover.leader", Expected: failover.Leader, Source: EvidenceDCS},
		{Field: "failover.candidate", Expected: failover.Candidate, Source: EvidenceDCS},
		{Field: "failover.scheduledAt", Expected: failover.ScheduledAt, Source: EvidenceDCS},
		{Field: "rest.members", Expected: strings.Join(memberNamesFromTargets(targets), ","), Source: EvidenceDCS},
		{Field: "rest.targets", Expected: strings.Join(memberIDsFromTargets(targets), ","), Source: EvidenceDCS},
	}
}

func emptyFailoverFlushPreconditions() []Precondition {
	return []Precondition{
		{Field: "failover.modRevision", Expected: "0", Source: EvidenceDCS},
		{Field: "failover.leader", Expected: "", Source: EvidenceDCS},
		{Field: "failover.candidate", Expected: "", Source: EvidenceDCS},
		{Field: "failover.scheduledAt", Expected: "", Source: EvidenceDCS},
		{Field: "rest.members", Expected: "", Source: EvidenceDCS},
		{Field: "rest.targets", Expected: "", Source: EvidenceDCS},
	}
}

func validateFlushPlan(plan Plan, intent flushIntent) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "flush" || plan.Path != flushPath(intent.event) || plan.Risk != RiskAdminWrite || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from flush")
	}
	if plan.Target.Normalize().ClusterID() != intent.target.ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	expected := flushRequestPreconditions(0, intent, "")
	for _, precondition := range expected[1:] {
		value, ok := expectedPrecondition(plan, precondition.Field)
		if !ok || precondition.Field != "citus.groups" && value != precondition.Expected {
			return fmt.Errorf("plan %s differs from request", precondition.Field)
		}
	}
	plannedTargets, ok := expectedPrecondition(plan, "rest.members")
	if !ok || plannedTargets != strings.Join(memberNamesFromTargets(plan.Targets), ",") {
		return errors.New("plan REST target set is incomplete or changed")
	}
	plannedTargetIDs, ok := expectedPrecondition(plan, "rest.targets")
	if !ok || plannedTargetIDs != strings.Join(memberIDsFromTargets(plan.Targets), ",") {
		return errors.New("plan REST target identities are incomplete or changed")
	}
	if intent.event == FlushRestart && len(plan.Targets) == 0 {
		return errors.New("restart flush plan requires member targets")
	}
	return nil
}

func memberTargets(target model.Target, members []dcs.Member) []model.Target {
	targets := make([]model.Target, 0, len(members))
	for _, member := range members {
		memberTarget := target
		memberTarget.Member = member.Name
		targets = append(targets, memberTarget)
	}
	return targets
}

func memberIDsFromTargets(targets []model.Target) []string {
	identities := make([]string, 0, len(targets))
	for _, target := range targets {
		identities = append(identities, target.Normalize().MemberID())
	}
	return identities
}

func scheduledRestartPreconditionField(target model.Target) string {
	target = target.Normalize()
	if target.Group == nil {
		return "member." + target.Member + ".scheduledRestart"
	}
	return "member." + target.MemberID() + ".scheduledRestart"
}

func hasScheduledRestart(member dcs.Member) bool {
	return decodeScheduledRestart(member.Data.ScheduledRestart) != nil
}

func leaderFirstRESTMembers(cluster dcs.ClusterState) []dcs.Member {
	leader := ""
	if cluster.Leader != nil {
		leader = cluster.Leader.Name
	}
	result := make([]dcs.Member, 0, len(cluster.Members))
	if leader != "" {
		for _, member := range cluster.Members {
			if member.Name == leader && strings.TrimSpace(member.Data.APIURL) != "" {
				result = append(result, member)
				break
			}
		}
	}
	for _, member := range cluster.Members {
		if member.Name != leader && strings.TrimSpace(member.Data.APIURL) != "" {
			result = append(result, member)
		}
	}
	return result
}

func (service *Service) executeRestartFlush(
	ctx context.Context,
	operationID string,
	intent flushIntent,
	plan Plan,
	snapshots []dcs.Snapshot,
	evidence []Evidence,
) Result[FlushData] {
	resolved, conflicts := resolvePlannedMembersAcrossSnapshots(service, "flush", snapshots, plan.Targets, intent.role)
	if len(conflicts) > 0 {
		return finalizeFlushResults(service, operationID, intent, PathREST, abortBatchForTargetChange(service, "flush", plan.Targets, conflicts).Members, evidence)
	}
	stateConflicts := make([]MemberWriteResult, 0)
	for _, resolvedMember := range resolved {
		member := resolvedMember.member
		planned, ok := expectedPrecondition(plan, scheduledRestartPreconditionField(resolvedMember.target))
		if !ok {
			return flushFailure(service, operationID, intent, PathREST, FlushData{Event: intent.event, RESTSendState: SendNotSent, Verification: VerifiedFailed},
				CategoryUsage, "flush plan is missing a member event precondition", nil, evidence)
		}
		if planned == "false" && hasScheduledRestart(member) {
			target := resolvedMember.target
			itemEvidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "scheduled restart appeared after confirmation", Revision: strconv.FormatInt(resolvedMember.snapshot.Revision, 10), Path: resolvedMember.snapshot.Prefix, SendState: SendNotSent}
			errorValue := NewError(CategoryConflict, "flush", target, false, "new scheduled restart was not deleted because it was absent from the confirmed plan", nil, itemEvidence)
			stateConflicts = append(stateConflicts, MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{itemEvidence}, Error: errorValue})
		}
	}
	if len(stateConflicts) > 0 {
		return finalizeFlushResults(service, operationID, intent, PathREST, abortBatchForTargetChange(service, "flush", plan.Targets, stateConflicts).Members, evidence)
	}

	results := make([]MemberWriteResult, 0, len(resolved))
	for _, resolvedMember := range resolved {
		member := resolvedMember.member
		target := resolvedMember.target
		snapshot := resolvedMember.snapshot
		if !hasScheduledRestart(member) {
			results = append(results, flushMemberNoop(service, target, snapshot, "member has no scheduled restart"))
			continue
		}
		if err := ctx.Err(); err != nil {
			results = append(results, skippedWriteAfterCancellation(service, "flush", target, err))
			continue
		}
		if service.patroni == nil {
			results = append(results, flushMemberNotSent(service, target, CategoryConfig, false, "scheduled restart flush requires a Patroni REST client", nil))
			continue
		}
		baseURL, err := patroniBaseURL(member.Data.APIURL)
		if err != nil {
			results = append(results, invalidRESTAddressWrite(service, "flush", target, snapshot, err))
			continue
		}
		response, callError := service.patroni.DeleteRestart(ctx, baseURL)
		result := classifyRestartFlushResponse(service, target, response, callError)
		if result.Outcome == Unknown {
			verified, verificationEvidence := service.verifyRestartFlush(ctx, target, member.Name)
			result.Evidence = append(result.Evidence, verificationEvidence...)
			if verified {
				result.Outcome = Succeeded
				result.Verification = VerifiedSucceeded
				result.Summary = "scheduled restart absence verified in DCS"
				result.Error = nil
			}
		}
		results = append(results, result)
	}
	return finalizeFlushResults(service, operationID, intent, PathREST, results, evidence)
}

func classifyRestartFlushResponse(service *Service, target model.Target, response patroni.Response[string], callError error) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/restart", SendState: send}
	if response.StatusCode > 0 && response.StatusCode < 400 {
		evidence.Summary = "Patroni removed the scheduled restart"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded, HTTPStatus: response.StatusCode, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if response.StatusCode != 0 {
		evidence.Summary = "Patroni rejected scheduled restart flush"
		category, retryable := classifyHTTPStatus(response.StatusCode)
		errorValue := NewError(category, "flush", target, retryable, evidence.Summary, callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed, HTTPStatus: response.StatusCode, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = "scheduled restart flush transport ended without an HTTP response"
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "flush", target, false, "scheduled restart flush may have been sent; DCS verification is required", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, "flush", target, retryable, "scheduled restart flush was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) verifyRestartFlush(ctx context.Context, target model.Target, memberName string) (bool, []Evidence) {
	target.Member = ""
	evidence := make([]Evidence, 0, service.verificationAttempts)
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "scheduled restart flush verification canceled", Path: string(PathDCS)})
				return false, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "scheduled restart flush readback failed", Path: string(PathDCS)})
			continue
		}
		present := false
		for _, member := range snapshot.Cluster.Members {
			if member.Name == memberName {
				present = hasScheduledRestart(member)
				break
			}
		}
		summary := "scheduled restart remains present in DCS"
		if !present {
			summary = "scheduled restart is absent from DCS"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary, Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if !present {
			return true, evidence
		}
	}
	return false, evidence
}

func flushMemberNoop(service *Service, target model.Target, snapshot dcs.Snapshot, summary string) MemberWriteResult {
	evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary, Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent}
	return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: SendNotSent, Verification: VerifiedSucceeded, Summary: summary, Evidence: []Evidence{evidence}}
}

func flushMemberNotSent(service *Service, target model.Target, category Category, retryable bool, message string, cause error) MemberWriteResult {
	evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: message, Path: string(PathREST), SendState: SendNotSent}
	errorValue := NewError(category, "flush", target, retryable, message, cause, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) executeSwitchoverFlush(
	ctx context.Context,
	operationID string,
	intent flushIntent,
	plan Plan,
	snapshot dcs.Snapshot,
	evidence []Evidence,
) Result[FlushData] {
	plannedSchedule, _ := expectedPrecondition(plan, "failover.scheduledAt")
	current := snapshot.Cluster.Failover
	data := FlushData{Event: intent.event, RESTSendState: SendNotSent, Verification: Unverified}
	if plannedSchedule == "" {
		if current != nil && current.ScheduledAt != "" {
			data.Verification = VerifiedFailed
			return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict,
				"a scheduled switchover appeared after confirmation and was not deleted", nil, evidence)
		}
		data.Noop = true
		data.Verification = VerifiedSucceeded
		return flushSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	if current == nil || current.ScheduledAt == "" {
		data.Noop = true
		data.Verification = VerifiedSucceeded
		return flushSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	if err := revalidateSwitchoverFlushPlan(plan, current, leaderFirstRESTMembers(snapshot.Cluster), snapshot.Target); err != nil {
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict,
			"scheduled switchover changed after confirmation", err, evidence)
	}

	members := memberMap(snapshot.Cluster.Members)
	for _, target := range plan.Targets {
		if err := ctx.Err(); err != nil {
			return service.finishCanceledSwitchoverFlush(operationID, intent, data, err, evidence)
		}
		member := members[target.Member]
		var result MemberWriteResult
		if service.patroni == nil {
			result = flushMemberNotSent(service, target, CategoryConfig, false, "scheduled switchover flush REST client is unavailable", nil)
		} else if baseURL, err := patroniBaseURL(member.Data.APIURL); err != nil {
			result = invalidRESTAddressWrite(service, "flush", target, snapshot, err)
		} else {
			response, callError := service.patroni.DeleteSwitchover(ctx, baseURL)
			result = classifySwitchoverFlushResponse(service, target, response, callError)
		}
		data.Results = append(data.Results, result)
		data.RESTSendState = aggregateRESTSendState(data.Results)
		if result.HTTPStatus == 200 {
			data.Verification = VerifiedSucceeded
			return flushSuccess(service, operationID, intent, PathRESTToDCS, data, appendFlushEvidence(evidence, data.Results))
		}
		if result.HTTPStatus == 404 {
			data.Verification = VerifiedFailed
			return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryNotFound,
				"Patroni reported that no scheduled switchover could be flushed", result.Error, appendFlushEvidence(evidence, data.Results))
		}
	}
	return service.executeSwitchoverFlushFallback(ctx, operationID, intent, plan, snapshot.Target, data, evidence)
}

func classifySwitchoverFlushResponse(service *Service, target model.Target, response patroni.Response[string], callError error) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/switchover", SendState: send}
	if response.StatusCode == 200 {
		evidence.Summary = "Patroni removed the scheduled switchover"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded, HTTPStatus: 200, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if response.StatusCode != 0 {
		evidence.Summary = "Patroni did not remove the scheduled switchover"
		category, retryable := classifyHTTPStatus(response.StatusCode)
		errorValue := NewError(category, "flush", target, retryable, evidence.Summary, callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed, HTTPStatus: response.StatusCode, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = "scheduled switchover flush transport ended without an HTTP response"
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "flush", target, false, "scheduled switchover flush may have been sent", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, "flush", target, retryable, "scheduled switchover flush was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) executeSwitchoverFlushFallback(
	ctx context.Context,
	operationID string,
	intent flushIntent,
	plan Plan,
	effectiveTarget model.Target,
	data FlushData,
	baseEvidence []Evidence,
) Result[FlushData] {
	evidence := appendFlushEvidence(baseEvidence, data.Results)
	effectiveTarget.Member = ""
	fresh, err := service.snapshots.Snapshot(ctx, effectiveTarget)
	if err != nil {
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "scheduled switchover fallback could not read DCS", Path: string(PathDCS), SendState: SendNotSent})
		if flushHasAmbiguousREST(data.Results) {
			return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover REST outcome is ambiguous and DCS could not be verified", err, evidence)
		}
		category, retryable := classifyReadError(err)
		data.Verification = VerifiedFailed
		return flushFailureWithRetry(service, operationID, intent, PathRESTToDCS, data, category, retryable, "scheduled switchover fallback could not read DCS", err, evidence)
	}
	evidence = append(evidence, snapshotEvidence(service, fresh, "scheduled switchover fallback obtained a fresh DCS snapshot"))
	if fresh.Cluster.Failover == nil || fresh.Cluster.Failover.ScheduledAt == "" {
		data.Verification = VerifiedSucceeded
		data.DCSRevision = fresh.Revision
		return flushSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	if err := revalidateSwitchoverFlushPlan(plan, fresh.Cluster.Failover, leaderFirstRESTMembers(fresh.Cluster), fresh.Target); err != nil {
		if flushHasAmbiguousREST(data.Results) {
			return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover may have been flushed but fallback preconditions changed", err, evidence)
		}
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict, "scheduled switchover fallback lost its confirmed preconditions", err, evidence)
	}
	if service.failoverDCS == nil {
		err := errors.New("DCS failover capability is unavailable")
		if flushHasAmbiguousREST(data.Results) {
			return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover may have been flushed and DCS fallback is unavailable", err, evidence)
		}
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConfig, "scheduled switchover fallback requires DCS delete capability", err, evidence)
	}
	if err := ctx.Err(); err != nil {
		if flushHasAmbiguousREST(data.Results) {
			return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover may have been flushed; cancellation prevented DCS fallback", err, evidence)
		}
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryFailed, "scheduled switchover fallback was canceled before DCS delete", err, evidence)
	}
	expectedRevision := fresh.Cluster.Failover.ModRevision
	writeResult, writeError := service.failoverDCS.DeleteFailover(ctx, effectiveTarget, &expectedRevision)
	data.DCSSendState = dcsSendState(writeError, writeResult.Applied)
	data.DCSRevision = writeResult.Revision
	evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: switchoverFlushDCSDeleteSummary(writeResult, writeError), Revision: strconv.FormatInt(writeResult.Revision, 10), Path: string(PathDCS), SendState: data.DCSSendState})
	verified, revision, verificationEvidence := service.verifySwitchoverFlush(ctx, effectiveTarget)
	evidence = append(evidence, verificationEvidence...)
	if revision > data.DCSRevision {
		data.DCSRevision = revision
	}
	if verified {
		data.Verification = VerifiedSucceeded
		return flushSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	var conflict *dcs.ConflictError
	if errors.As(writeError, &conflict) {
		if flushHasAmbiguousREST(data.Results) || data.DCSSendState == SendMaybeSent {
			return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover REST/DCS deletion conflicted and was not verified", writeError, evidence)
		}
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict, "scheduled switchover DCS delete lost a compare-and-swap race", writeError, evidence)
	}
	if writeError != nil && data.DCSSendState == SendNotSent && !flushHasAmbiguousREST(data.Results) {
		category, retryable := classifyReadError(writeError)
		data.Verification = VerifiedFailed
		return flushFailureWithRetry(service, operationID, intent, PathRESTToDCS, data, category, retryable, "scheduled switchover was not deleted through REST or DCS", writeError, evidence)
	}
	return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover deletion was not verified; inspect DCS before retrying", writeError, evidence)
}

func revalidateSwitchoverFlushPlan(plan Plan, failover *dcs.Failover, members []dcs.Member, clusterTarget model.Target) error {
	if failover == nil {
		return errors.New("scheduled failover key disappeared")
	}
	expected := map[string]string{
		"failover.modRevision": strconv.FormatInt(failover.ModRevision, 10),
		"failover.leader":      failover.Leader,
		"failover.candidate":   failover.Candidate,
		"failover.scheduledAt": failover.ScheduledAt,
		"rest.members":         strings.Join(memberNamesFromTargets(memberTargets(clusterTarget, members)), ","),
		"rest.targets":         strings.Join(memberIDsFromTargets(memberTargets(clusterTarget, members)), ","),
	}
	for field, value := range expected {
		planned, ok := expectedPrecondition(plan, field)
		if !ok || planned != value {
			return fmt.Errorf("confirmed %s changed", field)
		}
	}
	return nil
}

func memberMap(members []dcs.Member) map[string]dcs.Member {
	result := make(map[string]dcs.Member, len(members))
	for _, member := range members {
		result[member.Name] = member
	}
	return result
}

func aggregateRESTSendState(results []MemberWriteResult) SendState {
	state := SendNotSent
	for _, result := range results {
		if result.SendState == SendMaybeSent {
			return SendMaybeSent
		}
		if result.SendState == SendAccepted {
			state = SendAccepted
		}
	}
	return state
}

func flushHasAmbiguousREST(results []MemberWriteResult) bool {
	for _, result := range results {
		if result.Outcome == Unknown {
			return true
		}
	}
	return false
}

func appendFlushEvidence(base []Evidence, results []MemberWriteResult) []Evidence {
	evidence := append([]Evidence(nil), base...)
	for _, result := range results {
		evidence = append(evidence, result.Evidence...)
	}
	return evidence
}

func switchoverFlushDCSDeleteSummary(result dcs.WriteResult, err error) string {
	if err == nil && result.Applied {
		return "DCS scheduled switchover delete CAS was applied"
	}
	var conflict *dcs.ConflictError
	if errors.As(err, &conflict) {
		return "DCS scheduled switchover delete CAS conflicted"
	}
	return "DCS scheduled switchover delete ended without a verified apply"
}

func (service *Service) verifySwitchoverFlush(ctx context.Context, target model.Target) (bool, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	var revision int64
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "scheduled switchover flush verification canceled", Path: string(PathDCS)})
				return false, revision, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "scheduled switchover flush readback failed", Path: string(PathDCS)})
			continue
		}
		revision = snapshot.Revision
		present := snapshot.Cluster.Failover != nil && snapshot.Cluster.Failover.ScheduledAt != ""
		summary := "scheduled switchover remains present in DCS"
		if !present {
			summary = "scheduled switchover is absent from DCS"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary, Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if !present {
			return true, revision, evidence
		}
	}
	return false, revision, evidence
}

func (service *Service) finishCanceledSwitchoverFlush(operationID string, intent flushIntent, data FlushData, cause error, evidence []Evidence) Result[FlushData] {
	evidence = appendFlushEvidence(evidence, data.Results)
	if flushHasAmbiguousREST(data.Results) {
		return flushUnknown(service, operationID, intent, PathRESTToDCS, data, "scheduled switchover may have been flushed before cancellation; verify DCS", cause, evidence)
	}
	data.Verification = VerifiedFailed
	return flushFailure(service, operationID, intent, PathRESTToDCS, data, CategoryFailed, "scheduled switchover flush was canceled before completion", cause, evidence)
}

func finalizeFlushResults(service *Service, operationID string, intent flushIntent, path Path, results []MemberWriteResult, base []Evidence) Result[FlushData] {
	data := FlushData{Event: intent.event, Results: results, RESTSendState: aggregateRESTSendState(results)}
	evidence := appendFlushEvidence(base, results)
	outcome := Succeeded
	category := CategoryFailed
	var cause error
	allNoop := len(results) > 0
	for _, result := range results {
		if result.SendState != SendNotSent || result.Outcome != Succeeded {
			allNoop = false
		}
		if result.Outcome == Unknown {
			outcome = Unknown
			cause = result.Error
		} else if result.Outcome == Failed && outcome != Unknown {
			outcome = Failed
			cause = result.Error
			if result.Error != nil {
				category = result.Error.Category
			}
		}
	}
	data.Noop = allNoop
	switch outcome {
	case Succeeded:
		data.Verification = VerifiedSucceeded
		return flushSuccess(service, operationID, intent, path, data, evidence)
	case Failed:
		data.Verification = VerifiedFailed
		return flushFailure(service, operationID, intent, path, data, category, "flush failed for one or more members", cause, evidence)
	default:
		return flushUnknown(service, operationID, intent, path, data, "flush outcome is unknown for one or more members; verify before retrying", cause, evidence)
	}
}

func flushSuccess(service *Service, operationID string, intent flushIntent, path Path, data FlushData, evidence []Evidence) Result[FlushData] {
	return Result[FlushData]{OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: path, Data: data, Evidence: evidence}
}

func flushFailure(service *Service, operationID string, intent flushIntent, path Path, data FlushData, category Category, message string, cause error, evidence []Evidence) Result[FlushData] {
	return flushFailureWithRetry(service, operationID, intent, path, data, category, false, message, cause, evidence)
}

func flushFailureWithRetry(service *Service, operationID string, intent flushIntent, path Path, data FlushData, category Category, retryable bool, message string, cause error, evidence []Evidence) Result[FlushData] {
	data.Verification = VerifiedFailed
	return Result[FlushData]{OperationID: operationID, Outcome: Failed, Target: intent.target, Path: path, Data: data, Evidence: evidence,
		Error: NewError(category, "flush", intent.target, retryable, message, cause, evidence...)}
}

func flushUnknown(service *Service, operationID string, intent flushIntent, path Path, data FlushData, message string, cause error, evidence []Evidence) Result[FlushData] {
	data.Verification = Unverified
	return Result[FlushData]{OperationID: operationID, Outcome: Unknown, Target: intent.target, Path: path, Data: data, Evidence: evidence,
		Error: NewError(CategoryUnknown, "flush", intent.target, false, message, cause, evidence...)}
}
