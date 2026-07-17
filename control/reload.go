package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni"
)

func (service *Service) PrepareReload(ctx context.Context, request ReloadRequest) Result[Plan] {
	operationID := service.operationID()
	target, requestedMembers, validationError := normalizeReloadRequest(request)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "reload", target, PathREST, CategoryUsage, false, "reload requires a context", nil)
	}
	if validationError != nil {
		return failedRead[Plan](service, operationID, "reload", target, PathREST, CategoryUsage, false, "reload request is invalid", validationError)
	}
	snapshots, evidence, failure := service.operationSnapshots(ctx, "reload", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[Plan](service, operationID, "reload", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	targets := make([]model.Target, 0)
	for _, snapshot := range snapshots {
		selected := selectMembers(snapshot.Cluster, normalizedWriteRole(request.Role), requestedMembers, true)
		for _, member := range selected {
			memberTarget := snapshot.Target.Normalize()
			memberTarget.Member = member.Name
			targets = append(targets, memberTarget)
		}
	}
	if len(targets) == 0 {
		return failedReadWithEvidence[Plan](service, operationID, "reload", target, PathREST, CategoryNotFound, false, "reload found no matching members", nil, evidence)
	}
	preconditions := []Precondition{
		{Field: "dcs.revision", Expected: strconv.FormatInt(snapshots[0].Revision, 10), Source: EvidenceDCS},
		{Field: "selector.role", Expected: string(normalizedWriteRole(request.Role)), Source: EvidenceControl},
		{Field: "selector.members", Expected: strings.Join(requestedMembers, ","), Source: EvidenceControl},
		{Field: "selector.citus", Expected: strconv.FormatBool(request.Citus), Source: EvidenceControl},
		{Field: "citus.groups", Expected: snapshotGroupIDs(snapshots), Source: EvidenceDCS},
		{Field: "cluster.loopWait", Expected: strconv.Itoa(clusterLoopWait(snapshots[0].Cluster.Config)), Source: EvidenceDCS},
	}
	plan := Plan{
		OperationID: operationID, Operation: "reload", Target: target, Targets: targets,
		Path: PathREST, Risk: RiskAdminWrite, RetrySafety: UnsafeAfterSend,
		Summary:       fmt.Sprintf("reload %d selected member(s): %s", len(targets), strings.Join(memberNamesFromTargets(targets), ", ")),
		Preconditions: preconditions,
	}
	if err := plan.Validate(); err != nil {
		return failedReadWithEvidence[Plan](service, operationID, "reload", target, PathREST, CategoryInternal, false, "reload plan construction failed", err, evidence)
	}
	return Result[Plan]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathREST, Data: plan,
		Evidence: evidence,
	}
}

func clusterLoopWait(configuration map[string]any) int {
	value, exists := configuration["loop_wait"]
	if !exists {
		return 10
	}
	parsed, err := strconv.Atoi(fmt.Sprint(value))
	if err != nil || parsed <= 0 {
		return 10
	}
	return parsed
}

func (service *Service) ExecuteReload(ctx context.Context, request ReloadRequest, plan Plan) Result[BatchWriteData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	target, requestedMembers, requestError := normalizeReloadRequest(request)
	if !validContext(ctx) {
		return failedRead[BatchWriteData](service, operationID, "reload", target, PathREST, CategoryUsage, false, "reload requires a context", nil)
	}
	if requestError != nil {
		return failedRead[BatchWriteData](service, operationID, "reload", target, PathREST, CategoryUsage, false, "reload request is invalid", requestError)
	}
	if err := validateReloadPlan(plan, target, normalizedWriteRole(request.Role), requestedMembers, request.Citus); err != nil {
		return failedRead[BatchWriteData](service, operationID, "reload", target, PathREST, CategoryUsage, false, "reload plan does not match the request", err)
	}
	if service.patroni == nil {
		return failedRead[BatchWriteData](service, operationID, "reload", target, PathREST, CategoryConfig, false, "reload requires a Patroni REST client", nil)
	}
	snapshots, baseEvidence, failure := service.operationSnapshots(ctx, "reload", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "reload", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, baseEvidence)
	}
	plannedGroups, _ := expectedPrecondition(plan, "citus.groups")
	if snapshotGroupIDs(snapshots) != plannedGroups {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "reload", target, PathREST, CategoryConflict, false,
			"reload was not sent because the Citus group inventory changed", nil, baseEvidence)
	}
	resolved, conflictResults := resolvePlannedMembersAcrossSnapshots(service, "reload", snapshots, plan.Targets, normalizedWriteRole(request.Role))
	if len(conflictResults) > 0 {
		members := make([]MemberWriteResult, 0, len(plan.Targets))
		conflicts := make(map[string]MemberWriteResult, len(conflictResults))
		for _, result := range conflictResults {
			conflicts[result.Target.MemberID()] = result
		}
		for _, planned := range plan.Targets {
			if result, ok := conflicts[planned.Normalize().MemberID()]; ok {
				members = append(members, result)
				continue
			}
			evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "reload skipped because the confirmed target set changed", Path: string(PathREST), SendState: SendNotSent}
			errorValue := NewError(CategoryConflict, "reload", planned, false, "reload was not sent because the confirmed target set changed", nil, evidence)
			members = append(members, MemberWriteResult{Target: planned, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
		}
		return finalizeBatchWrite(service, operationID, "reload", target, PathREST, BatchWriteData{Members: members}, baseEvidence)
	}

	results := make([]MemberWriteResult, 0, len(resolved))
	for _, resolvedMember := range resolved {
		memberTarget := resolvedMember.target
		if err := ctx.Err(); err != nil {
			evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "reload skipped after cancellation", Path: "/reload", SendState: SendNotSent}
			errorValue := NewError(CategoryFailed, "reload", memberTarget, false, "reload was not sent because the operation was canceled", err, evidence)
			results = append(results, MemberWriteResult{Target: memberTarget, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
			continue
		}
		baseURL, urlError := patroniBaseURL(resolvedMember.member.Data.APIURL)
		if urlError != nil {
			evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "member has no usable Patroni REST address", Revision: strconv.FormatInt(resolvedMember.snapshot.Revision, 10), Path: resolvedMember.snapshot.Prefix, SendState: SendNotSent}
			errorValue := NewError(CategoryConfig, "reload", memberTarget, false, "reload was not sent because the member REST address is invalid", urlError, evidence)
			results = append(results, MemberWriteResult{Target: memberTarget, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
			continue
		}
		response, callError := service.patroni.PostReload(ctx, baseURL)
		results = append(results, classifyReloadResponse(service, memberTarget, response, callError))
	}
	return finalizeBatchWrite(service, operationID, "reload", target, PathREST, BatchWriteData{Members: results}, baseEvidence)
}

func normalizeReloadRequest(request ReloadRequest) (model.Target, []string, error) {
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
	return target, members, nil
}

func normalizedWriteRole(role Role) Role {
	if role == "" {
		return RoleAny
	}
	return role
}

func selectMembers(cluster dcs.ClusterState, role Role, requested []string, selectAllWhenEmpty bool) []dcs.Member {
	requestedSet := stringSet(requested)
	selected := make([]dcs.Member, 0, len(cluster.Members))
	for _, member := range cluster.Members {
		if !memberMatchesRole(cluster, member, role) {
			continue
		}
		if len(requestedSet) > 0 {
			if _, ok := requestedSet[member.Name]; !ok {
				continue
			}
		} else if !selectAllWhenEmpty {
			continue
		}
		selected = append(selected, member)
	}
	sort.SliceStable(selected, func(left, right int) bool { return selected[left].Name < selected[right].Name })
	return selected
}

func memberMatchesRole(cluster dcs.ClusterState, member dcs.Member, role Role) bool {
	leaderName := ""
	if cluster.Leader != nil {
		leaderName = cluster.Leader.Name
	}
	isLeader := member.Name == leaderName
	wireRole := strings.ReplaceAll(strings.ToLower(member.Data.Role), "-", "_")
	switch role {
	case RoleAny:
		return true
	case RoleLeader:
		return isLeader
	case RolePrimary:
		return isLeader && wireRole != "standby_leader"
	case RoleStandbyLeader:
		return isLeader && wireRole != "primary" && wireRole != "master"
	case RoleReplica, RoleStandby:
		return !isLeader
	default:
		return false
	}
}

func validateReloadPlan(plan Plan, target model.Target, role Role, members []string, citus bool) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "reload" || plan.Path != PathREST || plan.Risk != RiskAdminWrite || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from reload")
	}
	if plan.Target.Normalize().ClusterID() != target.Normalize().ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	plannedRole, hasRole := expectedPrecondition(plan, "selector.role")
	plannedMembers, hasMembers := expectedPrecondition(plan, "selector.members")
	plannedCitus, hasCitus := expectedPrecondition(plan, "selector.citus")
	_, hasGroups := expectedPrecondition(plan, "citus.groups")
	if !hasRole || !hasMembers || !hasCitus || !hasGroups || plannedRole != string(role) ||
		plannedMembers != strings.Join(members, ",") || plannedCitus != strconv.FormatBool(citus) {
		return errors.New("plan selectors differ from request")
	}
	return nil
}

func expectedPrecondition(plan Plan, field string) (string, bool) {
	for _, precondition := range plan.Preconditions {
		if precondition.Field == field {
			return precondition.Expected, true
		}
	}
	return "", false
}

func memberNamesFromTargets(targets []model.Target) []string {
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Member)
	}
	return names
}

func resolvePlannedMembers(service *Service, operation string, snapshot dcs.Snapshot, targets []model.Target, role Role) ([]dcs.Member, []MemberWriteResult) {
	byName := make(map[string]dcs.Member, len(snapshot.Cluster.Members))
	for _, member := range snapshot.Cluster.Members {
		byName[member.Name] = member
	}
	resolved := make([]dcs.Member, 0, len(targets))
	conflicts := make([]MemberWriteResult, 0)
	for _, target := range targets {
		member, exists := byName[target.Member]
		if exists && memberMatchesRole(snapshot.Cluster, member, role) {
			resolved = append(resolved, member)
			continue
		}
		evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "confirmed member is absent or no longer matches its role", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent}
		errorValue := NewError(CategoryConflict, operation, target, false, operation+" was not sent because cluster membership changed", nil, evidence)
		conflicts = append(conflicts, MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
	}
	return resolved, conflicts
}

func classifyReloadResponse(service *Service, target model.Target, response patroni.Response[string], callError error) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	status := response.StatusCode
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/reload", SendState: send}
	if status == 200 || status == 202 {
		evidence.Summary = "Patroni accepted reload request"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded, HTTPStatus: status, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if status != 0 {
		evidence.Summary = "Patroni rejected reload request"
		category, retryable := classifyHTTPStatus(status)
		errorValue := NewError(category, "reload", target, retryable, "Patroni rejected reload request", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed, HTTPStatus: status, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = "reload transport ended without an HTTP response"
	category, retryable := classifyReadError(callError)
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "reload", target, false, "reload may have been sent; verify the member before retrying", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	errorValue := NewError(category, "reload", target, retryable, "reload was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func patroniSendState(err error, status int) SendState {
	if status != 0 {
		return SendAccepted
	}
	var typed *patroni.Error
	if errors.As(err, &typed) {
		switch typed.Delivery {
		case patroni.DeliveryNotSent:
			return SendNotSent
		case patroni.DeliveryMaybeSent:
			return SendMaybeSent
		case patroni.DeliveryResponseReceived:
			return SendAccepted
		}
	}
	// A write port that loses delivery metadata is unsafe to classify as a
	// definite failure. Concrete BOAR transports always return typed errors,
	// but third-party implementations fail closed here.
	return SendMaybeSent
}

func finalizeBatchWrite(service *Service, operationID, operation string, target model.Target, path Path, data BatchWriteData, base []Evidence) Result[BatchWriteData] {
	evidence := append([]Evidence(nil), base...)
	outcome := Succeeded
	for _, member := range data.Members {
		evidence = append(evidence, member.Evidence...)
		if member.Outcome == Unknown {
			outcome = Unknown
		} else if member.Outcome == Failed && outcome != Unknown {
			outcome = Failed
		}
	}
	result := Result[BatchWriteData]{OperationID: operationID, Outcome: outcome, Target: target, Path: path, Data: data, Evidence: evidence}
	switch outcome {
	case Failed:
		result.Error = NewError(CategoryFailed, operation, target, false, operation+" failed for one or more members", nil, evidence...)
	case Unknown:
		result.Error = NewError(CategoryUnknown, operation, target, false, operation+" outcome is unknown for one or more members; verify before retrying", nil, evidence...)
	}
	return result
}
