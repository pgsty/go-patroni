package control

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni"
)

func (service *Service) PrepareReinitialize(ctx context.Context, request ReinitializeRequest) Result[Plan] {
	operationID := service.operationID()
	target, members, err := normalizeReinitializeRequest(request)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "reinit", target, PathREST, CategoryUsage, false, "reinitialize requires a context", nil)
	}
	if err != nil {
		return failedRead[Plan](service, operationID, "reinit", target, PathREST, CategoryUsage, false, "reinitialize request is invalid", err)
	}
	if len(members) == 0 && !request.Force {
		return failedRead[Plan](service, operationID, "reinit", target, PathREST, CategoryUsage, false,
			"reinitialize requires an explicitly selected member before confirmation", nil)
	}
	snapshots, evidence, failure := service.operationSnapshots(ctx, "reinit", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[Plan](service, operationID, "reinit", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	replicaCount := 0
	targets := make([]model.Target, 0)
	for _, snapshot := range snapshots {
		replicaCount += len(selectMembers(snapshot.Cluster, RoleReplica, nil, true))
		for _, member := range selectMembers(snapshot.Cluster, RoleReplica, members, false) {
			memberTarget := snapshot.Target.Normalize()
			memberTarget.Member = member.Name
			targets = append(targets, memberTarget)
		}
	}
	if replicaCount == 0 {
		return failedReadWithEvidence[Plan](service, operationID, "reinit", target, PathREST, CategoryNotFound, false,
			"reinitialize found no replica members in the cluster", nil, evidence)
	}
	if len(members) > 0 && len(targets) == 0 {
		return failedReadWithEvidence[Plan](service, operationID, "reinit", target, PathREST, CategoryNotFound, false,
			"reinitialize found no replica among the selected members", nil, evidence)
	}
	summary := "reinitialize no members (forced patronictl-compatible no-op)"
	if len(targets) > 0 {
		summary = fmt.Sprintf("reinitialize %d selected replica member(s): %s", len(targets), strings.Join(memberNamesFromTargets(targets), ", "))
	}
	plan := Plan{
		OperationID: operationID, Operation: "reinit", Target: target, Targets: targets,
		Path: PathREST, Risk: RiskDestructive, RetrySafety: UnsafeAfterSend, Summary: summary,
		Preconditions: reinitializePreconditions(snapshots[0].Revision, request, members, snapshotGroupIDs(snapshots)),
	}
	if err := plan.Validate(); err != nil {
		return failedReadWithEvidence[Plan](service, operationID, "reinit", target, PathREST, CategoryInternal, false,
			"reinitialize plan construction failed", err, evidence)
	}
	return Result[Plan]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathREST, Data: plan,
		Evidence: evidence,
	}
}

func (service *Service) ExecuteReinitialize(ctx context.Context, request ReinitializeRequest, plan Plan) Result[BatchWriteData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	target, members, requestError := normalizeReinitializeRequest(request)
	if !validContext(ctx) {
		return failedRead[BatchWriteData](service, operationID, "reinit", target, PathREST, CategoryUsage, false, "reinitialize requires a context", nil)
	}
	if requestError != nil {
		return failedRead[BatchWriteData](service, operationID, "reinit", target, PathREST, CategoryUsage, false, "reinitialize request is invalid", requestError)
	}
	if err := validateReinitializePlan(plan, target, request, members); err != nil {
		return failedRead[BatchWriteData](service, operationID, "reinit", target, PathREST, CategoryUsage, false, "reinitialize plan does not match the request", err)
	}
	if service.patroni == nil {
		return failedRead[BatchWriteData](service, operationID, "reinit", target, PathREST, CategoryConfig, false, "reinitialize requires a Patroni REST client", nil)
	}
	snapshots, baseEvidence, failure := service.operationSnapshots(ctx, "reinit", target, request.Citus, false)
	if failure != nil {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "reinit", target, PathREST, failure.category, failure.retryable, failure.message, failure.cause, baseEvidence)
	}
	plannedGroups, _ := expectedPrecondition(plan, "citus.groups")
	if snapshotGroupIDs(snapshots) != plannedGroups {
		return failedReadWithEvidence[BatchWriteData](service, operationID, "reinit", target, PathREST, CategoryConflict, false,
			"reinitialize was not sent because the Citus group inventory changed", nil, baseEvidence)
	}
	if len(plan.Targets) == 0 {
		return finalizeBatchWrite(service, operationID, "reinit", target, PathREST, BatchWriteData{Members: []MemberWriteResult{}}, baseEvidence)
	}
	resolved, conflicts := resolvePlannedMembersAcrossSnapshots(service, "reinit", snapshots, plan.Targets, RoleReplica)
	if len(conflicts) > 0 {
		data := abortBatchForTargetChange(service, "reinit", plan.Targets, conflicts)
		return finalizeBatchWrite(service, operationID, "reinit", target, PathREST, data, baseEvidence)
	}
	results := make([]MemberWriteResult, 0, len(resolved))
	for _, resolvedMember := range resolved {
		member := resolvedMember.member
		memberTarget := resolvedMember.target
		snapshot := resolvedMember.snapshot
		if err := ctx.Err(); err != nil {
			results = append(results, skippedWriteAfterCancellation(service, "reinit", memberTarget, err))
			continue
		}
		baseURL, urlError := patroniBaseURL(member.Data.APIURL)
		if urlError != nil {
			results = append(results, invalidRESTAddressWrite(service, "reinit", memberTarget, snapshot, urlError))
			continue
		}
		response, callError := service.patroni.PostReinitialize(ctx, baseURL, patroni.ReinitializeRequest{
			Force: request.Force, FromLeader: request.FromLeader,
		})
		memberResult := classifyReinitializeResponse(service, memberTarget, response, callError)
		if request.Wait && (response.StatusCode > 0 && response.StatusCode < 400 || memberResult.SendState == SendMaybeSent) {
			accepted := response.StatusCode > 0 && response.StatusCode < 400
			completed, verificationEvidence := service.verifyReinitialize(ctx, baseURL, accepted)
			memberResult.Evidence = append(memberResult.Evidence, verificationEvidence...)
			if completed {
				memberResult.Outcome = Succeeded
				memberResult.Verification = VerifiedSucceeded
				memberResult.Error = nil
				memberResult.Summary = "reinitialize completion verified by Patroni"
			} else if memberResult.Outcome != Failed {
				memberResult.Outcome = Unknown
				memberResult.Verification = Unverified
				memberResult.Summary = "reinitialize completion was not verified"
				memberResult.Error = NewError(CategoryUnknown, "reinit", memberTarget, false,
					"reinitialize outcome is unknown; verify the member before retrying", callError, memberResult.Evidence...)
			}
		}
		results = append(results, memberResult)
	}
	return finalizeBatchWrite(service, operationID, "reinit", target, PathREST, BatchWriteData{Members: results}, baseEvidence)
}

func normalizeReinitializeRequest(request ReinitializeRequest) (model.Target, []string, error) {
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
	return target, members, nil
}

func reinitializePreconditions(revision int64, request ReinitializeRequest, members []string, groups string) []Precondition {
	return []Precondition{
		{Field: "dcs.revision", Expected: strconv.FormatInt(revision, 10), Source: EvidenceDCS},
		{Field: "selector.members", Expected: strings.Join(members, ","), Source: EvidenceControl},
		{Field: "selector.citus", Expected: strconv.FormatBool(request.Citus), Source: EvidenceControl},
		{Field: "citus.groups", Expected: groups, Source: EvidenceDCS},
		{Field: "reinit.force", Expected: strconv.FormatBool(request.Force), Source: EvidenceControl},
		{Field: "reinit.fromLeader", Expected: strconv.FormatBool(request.FromLeader), Source: EvidenceControl},
		{Field: "reinit.wait", Expected: strconv.FormatBool(request.Wait), Source: EvidenceControl},
	}
}

func validateReinitializePlan(plan Plan, target model.Target, request ReinitializeRequest, members []string) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "reinit" || plan.Path != PathREST || plan.Risk != RiskDestructive || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from reinitialize")
	}
	if plan.Target.Normalize().ClusterID() != target.Normalize().ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	expected := reinitializePreconditions(0, request, members, "")
	for _, precondition := range expected[1:] {
		value, exists := expectedPrecondition(plan, precondition.Field)
		if !exists || precondition.Field != "citus.groups" && value != precondition.Expected {
			return fmt.Errorf("plan %s differs from request", precondition.Field)
		}
	}
	return nil
}

func classifyReinitializeResponse(service *Service, target model.Target, response patroni.Response[string], callError error) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/reinitialize", SendState: send}
	if response.StatusCode > 0 && response.StatusCode < 400 {
		evidence.Summary = "Patroni accepted reinitialize request"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded,
			HTTPStatus: response.StatusCode, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if response.StatusCode != 0 {
		evidence.Summary = "Patroni rejected reinitialize request"
		category, retryable := classifyHTTPStatus(response.StatusCode)
		errorValue := NewError(category, "reinit", target, retryable, "Patroni rejected reinitialize request", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed,
			HTTPStatus: response.StatusCode, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = "reinitialize transport ended without an HTTP response"
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, "reinit", target, false, "reinitialize may have been sent; verify before retrying", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified,
			Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, "reinit", target, retryable, "reinitialize was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed,
		Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) verifyReinitialize(ctx context.Context, baseURL string, accepted bool) (bool, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	seenCreating := false
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "reinitialize verification canceled", Path: "/patroni"})
				return false, evidence
			}
		}
		response, err := service.patroni.GetPatroni(ctx, baseURL)
		if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
			evidence = append(evidence, Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Summary: "reinitialize status verification failed", Path: "/patroni"})
			continue
		}
		if response.Data.State == "creating replica" {
			seenCreating = true
			evidence = append(evidence, Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Summary: "reinitialize is still creating replica", Path: "/patroni"})
			continue
		}
		evidence = append(evidence, Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Summary: "member left creating-replica state", Path: "/patroni"})
		if accepted || seenCreating {
			return true, evidence
		}
	}
	return false, evidence
}
