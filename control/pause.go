package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

type pauseIntent struct {
	operation string
	target    model.Target
	desired   bool
	wait      bool
	citus     bool
}

func (service *Service) PreparePause(ctx context.Context, request PauseRequest) Result[Plan] {
	intent, err := normalizePauseRequest("pause", true, request)
	return service.preparePause(ctx, intent, err)
}

func (service *Service) ExecutePause(ctx context.Context, request PauseRequest, plan Plan) Result[PauseData] {
	intent, err := normalizePauseRequest("pause", true, request)
	return service.executePause(ctx, intent, err, plan)
}

func (service *Service) PrepareResume(ctx context.Context, request PauseRequest) Result[Plan] {
	intent, err := normalizePauseRequest("resume", false, request)
	return service.preparePause(ctx, intent, err)
}

func (service *Service) ExecuteResume(ctx context.Context, request PauseRequest, plan Plan) Result[PauseData] {
	intent, err := normalizePauseRequest("resume", false, request)
	return service.executePause(ctx, intent, err, plan)
}

func normalizePauseRequest(operation string, desired bool, request PauseRequest) (pauseIntent, error) {
	intent := pauseIntent{operation: operation, target: request.Target.Normalize(), desired: desired, wait: request.Wait, citus: request.Citus}
	if intent.target.Member != "" {
		return intent, errors.New("pause/resume target must be a cluster")
	}
	if err := intent.target.Validate(true); err != nil {
		return intent, err
	}
	return intent, nil
}

func (service *Service) preparePause(ctx context.Context, intent pauseIntent, requestError error) Result[Plan] {
	operationID := service.operationID()
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false, intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false, intent.operation+" request is invalid", requestError)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, category, retryable, intent.operation+" cluster discovery failed", err)
	}
	if versionError := checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[Plan](service, operationID, intent.operation, intent.target, PathREST, snapshot, versionError)
	}
	current := boolConfig(snapshot.Cluster.Config["pause"])
	if current == intent.desired {
		message := "cluster is already paused"
		if !intent.desired {
			message = "cluster is not paused"
		}
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryConflict, false, message, nil,
			snapshotEvidence(service, snapshot, intent.operation+" observed current cluster state"))
	}
	members := leaderFirstRESTMembers(snapshot.Cluster)
	if len(members) == 0 {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryNotFound, false,
			"cannot find an accessible cluster member", nil, snapshotEvidence(service, snapshot, intent.operation+" member selection completed"))
	}
	targets := memberTargets(intent.target, members)
	plan := Plan{
		OperationID: operationID, Operation: intent.operation, Target: intent.target, Targets: targets,
		Path: PathREST, Risk: RiskAvailability, RetrySafety: UnsafeAfterSend,
		Summary: fmt.Sprintf("%s cluster management through leader-first members: %s", intent.operation, strings.Join(memberNamesFromTargets(targets), ", ")),
		Preconditions: []Precondition{
			{Field: "dcs.revision", Expected: strconv.FormatInt(snapshot.Revision, 10), Source: EvidenceDCS},
			{Field: "pause.current", Expected: strconv.FormatBool(current), Source: EvidenceDCS},
			{Field: "pause.desired", Expected: strconv.FormatBool(intent.desired), Source: EvidenceControl},
			{Field: "request.wait", Expected: strconv.FormatBool(intent.wait), Source: EvidenceControl},
			{Field: "request.citus", Expected: strconv.FormatBool(intent.citus), Source: EvidenceControl},
			{Field: "rest.members", Expected: strings.Join(memberNamesFromTargets(targets), ","), Source: EvidenceDCS},
		},
	}
	if err := plan.Validate(); err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" plan construction failed", err, snapshotEvidence(service, snapshot, intent.operation+" member selection completed"))
	}
	return Result[Plan]{OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: PathREST, Data: plan,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, intent.operation+" plan built from fresh cluster snapshot")}}
}

func (service *Service) executePause(ctx context.Context, intent pauseIntent, requestError error, plan Plan) Result[PauseData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	if !validContext(ctx) {
		return failedRead[PauseData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false, intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[PauseData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false, intent.operation+" request is invalid", requestError)
	}
	if err := validatePausePlan(plan, intent); err != nil {
		return failedRead[PauseData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false, intent.operation+" plan does not match the request", err)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[PauseData](service, operationID, intent.operation, intent.target, PathREST, category, retryable, intent.operation+" execution snapshot failed", err)
	}
	if versionError := checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[PauseData](service, operationID, intent.operation, intent.target, PathREST, snapshot, versionError)
	}
	evidence := []Evidence{snapshotEvidence(service, snapshot, intent.operation+" targets revalidated before execution")}
	data := PauseData{Paused: intent.desired, Wait: intent.wait, RESTSendState: SendNotSent, Verification: Unverified}
	memberNames := memberNamesFromTargets(plan.Targets)
	if boolConfig(snapshot.Cluster.Config["pause"]) == intent.desired {
		data.Noop = true
		if !intent.wait {
			data.Verification = VerifiedSucceeded
			data.DCSRevision = snapshot.Revision
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		verified, pending, revision, verificationEvidence := service.verifyPauseState(ctx, intent, memberNames)
		evidence = append(evidence, verificationEvidence...)
		data.PendingMembers, data.DCSRevision = pending, revision
		if verified {
			data.Verification = VerifiedSucceeded
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		return pauseUnknown(service, operationID, intent, data, intent.operation+" state is set but member convergence is unverified", nil, evidence)
	}
	currentMembers := leaderFirstRESTMembers(snapshot.Cluster)
	if strings.Join(memberNamesFromTargets(memberTargets(intent.target, currentMembers)), ",") != strings.Join(memberNames, ",") {
		data.Verification = VerifiedFailed
		return pauseFailure(service, operationID, intent, data, CategoryConflict, false,
			intent.operation+" was not sent because the confirmed REST member set changed", nil, evidence)
	}

	members := memberMap(snapshot.Cluster.Members)
	payload := patroni.DynamicConfig{"pause": true}
	if !intent.desired {
		payload["pause"] = nil
	}
	for _, target := range plan.Targets {
		if err := ctx.Err(); err != nil {
			return service.finishCanceledPause(operationID, intent, data, err, evidence)
		}
		member := members[target.Member]
		var result MemberWriteResult
		if service.patroni == nil {
			result = pauseMemberNotSent(service, intent, target, CategoryConfig, false, "Patroni REST client is unavailable", nil)
		} else if baseURL, urlError := patroniBaseURL(member.Data.APIURL); urlError != nil {
			result = invalidRESTAddressWrite(service, intent.operation, target, snapshot, urlError)
		} else {
			response, callError := service.patroni.PatchConfig(ctx, baseURL, payload)
			result = classifyPauseResponse(service, intent, target, response, callError)
		}
		data.Results = append(data.Results, result)
		data.RESTSendState = aggregateRESTSendState(data.Results)
		if result.HTTPStatus != 0 {
			return service.finishPauseHTTPResponse(ctx, operationID, intent, data, result, memberNames, evidence)
		}
	}
	if flushHasAmbiguousREST(data.Results) {
		verified, pending, revision, verificationEvidence := service.verifyPauseState(ctx, intent, memberNames)
		evidence = append(appendFlushEvidence(evidence, data.Results), verificationEvidence...)
		data.PendingMembers, data.DCSRevision = pending, revision
		if verified {
			data.Verification = VerifiedSucceeded
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		return pauseUnknown(service, operationID, intent, data, intent.operation+" may have been sent but authoritative state did not verify it", lastPauseError(data.Results), evidence)
	}
	data.Verification = VerifiedFailed
	evidence = appendFlushEvidence(evidence, data.Results)
	category, retryable, cause := terminalPauseFailure(data.Results)
	return pauseFailure(service, operationID, intent, data, category, retryable, "cannot find an accessible cluster member", cause, evidence)
}

func validatePausePlan(plan Plan, intent pauseIntent) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != intent.operation || plan.Path != PathREST || plan.Risk != RiskAvailability || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from pause/resume")
	}
	if plan.Target.Normalize().ClusterID() != intent.target.ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	expected := map[string]string{
		"pause.desired": strconv.FormatBool(intent.desired),
		"request.wait":  strconv.FormatBool(intent.wait),
		"request.citus": strconv.FormatBool(intent.citus),
		"rest.members":  strings.Join(memberNamesFromTargets(plan.Targets), ","),
	}
	for field, value := range expected {
		planned, ok := expectedPrecondition(plan, field)
		if !ok || planned != value {
			return fmt.Errorf("plan %s differs from request", field)
		}
	}
	if len(plan.Targets) == 0 {
		return errors.New("pause/resume plan requires REST member targets")
	}
	return nil
}

func classifyPauseResponse(
	service *Service,
	intent pauseIntent,
	target model.Target,
	response patroni.Response[patroni.DynamicConfig],
	callError error,
) MemberWriteResult {
	send := patroniSendState(callError, response.StatusCode)
	evidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/config", SendState: send}
	if response.StatusCode == 200 {
		evidence.Summary = "Patroni accepted " + intent.operation + " configuration"
		return MemberWriteResult{Target: target, Outcome: Succeeded, SendState: send, Verification: VerifiedSucceeded, HTTPStatus: 200, Summary: evidence.Summary, Evidence: []Evidence{evidence}}
	}
	if response.StatusCode != 0 {
		evidence.Summary = "Patroni rejected " + intent.operation + " configuration"
		category, retryable := classifyHTTPStatus(response.StatusCode)
		errorValue := NewError(category, intent.operation, target, retryable, evidence.Summary, callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Failed, SendState: send, Verification: VerifiedFailed, HTTPStatus: response.StatusCode, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	evidence.Summary = intent.operation + " transport ended without an HTTP response"
	if send == SendMaybeSent || send == SendAccepted {
		errorValue := NewError(CategoryUnknown, intent.operation, target, false, intent.operation+" may have been sent", callError, evidence)
		return MemberWriteResult{Target: target, Outcome: Unknown, SendState: send, Verification: Unverified, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
	}
	category, retryable := classifyReadError(callError)
	errorValue := NewError(category, intent.operation, target, retryable, intent.operation+" was not sent", callError, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func pauseMemberNotSent(service *Service, intent pauseIntent, target model.Target, category Category, retryable bool, message string, cause error) MemberWriteResult {
	evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: message, Path: string(PathREST), SendState: SendNotSent}
	errorValue := NewError(category, intent.operation, target, retryable, message, cause, evidence)
	return MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: message, Evidence: []Evidence{evidence}, Error: errorValue}
}

func (service *Service) finishPauseHTTPResponse(
	ctx context.Context,
	operationID string,
	intent pauseIntent,
	data PauseData,
	terminal MemberWriteResult,
	members []string,
	baseEvidence []Evidence,
) Result[PauseData] {
	evidence := appendFlushEvidence(baseEvidence, data.Results)
	if terminal.HTTPStatus == 200 {
		if !intent.wait {
			data.Verification = VerifiedSucceeded
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		verified, pending, revision, verificationEvidence := service.verifyPauseState(ctx, intent, members)
		evidence = append(evidence, verificationEvidence...)
		data.PendingMembers, data.DCSRevision = pending, revision
		if verified {
			data.Verification = VerifiedSucceeded
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		return pauseUnknown(service, operationID, intent, data, intent.operation+" was accepted but member convergence was not verified", terminal.Error, evidence)
	}
	if flushHasAmbiguousREST(data.Results) {
		verified, pending, revision, verificationEvidence := service.verifyPauseState(ctx, intent, members)
		evidence = append(evidence, verificationEvidence...)
		data.PendingMembers, data.DCSRevision = pending, revision
		if verified {
			data.Verification = VerifiedSucceeded
			return pauseSuccess(service, operationID, intent, data, evidence)
		}
		return pauseUnknown(service, operationID, intent, data, intent.operation+" had an earlier ambiguous send and was not verified", terminal.Error, evidence)
	}
	data.Verification = VerifiedFailed
	category, retryable := classifyHTTPStatus(terminal.HTTPStatus)
	return pauseFailure(service, operationID, intent, data, category, retryable, "Patroni rejected "+intent.operation, terminal.Error, evidence)
}

func (service *Service) verifyPauseState(ctx context.Context, intent pauseIntent, plannedMembers []string) (bool, []string, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	var pending []string
	var revision int64
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: intent.operation + " convergence verification canceled", Path: string(PathDCS)})
				return false, pending, revision, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: intent.operation + " convergence readback failed", Path: string(PathDCS)})
			continue
		}
		revision = snapshot.Revision
		pending = pausePendingMembers(snapshot.Cluster, plannedMembers, intent.desired)
		configMatches := boolConfig(snapshot.Cluster.Config["pause"]) == intent.desired
		matched := configMatches && (!intent.wait || len(pending) == 0)
		summary := intent.operation + " state is not yet authoritative"
		if matched {
			summary = intent.operation + " state is authoritative"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary, Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if matched {
			return true, nil, revision, evidence
		}
	}
	return false, pending, revision, evidence
}

func pausePendingMembers(cluster dcs.ClusterState, planned []string, desired bool) []string {
	byName := memberMap(cluster.Members)
	pending := make([]string, 0)
	for _, name := range planned {
		member, ok := byName[name]
		paused := ok && member.Data.Pause != nil && *member.Data.Pause
		if !ok || paused != desired {
			pending = append(pending, name)
		}
	}
	sort.Strings(pending)
	return pending
}

func (service *Service) finishCanceledPause(operationID string, intent pauseIntent, data PauseData, cause error, evidence []Evidence) Result[PauseData] {
	evidence = appendFlushEvidence(evidence, data.Results)
	if flushHasAmbiguousREST(data.Results) {
		return pauseUnknown(service, operationID, intent, data, intent.operation+" may have been sent before cancellation; verify DCS", cause, evidence)
	}
	data.Verification = VerifiedFailed
	return pauseFailure(service, operationID, intent, data, CategoryFailed, false, intent.operation+" was canceled before completion", cause, evidence)
}

func terminalPauseFailure(results []MemberWriteResult) (Category, bool, error) {
	category, retryable := CategoryUnreachable, true
	var cause error
	for _, result := range results {
		if result.Error != nil {
			category, retryable, cause = result.Error.Category, result.Error.Retryable, result.Error
		}
	}
	return category, retryable, cause
}

func lastPauseError(results []MemberWriteResult) error {
	for index := len(results) - 1; index >= 0; index-- {
		if results[index].Error != nil {
			return results[index].Error
		}
	}
	return nil
}

func pauseSuccess(service *Service, operationID string, intent pauseIntent, data PauseData, evidence []Evidence) Result[PauseData] {
	return Result[PauseData]{OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence}
}

func pauseFailure(service *Service, operationID string, intent pauseIntent, data PauseData, category Category, retryable bool, message string, cause error, evidence []Evidence) Result[PauseData] {
	data.Verification = VerifiedFailed
	return Result[PauseData]{OperationID: operationID, Outcome: Failed, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence,
		Error: NewError(category, intent.operation, intent.target, retryable, message, cause, evidence...)}
}

func pauseUnknown(service *Service, operationID string, intent pauseIntent, data PauseData, message string, cause error, evidence []Evidence) Result[PauseData] {
	data.Verification = Unverified
	return Result[PauseData]{OperationID: operationID, Outcome: Unknown, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence,
		Error: NewError(CategoryUnknown, intent.operation, intent.target, false, message, cause, evidence...)}
}
