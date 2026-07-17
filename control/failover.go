package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

const legacySwitchoverResponse = "Server does not support this operation"

type transitionIntent struct {
	operation   string
	target      model.Target
	leader      string
	candidate   string
	scheduledAt *time.Time
	force       bool
	citus       bool
}

type transitionSelection struct {
	leader          string
	candidate       string
	candidateNames  []string
	candidateMember *dcs.Member
	restMember      *dcs.Member
	syncEligible    bool
	paused          bool
	failoverVersion int64
}

func (service *Service) PrepareFailover(ctx context.Context, request FailoverRequest) Result[Plan] {
	intent, err := normalizeFailoverRequest(request)
	return service.prepareTransition(ctx, intent, err)
}

func (service *Service) ExecuteFailover(ctx context.Context, request FailoverRequest, plan Plan) Result[ClusterWriteData] {
	intent, err := normalizeFailoverRequest(request)
	return service.executeTransition(ctx, intent, err, plan)
}

func (service *Service) PrepareSwitchover(ctx context.Context, request SwitchoverRequest) Result[Plan] {
	intent, err := normalizeSwitchoverRequest(request)
	return service.prepareTransition(ctx, intent, err)
}

func (service *Service) ExecuteSwitchover(ctx context.Context, request SwitchoverRequest, plan Plan) Result[ClusterWriteData] {
	intent, err := normalizeSwitchoverRequest(request)
	return service.executeTransition(ctx, intent, err, plan)
}

func (service *Service) prepareTransition(ctx context.Context, intent transitionIntent, requestError error) Result[Plan] {
	operationID := service.operationID()
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" request is invalid", requestError)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, category, retryable,
			intent.operation+" cluster discovery failed", err)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[Plan](service, operationID, intent.operation, intent.target, PathREST, snapshot, versionError)
	}
	selection, category, selectionError := resolveTransition(snapshot, intent)
	if selectionError != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, category, false,
			intent.operation+" target selection failed", selectionError,
			snapshotEvidence(service, snapshot, intent.operation+" target selection completed"))
	}
	plan := Plan{
		OperationID: operationID,
		Operation:   intent.operation,
		Target:      intent.target,
		Path:        PathREST,
		Risk:        RiskAvailability,
		RetrySafety: UnsafeAfterSend,
		Summary:     transitionSummary(intent, selection),
		Preconditions: transitionPreconditions(
			snapshot.Revision,
			intent,
			selection,
		),
	}
	if err := plan.Validate(); err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" plan construction failed", err,
			snapshotEvidence(service, snapshot, intent.operation+" target selection completed"))
	}
	return Result[Plan]{
		OperationID: operationID,
		Outcome:     Succeeded,
		Target:      intent.target,
		Path:        PathREST,
		Data:        plan,
		Evidence: []Evidence{
			snapshotEvidence(service, snapshot, intent.operation+" plan built from fresh cluster snapshot"),
		},
	}
}

func (service *Service) executeTransition(
	ctx context.Context,
	intent transitionIntent,
	requestError error,
	plan Plan,
) Result[ClusterWriteData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	if !validContext(ctx) {
		return failedRead[ClusterWriteData](service, operationID, intent.operation, intent.target, PathREST,
			CategoryUsage, false, intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[ClusterWriteData](service, operationID, intent.operation, intent.target, PathREST,
			CategoryUsage, false, intent.operation+" request is invalid", requestError)
	}
	if err := validateTransitionPlan(plan, intent); err != nil {
		return failedRead[ClusterWriteData](service, operationID, intent.operation, intent.target, PathREST,
			CategoryUsage, false, intent.operation+" plan does not match the request", err)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[ClusterWriteData](service, operationID, intent.operation, intent.target, PathREST,
			category, retryable, intent.operation+" execution snapshot failed", err)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[ClusterWriteData](service, operationID, intent.operation, intent.target, PathREST, snapshot, versionError)
	}
	selection, _, selectionError := resolveTransition(snapshot, intent)
	baseEvidence := []Evidence{snapshotEvidence(service, snapshot, intent.operation+" target revalidated before execution")}
	if selectionError != nil {
		return transitionFailure(service, operationID, intent, PathREST, ClusterWriteData{
			RESTSendState: SendNotSent, Verification: VerifiedFailed,
		}, CategoryConflict, intent.operation+" was not sent because cluster topology changed", selectionError, baseEvidence)
	}
	if err := revalidateTransitionPlan(plan, selection); err != nil {
		return transitionFailure(service, operationID, intent, PathREST, transitionData(intent, selection),
			CategoryConflict, intent.operation+" was not sent because confirmed preconditions changed", err, baseEvidence)
	}
	data := transitionData(intent, selection)
	if err := ctx.Err(); err != nil {
		data.RESTSendState = SendNotSent
		data.Verification = VerifiedFailed
		evidence := Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: intent.operation + " canceled before REST write", Path: string(PathREST), SendState: SendNotSent}
		return transitionFailure(service, operationID, intent, PathREST, data, CategoryFailed,
			intent.operation+" was canceled before it was sent", err, append(baseEvidence, evidence))
	}

	payload := transitionRESTPayload(intent, selection)
	response, restSend, legacyEndpoint, restEvidence, callError := service.callTransitionREST(ctx, intent, selection, payload)
	baseEvidence = append(baseEvidence, restEvidence...)
	data.RESTSendState = restSend
	data.HTTPStatus = response.StatusCode
	data.LegacyEndpoint = legacyEndpoint

	if response.StatusCode == 200 || response.StatusCode == 202 {
		if response.StatusCode == 202 && intent.scheduledAt != nil {
			verified, revision, verificationEvidence := service.verifyTransitionFallback(ctx, intent, selection.leader)
			baseEvidence = append(baseEvidence, verificationEvidence...)
			data.DCSRevision = revision
			if !verified {
				return transitionUnknown(service, operationID, intent, PathREST, data,
					"Patroni accepted the scheduled switchover but DCS readback did not verify it", callError, baseEvidence)
			}
		}
		data.Verification = VerifiedSucceeded
		return transitionSuccess(service, operationID, intent, PathREST, data, baseEvidence)
	}
	if response.StatusCode != 0 {
		data.Verification = VerifiedFailed
		category, retryable := classifyHTTPStatus(response.StatusCode)
		return transitionFailureWithRetry(service, operationID, intent, PathREST, data, category, retryable,
			"Patroni rejected "+intent.operation, callError, baseEvidence)
	}
	if err := ctx.Err(); err != nil {
		data.Verification = Unverified
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathREST, data,
				intent.operation+" may have been sent before cancellation; verify before retrying", err, baseEvidence)
		}
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathREST, data, CategoryFailed,
			intent.operation+" was canceled before fallback", err, baseEvidence)
	}
	return service.executeTransitionFallback(ctx, operationID, intent, selection, data, callError, baseEvidence)
}

func normalizeFailoverRequest(request FailoverRequest) (transitionIntent, error) {
	intent := transitionIntent{
		operation: "failover",
		target:    request.Target.Normalize(),
		candidate: strings.TrimSpace(request.Candidate),
		force:     request.Force,
		citus:     request.Citus,
	}
	if err := validateTransitionTarget(intent.target, intent.candidate); err != nil {
		return intent, err
	}
	if intent.citus && intent.target.Group == nil {
		return intent, errors.New("citus failover requires an explicit group")
	}
	return intent, nil
}

func normalizeSwitchoverRequest(request SwitchoverRequest) (transitionIntent, error) {
	intent := transitionIntent{
		operation:   "switchover",
		target:      request.Target.Normalize(),
		leader:      strings.TrimSpace(request.Leader),
		candidate:   strings.TrimSpace(request.Candidate),
		scheduledAt: request.ScheduledAt,
		force:       request.Force,
		citus:       request.Citus,
	}
	if err := validateTransitionTarget(intent.target, intent.leader, intent.candidate); err != nil {
		return intent, err
	}
	if intent.scheduledAt != nil && intent.scheduledAt.IsZero() {
		return intent, errors.New("scheduled switchover time is zero")
	}
	if intent.citus && intent.target.Group == nil {
		return intent, errors.New("citus switchover requires an explicit group")
	}
	return intent, nil
}

func validateTransitionTarget(target model.Target, names ...string) error {
	if target.Member != "" {
		return errors.New("cluster transition target must not contain a member")
	}
	if err := target.Validate(true); err != nil {
		return err
	}
	for _, name := range names {
		if name == "" {
			continue
		}
		member := target
		member.Member = name
		if err := member.Validate(true); err != nil {
			return err
		}
	}
	return nil
}

func resolveTransition(snapshot dcs.Snapshot, intent transitionIntent) (transitionSelection, Category, error) {
	selection := transitionSelection{
		candidate: intent.candidate,
		paused:    boolConfig(snapshot.Cluster.Config["pause"]),
	}
	if snapshot.Cluster.Leader != nil {
		selection.leader = snapshot.Cluster.Leader.Name
	}
	if snapshot.Cluster.Failover != nil {
		selection.failoverVersion = snapshot.Cluster.Failover.ModRevision
	}
	if intent.operation == "switchover" {
		if selection.leader == "" {
			return selection, CategoryNotFound, errors.New("switchover requires a current leader")
		}
		if intent.leader == "" {
			if !intent.force {
				return selection, CategoryUsage, errors.New("switchover requires an explicitly selected leader before confirmation")
			}
		} else if intent.leader != selection.leader {
			return selection, CategoryConflict, fmt.Errorf("member %s is not the current leader", intent.leader)
		}
	}

	candidates := make([]dcs.Member, 0, len(snapshot.Cluster.Members))
	for _, member := range snapshot.Cluster.Members {
		if member.Name == selection.leader || boolConfig(member.Data.Tags["nofailover"]) {
			continue
		}
		candidates = append(candidates, member)
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].Name < candidates[right].Name })
	selection.candidateNames = make([]string, len(candidates))
	for index := range candidates {
		selection.candidateNames[index] = candidates[index].Name
	}
	if len(candidates) == 0 {
		return selection, CategoryNotFound, fmt.Errorf("no candidates found to %s to", intent.operation)
	}
	if intent.candidate == "" {
		if intent.operation == "failover" {
			return selection, CategoryUsage, errors.New("failover requires a specific candidate")
		}
		if !intent.force {
			return selection, CategoryUsage, errors.New("switchover requires an explicitly selected candidate before confirmation")
		}
	} else {
		if intent.candidate == selection.leader {
			return selection, CategoryConflict, fmt.Errorf("member %s is already the leader", intent.candidate)
		}
		for index := range candidates {
			if candidates[index].Name == intent.candidate {
				candidate := candidates[index]
				selection.candidateMember = &candidate
				break
			}
		}
		if selection.candidateMember == nil {
			return selection, CategoryNotFound, fmt.Errorf("member %s does not exist or is tagged nofailover", intent.candidate)
		}
	}
	if intent.operation == "switchover" && intent.scheduledAt != nil && selection.paused {
		return selection, CategoryConflict, errors.New("cannot schedule switchover while the cluster is paused")
	}
	selection.syncEligible = syncCandidateEligible(snapshot.Cluster, intent.candidate)
	memberName := selection.leader
	if memberName == "" {
		memberName = intent.candidate
	}
	for index := range snapshot.Cluster.Members {
		if snapshot.Cluster.Members[index].Name == memberName {
			member := snapshot.Cluster.Members[index]
			selection.restMember = &member
			break
		}
	}
	return selection, "", nil
}

func syncCandidateEligible(cluster dcs.ClusterState, candidate string) bool {
	if candidate == "" || !synchronousMode(cluster.Config) || cluster.Sync.Leader == "" {
		return true
	}
	if strings.EqualFold(cluster.Sync.Leader, candidate) {
		return true
	}
	for _, standby := range cluster.Sync.Standbys {
		if strings.EqualFold(standby, candidate) {
			return true
		}
	}
	return false
}

func transitionSummary(intent transitionIntent, selection transitionSelection) string {
	leader := selection.leader
	if intent.operation == "failover" {
		leader = ""
	}
	summary := intent.operation + " cluster"
	if leader != "" {
		summary += " from " + leader
	}
	if intent.candidate != "" {
		summary += " to " + intent.candidate
	} else {
		summary += " to a Patroni-selected eligible candidate"
	}
	if intent.scheduledAt != nil {
		summary += " at " + intent.scheduledAt.Format(time.RFC3339Nano)
	}
	return summary
}

func transitionPreconditions(revision int64, intent transitionIntent, selection transitionSelection) []Precondition {
	return []Precondition{
		{Field: "dcs.revision", Expected: strconv.FormatInt(revision, 10), Source: EvidenceDCS},
		{Field: "leader.name", Expected: selection.leader, Source: EvidenceDCS},
		{Field: "candidate.name", Expected: intent.candidate, Source: EvidenceControl},
		{Field: "candidate.options", Expected: strings.Join(selection.candidateNames, ","), Source: EvidenceDCS},
		{Field: "candidate.syncEligible", Expected: strconv.FormatBool(selection.syncEligible), Source: EvidenceDCS},
		{Field: "cluster.paused", Expected: strconv.FormatBool(selection.paused), Source: EvidenceDCS},
		{Field: "failover.modRevision", Expected: strconv.FormatInt(selection.failoverVersion, 10), Source: EvidenceDCS},
		{Field: "request.leader", Expected: intent.leader, Source: EvidenceControl},
		{Field: "request.schedule", Expected: transitionSchedule(intent), Source: EvidenceControl},
		{Field: "request.force", Expected: strconv.FormatBool(intent.force), Source: EvidenceControl},
		{Field: "request.citus", Expected: strconv.FormatBool(intent.citus), Source: EvidenceControl},
		{Field: "fallback.path", Expected: string(PathRESTToDCS), Source: EvidenceControl},
	}
}

func transitionSchedule(intent transitionIntent) string {
	if intent.scheduledAt == nil {
		return ""
	}
	return formatPatroniTimestamp(*intent.scheduledAt)
}

func validateTransitionPlan(plan Plan, intent transitionIntent) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != intent.operation || plan.Path != PathREST || plan.Risk != RiskAvailability || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from cluster transition")
	}
	if plan.Target.Normalize().ClusterID() != intent.target.ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	expected := map[string]string{
		"candidate.name":   intent.candidate,
		"request.leader":   intent.leader,
		"request.schedule": transitionSchedule(intent),
		"request.force":    strconv.FormatBool(intent.force),
		"request.citus":    strconv.FormatBool(intent.citus),
		"fallback.path":    string(PathRESTToDCS),
	}
	for field, value := range expected {
		observed, ok := expectedPrecondition(plan, field)
		if !ok || observed != value {
			return fmt.Errorf("plan %s differs from request", field)
		}
	}
	return nil
}

func revalidateTransitionPlan(plan Plan, selection transitionSelection) error {
	expected := map[string]string{
		"leader.name":            selection.leader,
		"candidate.options":      strings.Join(selection.candidateNames, ","),
		"candidate.syncEligible": strconv.FormatBool(selection.syncEligible),
		"cluster.paused":         strconv.FormatBool(selection.paused),
		"failover.modRevision":   strconv.FormatInt(selection.failoverVersion, 10),
	}
	for field, value := range expected {
		planned, ok := expectedPrecondition(plan, field)
		if !ok || planned != value {
			return fmt.Errorf("confirmed %s changed", field)
		}
	}
	return nil
}

func transitionData(intent transitionIntent, selection transitionSelection) ClusterWriteData {
	leader := ""
	if intent.operation == "switchover" {
		leader = selection.leader
	}
	return ClusterWriteData{
		Leader: leader, Candidate: intent.candidate, ScheduledAt: transitionSchedule(intent),
		RESTSendState: SendNotSent, Verification: Unverified,
	}
}

func transitionRESTPayload(intent transitionIntent, selection transitionSelection) patroni.FailoverRequest {
	payload := patroni.FailoverRequest{Candidate: intent.candidate, ScheduledAt: transitionSchedule(intent)}
	if intent.operation == "switchover" {
		payload.Leader = selection.leader
	}
	return payload
}

func (service *Service) callTransitionREST(
	ctx context.Context,
	intent transitionIntent,
	selection transitionSelection,
	payload patroni.FailoverRequest,
) (patroni.Response[string], SendState, bool, []Evidence, error) {
	path := "/" + intent.operation
	if service.patroni == nil {
		err := errors.New("patroni REST client is unavailable")
		return patroni.Response[string]{}, SendNotSent, false, []Evidence{{
			Source: EvidencePatroni, ObservedAt: service.now(), Summary: intent.operation + " REST client is unavailable; command fallback is required",
			Path: path, SendState: SendNotSent,
		}}, err
	}
	if selection.restMember == nil {
		err := errors.New("rest target member is absent from DCS")
		return patroni.Response[string]{}, SendNotSent, false, []Evidence{{
			Source: EvidencePatroni, ObservedAt: service.now(), Summary: intent.operation + " REST target is unavailable; command fallback is required",
			Path: path, SendState: SendNotSent,
		}}, err
	}
	baseURL, err := patroniBaseURL(selection.restMember.Data.APIURL)
	if err != nil {
		return patroni.Response[string]{}, SendNotSent, false, []Evidence{{
			Source: EvidencePatroni, ObservedAt: service.now(), Summary: intent.operation + " REST target has no usable address; command fallback is required",
			Path: path, SendState: SendNotSent,
		}}, err
	}
	var response patroni.Response[string]
	if intent.operation == "failover" {
		response, err = service.patroni.PostFailover(ctx, baseURL, payload)
	} else {
		response, err = service.patroni.PostSwitchover(ctx, baseURL, payload)
	}
	evidence := []Evidence{transitionRESTEvidence(service, intent.operation, path, response, err)}
	send := patroniSendState(err, response.StatusCode)
	if intent.operation == "switchover" && response.StatusCode == 501 && strings.Contains(response.Data, legacySwitchoverResponse) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return response, send, false, evidence, ctxErr
		}
		legacyResponse, legacyError := service.patroni.PostFailover(ctx, baseURL, payload)
		evidence = append(evidence, Evidence{
			Source: EvidenceControl, ObservedAt: service.now(), Summary: "Patroni 501 selected the source-compatible legacy /failover endpoint",
			Path: "/switchover->/failover", SendState: SendNotSent,
		})
		evidence = append(evidence, transitionRESTEvidence(service, intent.operation, "/failover", legacyResponse, legacyError))
		return legacyResponse, patroniSendState(legacyError, legacyResponse.StatusCode), true, evidence, legacyError
	}
	return response, send, false, evidence, err
}

func transitionRESTEvidence(service *Service, operation, path string, response patroni.Response[string], err error) Evidence {
	send := patroniSendState(err, response.StatusCode)
	summary := "Patroni " + operation + " transport ended without an HTTP response"
	if response.StatusCode == 200 || response.StatusCode == 202 {
		summary = "Patroni accepted " + operation
	} else if response.StatusCode != 0 {
		summary = "Patroni rejected " + operation
	}
	return Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Summary: summary, Path: path, SendState: send}
}

func (service *Service) executeTransitionFallback(
	ctx context.Context,
	operationID string,
	intent transitionIntent,
	planned transitionSelection,
	data ClusterWriteData,
	restError error,
	evidence []Evidence,
) Result[ClusterWriteData] {
	fresh, err := service.snapshots.Snapshot(ctx, intent.target)
	if err != nil {
		dcsEvidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: intent.operation + " fallback could not obtain a fresh DCS snapshot", Path: string(PathDCS), SendState: SendNotSent}
		evidence = append(evidence, dcsEvidence)
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
				intent.operation+" REST outcome is ambiguous and DCS fallback could not be verified", err, evidence)
		}
		category, retryable := classifyReadError(err)
		data.Verification = VerifiedFailed
		return transitionFailureWithRetry(service, operationID, intent, PathRESTToDCS, data, category, retryable,
			intent.operation+" fallback could not read DCS", err, evidence)
	}
	evidence = append(evidence, snapshotEvidence(service, fresh, intent.operation+" fallback obtained a fresh DCS snapshot"))
	if transitionLeaderCompleted(fresh.Cluster, intent, planned.leader) {
		data.Verification = VerifiedSucceeded
		data.DCSRevision = fresh.Revision
		return transitionSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	if err := validateFallbackTransition(fresh.Cluster, intent, planned); err != nil {
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
				intent.operation+" may have been sent but fallback preconditions changed", err, evidence)
		}
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict,
			intent.operation+" fallback aborted because cluster topology changed", err, evidence)
	}
	if service.failoverDCS == nil {
		err := errors.New("dcs failover capability is unavailable")
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
				intent.operation+" may have been sent and DCS fallback is unavailable", err, evidence)
		}
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConfig,
			intent.operation+" fallback requires a DCS failover capability", err, evidence)
	}
	if err := ctx.Err(); err != nil {
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
				intent.operation+" may have been sent; cancellation prevented fallback", err, evidence)
		}
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathRESTToDCS, data, CategoryFailed,
			intent.operation+" fallback was canceled before DCS write", err, evidence)
	}
	payload, err := transitionDCSPayload(intent, planned)
	if err != nil {
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathRESTToDCS, data, CategoryInternal,
			intent.operation+" fallback payload construction failed", err, evidence)
	}
	expectedRevision := int64(0)
	if fresh.Cluster.Failover != nil {
		expectedRevision = fresh.Cluster.Failover.ModRevision
	}
	writeResult, writeError := service.failoverDCS.WriteFailover(ctx, intent.target, payload, &expectedRevision)
	data.DCSSendState = dcsSendState(writeError, writeResult.Applied)
	data.DCSRevision = writeResult.Revision
	evidence = append(evidence, Evidence{
		Source: EvidenceDCS, ObservedAt: service.now(), Summary: transitionDCSWriteSummary(writeResult, writeError),
		Revision: strconv.FormatInt(writeResult.Revision, 10), Path: string(PathDCS), SendState: data.DCSSendState,
	})
	verified, verificationRevision, verificationEvidence := service.verifyTransitionFallback(ctx, intent, planned.leader)
	evidence = append(evidence, verificationEvidence...)
	if verificationRevision > data.DCSRevision {
		data.DCSRevision = verificationRevision
	}
	if verified {
		data.Verification = VerifiedSucceeded
		return transitionSuccess(service, operationID, intent, PathRESTToDCS, data, evidence)
	}
	data.Verification = Unverified
	var conflict *dcs.ConflictError
	if errors.As(writeError, &conflict) {
		if data.RESTSendState == SendMaybeSent || data.RESTSendState == SendAccepted {
			return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
				intent.operation+" REST may have been sent and DCS fallback conflicted", writeError, evidence)
		}
		data.Verification = VerifiedFailed
		return transitionFailure(service, operationID, intent, PathRESTToDCS, data, CategoryConflict,
			intent.operation+" DCS fallback lost a compare-and-swap race", writeError, evidence)
	}
	if writeError != nil && data.DCSSendState == SendNotSent && data.RESTSendState == SendNotSent {
		category, retryable := classifyReadError(writeError)
		data.Verification = VerifiedFailed
		return transitionFailureWithRetry(service, operationID, intent, PathRESTToDCS, data, category, retryable,
			intent.operation+" was not sent through REST or DCS", writeError, evidence)
	}
	cause := writeError
	if cause == nil {
		cause = restError
	}
	return transitionUnknown(service, operationID, intent, PathRESTToDCS, data,
		intent.operation+" fallback was not verified; inspect DCS before retrying", cause, evidence)
}

func validateFallbackTransition(cluster dcs.ClusterState, intent transitionIntent, planned transitionSelection) error {
	leader := ""
	if cluster.Leader != nil {
		leader = cluster.Leader.Name
	}
	if leader != planned.leader {
		return fmt.Errorf("leader changed from %s to %s", planned.leader, leader)
	}
	if intent.scheduledAt != nil && boolConfig(cluster.Config["pause"]) {
		return errors.New("cluster became paused before scheduled switchover fallback")
	}
	if intent.candidate != "" {
		for _, member := range cluster.Members {
			if member.Name == intent.candidate && member.Name != leader && !boolConfig(member.Data.Tags["nofailover"]) {
				return nil
			}
		}
		return fmt.Errorf("candidate %s disappeared or became ineligible", intent.candidate)
	}
	current := make([]string, 0, len(cluster.Members))
	for _, member := range cluster.Members {
		if member.Name != leader && !boolConfig(member.Data.Tags["nofailover"]) {
			current = append(current, member.Name)
		}
	}
	sort.Strings(current)
	if strings.Join(current, ",") != strings.Join(planned.candidateNames, ",") {
		return errors.New("eligible candidate set changed")
	}
	return nil
}

func transitionDCSPayload(intent transitionIntent, planned transitionSelection) ([]byte, error) {
	value := make(map[string]string, 3)
	if intent.operation == "switchover" && planned.leader != "" {
		value["leader"] = planned.leader
	}
	if intent.candidate != "" {
		value["member"] = intent.candidate
	}
	if schedule := transitionSchedule(intent); schedule != "" {
		value["scheduled_at"] = schedule
	}
	return json.Marshal(value)
}

func dcsSendState(err error, applied bool) SendState {
	if err == nil && applied {
		return SendAccepted
	}
	var typed *dcs.Error
	if errors.As(err, &typed) {
		switch typed.Delivery {
		case dcs.DeliveryNotSent:
			return SendNotSent
		case dcs.DeliveryMaybeSent:
			return SendMaybeSent
		case dcs.DeliveryResponseReceived:
			return SendAccepted
		}
	}
	var conflict *dcs.ConflictError
	if errors.As(err, &conflict) {
		return SendNotSent
	}
	if err == nil {
		return SendMaybeSent
	}
	return SendMaybeSent
}

func transitionDCSWriteSummary(result dcs.WriteResult, err error) string {
	if err == nil && result.Applied {
		return "DCS failover fallback CAS was applied"
	}
	var conflict *dcs.ConflictError
	if errors.As(err, &conflict) {
		return "DCS failover fallback CAS conflicted"
	}
	return "DCS failover fallback ended without a verified apply"
}

func (service *Service) verifyTransitionFallback(
	ctx context.Context,
	intent transitionIntent,
	plannedLeader string,
) (bool, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	var lastRevision int64
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: intent.operation + " fallback verification canceled", Path: string(PathDCS)})
				return false, lastRevision, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, intent.target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: intent.operation + " fallback readback failed", Path: string(PathDCS)})
			continue
		}
		lastRevision = snapshot.Revision
		matched := transitionCompleted(snapshot.Cluster, intent, plannedLeader)
		summary := intent.operation + " fallback state does not yet match the request"
		if matched {
			summary = intent.operation + " fallback state matches the request"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary,
			Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if matched {
			return true, lastRevision, evidence
		}
	}
	return false, lastRevision, evidence
}

func transitionCompleted(cluster dcs.ClusterState, intent transitionIntent, plannedLeader string) bool {
	if transitionLeaderCompleted(cluster, intent, plannedLeader) {
		return true
	}
	failover := cluster.Failover
	if failover == nil {
		return false
	}
	expectedLeader := ""
	if intent.operation == "switchover" {
		expectedLeader = plannedLeader
	}
	if failover.Leader != expectedLeader || failover.Candidate != intent.candidate {
		return false
	}
	expectedSchedule := transitionSchedule(intent)
	if expectedSchedule == "" {
		return failover.ScheduledAt == ""
	}
	if failover.ScheduledAt == expectedSchedule {
		return true
	}
	expectedTime, expectedError := time.Parse(time.RFC3339Nano, expectedSchedule)
	observedTime, observedError := time.Parse(time.RFC3339Nano, failover.ScheduledAt)
	return expectedError == nil && observedError == nil && expectedTime.Equal(observedTime)
}

func transitionLeaderCompleted(cluster dcs.ClusterState, intent transitionIntent, plannedLeader string) bool {
	leader := ""
	if cluster.Leader != nil {
		leader = cluster.Leader.Name
	}
	if intent.scheduledAt == nil {
		if intent.candidate != "" && leader == intent.candidate {
			return true
		}
		if intent.operation == "switchover" && intent.candidate == "" && leader != "" && leader != plannedLeader {
			return true
		}
	}
	return false
}

func transitionSuccess(
	service *Service,
	operationID string,
	intent transitionIntent,
	path Path,
	data ClusterWriteData,
	evidence []Evidence,
) Result[ClusterWriteData] {
	return Result[ClusterWriteData]{
		OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: path, Data: data, Evidence: evidence,
	}
}

func transitionFailure(
	service *Service,
	operationID string,
	intent transitionIntent,
	path Path,
	data ClusterWriteData,
	category Category,
	message string,
	cause error,
	evidence []Evidence,
) Result[ClusterWriteData] {
	return transitionFailureWithRetry(service, operationID, intent, path, data, category, false, message, cause, evidence)
}

func transitionFailureWithRetry(
	service *Service,
	operationID string,
	intent transitionIntent,
	path Path,
	data ClusterWriteData,
	category Category,
	retryable bool,
	message string,
	cause error,
	evidence []Evidence,
) Result[ClusterWriteData] {
	errorValue := NewError(category, intent.operation, intent.target, retryable, message, cause, evidence...)
	return Result[ClusterWriteData]{
		OperationID: operationID, Outcome: Failed, Target: intent.target, Path: path, Data: data, Evidence: evidence, Error: errorValue,
	}
}

func transitionUnknown(
	service *Service,
	operationID string,
	intent transitionIntent,
	path Path,
	data ClusterWriteData,
	message string,
	cause error,
	evidence []Evidence,
) Result[ClusterWriteData] {
	data.Verification = Unverified
	errorValue := NewError(CategoryUnknown, intent.operation, intent.target, false, message, cause, evidence...)
	return Result[ClusterWriteData]{
		OperationID: operationID, Outcome: Unknown, Target: intent.target, Path: path, Data: data, Evidence: evidence, Error: errorValue,
	}
}
