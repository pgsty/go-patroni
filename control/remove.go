package control

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

const RemoveAcknowledgement = "Yes I am aware"

type RemoveRequest struct {
	Target model.Target `json:"target" yaml:"target"`
	Citus  bool         `json:"citus" yaml:"citus"`
}

func (request RemoveRequest) String() string {
	return fmt.Sprintf("control.RemoveRequest{target:%s,citus:%t}", request.Target.Normalize().ClusterID(), request.Citus)
}

func (request RemoveRequest) GoString() string { return request.String() }

// RemoveConfirmation contains the three exact patronictl confirmation values.
// A leader value is required only when the confirmed snapshot had a leader.
type RemoveConfirmation struct {
	ClusterName     string `json:"clusterName" yaml:"clusterName"`
	Acknowledgement string `json:"acknowledgement" yaml:"acknowledgement"`
	Leader          string `json:"leader,omitempty" yaml:"leader,omitempty"`
}

func (confirmation RemoveConfirmation) String() string {
	return fmt.Sprintf("control.RemoveConfirmation{clusterName:%q,acknowledgement:%q,leader:%q}",
		confirmation.ClusterName, confirmation.Acknowledgement, confirmation.Leader)
}

func (confirmation RemoveConfirmation) GoString() string { return confirmation.String() }

type RemoveData struct {
	ExpectedKeys  int          `json:"expectedKeys" yaml:"expectedKeys"`
	Deleted       int64        `json:"deleted" yaml:"deleted"`
	RemainingKeys []string     `json:"remainingKeys,omitempty" yaml:"remainingKeys,omitempty"`
	DCSSendState  SendState    `json:"dcsSendState" yaml:"dcsSendState"`
	Verification  Verification `json:"verification" yaml:"verification"`
	DCSRevision   int64        `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	Noop          bool         `json:"noop" yaml:"noop"`
}

func (data RemoveData) Validate() error {
	if data.ExpectedKeys < 0 || data.Deleted < 0 || data.DCSRevision < 0 {
		return errors.New("remove counts and revision must be non-negative")
	}
	if !data.DCSSendState.validOrEmpty() || data.DCSSendState == "" {
		return fmt.Errorf("remove DCS send state %q is invalid", data.DCSSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("remove verification %q is invalid", data.Verification)
	}
	if !sort.StringsAreSorted(data.RemainingKeys) {
		return errors.New("remove remaining keys must be deterministic")
	}
	if data.Noop && (data.DCSSendState != SendNotSent || data.Verification != VerifiedSucceeded) {
		return errors.New("remove no-op requires verified NOT_SENT success")
	}
	return nil
}

func (service *Service) PrepareRemove(ctx context.Context, request RemoveRequest) Result[Plan] {
	operationID := service.operationID()
	request, requestError := normalizeRemoveRequest(request)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove requires a context", nil)
	}
	if requestError != nil {
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove request is invalid", requestError)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, request.Target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, category, retryable, "remove cluster snapshot failed", err)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[Plan](service, operationID, "remove", request.Target, PathDCS, snapshot, versionError)
	}
	keys := removeInventory(snapshot)
	if len(keys) == 0 {
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, CategoryNotFound, false, "cluster DCS state does not exist", nil,
			snapshotEvidence(service, snapshot, "remove target lookup completed"))
	}
	leader := removeLeader(snapshot.Cluster)
	keysJSON, _ := json.Marshal(keys)
	binding, err := service.removePlanBinding(request, snapshot.Revision, leader, string(keysJSON))
	if err != nil {
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, CategoryInternal, false, "remove Plan binding failed", err)
	}
	plan := Plan{
		OperationID: operationID, Operation: "remove", Target: request.Target, Path: PathDCS,
		Risk: RiskDestructive, RetrySafety: UnsafeAfterSend,
		Summary: fmt.Sprintf("delete %d keys under exact cluster DCS root %s", len(keys), request.Target.ClusterID()),
		Preconditions: []Precondition{
			{Field: "dcs.revision", Expected: strconv.FormatInt(snapshot.Revision, 10), Source: EvidenceDCS},
			{Field: "remove.clusterName", Expected: request.Target.Scope, Source: EvidenceControl},
			{Field: "remove.acknowledgement", Expected: RemoveAcknowledgement, Source: EvidenceControl},
			{Field: "remove.leader", Expected: leader, Source: EvidenceDCS},
			{Field: "remove.keys", Expected: string(keysJSON), Source: EvidenceDCS},
			{Field: "request.citus", Expected: strconv.FormatBool(request.Citus), Source: EvidenceControl},
			{Field: "remove.binding", Expected: binding, Source: EvidenceControl},
		},
	}
	if err := plan.Validate(); err != nil {
		return failedRead[Plan](service, operationID, "remove", request.Target, PathDCS, CategoryInternal, false, "remove Plan construction failed", err,
			snapshotEvidence(service, snapshot, "remove inventory completed"))
	}
	return Result[Plan]{OperationID: operationID, Outcome: Succeeded, Target: request.Target, Path: PathDCS, Data: plan,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, "remove Plan built from exact DCS key inventory")}}
}

func (service *Service) ExecuteRemove(ctx context.Context, request RemoveRequest, confirmation RemoveConfirmation, plan Plan) Result[RemoveData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	request, requestError := normalizeRemoveRequest(request)
	if !validContext(ctx) {
		return failedRead[RemoveData](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove requires a context", nil)
	}
	if requestError != nil {
		return failedRead[RemoveData](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove request is invalid", requestError)
	}
	if err := service.validateRemovePlan(plan, request); err != nil {
		return failedRead[RemoveData](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove Plan does not match the request", err)
	}
	if err := validateRemoveConfirmation(plan, confirmation); err != nil {
		return failedRead[RemoveData](service, operationID, "remove", request.Target, PathDCS, CategoryUsage, false, "remove confirmation does not match the Plan", err)
	}
	plannedKeys, _ := removePlanKeys(plan)
	data := RemoveData{ExpectedKeys: len(plannedKeys), DCSSendState: SendNotSent, Verification: Unverified}
	snapshot, err := service.snapshots.Snapshot(ctx, request.Target)
	if err != nil {
		category, retryable := classifyReadError(err)
		data.Verification = VerifiedFailed
		return removeFailure(service, operationID, request.Target, data, category, retryable, "remove execution snapshot failed", err, nil)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[RemoveData](service, operationID, "remove", request.Target, PathDCS, snapshot, versionError)
	}
	evidence := []Evidence{snapshotEvidence(service, snapshot, "remove key inventory revalidated before delete")}
	currentKeys := removeInventory(snapshot)
	if len(currentKeys) == 0 {
		data.Noop = true
		data.Verification = VerifiedSucceeded
		data.DCSRevision = snapshot.Revision
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "cluster DCS root is already absent", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent})
		return removeSuccess(operationID, request.Target, data, evidence)
	}
	plannedLeader, _ := expectedPrecondition(plan, "remove.leader")
	if !reflectStringSlices(currentKeys, plannedKeys) || removeLeader(snapshot.Cluster) != plannedLeader {
		data.RemainingKeys = currentKeys
		data.Verification = VerifiedFailed
		return removeFailure(service, operationID, request.Target, data, CategoryConflict, false, "remove was not sent because the confirmed cluster state changed", nil, evidence)
	}
	if err := ctx.Err(); err != nil {
		data.RemainingKeys = currentKeys
		data.Verification = VerifiedFailed
		return removeFailure(service, operationID, request.Target, data, CategoryFailed, false, "remove was canceled before DCS delete", err, evidence)
	}
	if service.removerDCS == nil {
		data.RemainingKeys = currentKeys
		data.Verification = VerifiedFailed
		return removeFailure(service, operationID, request.Target, data, CategoryConfig, false, "remove requires a DCS cluster remover capability", nil, evidence)
	}
	deleteResult, deleteError := service.removerDCS.DeleteCluster(ctx, request.Target)
	data.Deleted = deleteResult.Deleted
	data.DCSRevision = deleteResult.Revision
	data.DCSSendState = removeSendState(deleteError)
	evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: removeDeleteSummary(deleteResult, deleteError),
		Revision: strconv.FormatInt(deleteResult.Revision, 10), Path: string(PathDCS), SendState: data.DCSSendState})
	verified, remaining, revision, verificationEvidence := service.verifyRemove(ctx, request.Target)
	evidence = append(evidence, verificationEvidence...)
	data.RemainingKeys = remaining
	if revision > data.DCSRevision {
		data.DCSRevision = revision
	}
	if verified {
		data.Verification = VerifiedSucceeded
		if data.DCSSendState == SendNotSent {
			data.Noop = true
		}
		return removeSuccess(operationID, request.Target, data, evidence)
	}
	if data.DCSSendState == SendNotSent {
		category, retryable := classifyReadError(deleteError)
		data.Verification = VerifiedFailed
		return removeFailure(service, operationID, request.Target, data, category, retryable, "cluster DCS delete was definitely not sent", deleteError, evidence)
	}
	cause := deleteError
	if ctx.Err() != nil {
		cause = errors.Join(deleteError, ctx.Err())
	}
	return removeUnknown(service, operationID, request.Target, data, "cluster DCS deletion was not verified; inspect the exact root before retrying", cause, evidence)
}

func normalizeRemoveRequest(request RemoveRequest) (RemoveRequest, error) {
	request.Target = request.Target.Normalize()
	if request.Target.Member != "" {
		return request, errors.New("remove target must be a cluster")
	}
	if err := request.Target.Validate(true); err != nil {
		return request, err
	}
	if request.Citus && request.Target.Group == nil {
		return request, errors.New("Citus remove requires an explicit group")
	}
	return request, nil
}

func removeInventory(snapshot dcs.Snapshot) []string {
	keys := make([]string, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		keys = append(keys, entry.RelativePath)
	}
	sort.Strings(keys)
	return keys
}

func removeLeader(cluster dcs.ClusterState) string {
	if cluster.Leader == nil {
		return ""
	}
	return cluster.Leader.Name
}

func (service *Service) removePlanBinding(request RemoveRequest, revision int64, leader, keys string) (string, error) {
	material := struct {
		Target          string `json:"target"`
		Revision        int64  `json:"revision"`
		ClusterName     string `json:"clusterName"`
		Acknowledgement string `json:"acknowledgement"`
		Leader          string `json:"leader"`
		Keys            string `json:"keys"`
		Citus           bool   `json:"citus"`
	}{request.Target.ClusterID(), revision, request.Target.Scope, RemoveAcknowledgement, leader, keys, request.Citus}
	return service.planToken("remove/plan", material)
}

func (service *Service) validateRemovePlan(plan Plan, request RemoveRequest) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "remove" || plan.Path != PathDCS || plan.Risk != RiskDestructive || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("Plan operation contract differs from remove")
	}
	if plan.Target.Normalize().ClusterID() != request.Target.ClusterID() {
		return errors.New("Plan cluster differs from request")
	}
	clusterName, clusterOK := expectedPrecondition(plan, "remove.clusterName")
	acknowledgement, acknowledgementOK := expectedPrecondition(plan, "remove.acknowledgement")
	leader, leaderOK := expectedPrecondition(plan, "remove.leader")
	citus, citusOK := expectedPrecondition(plan, "request.citus")
	if !clusterOK || clusterName != request.Target.Scope || !acknowledgementOK || acknowledgement != RemoveAcknowledgement ||
		!leaderOK || !citusOK || citus != strconv.FormatBool(request.Citus) {
		return errors.New("Plan confirmation contract differs from remove request")
	}
	keys, err := removePlanKeys(plan)
	if err != nil || len(keys) == 0 {
		return errors.New("Plan remove key inventory is invalid")
	}
	keysJSON, _ := json.Marshal(keys)
	revisionText, revisionOK := expectedPrecondition(plan, "dcs.revision")
	revision, revisionError := strconv.ParseInt(revisionText, 10, 64)
	if !revisionOK || revisionError != nil || revision <= 0 {
		return errors.New("Plan DCS revision is invalid")
	}
	binding, err := service.removePlanBinding(request, revision, leader, string(keysJSON))
	if err != nil {
		return err
	}
	plannedBinding, ok := expectedPrecondition(plan, "remove.binding")
	if !ok || len(plannedBinding) != sha256.Size*2 || !hmac.Equal([]byte(plannedBinding), []byte(binding)) {
		return errors.New("Plan remove binding is invalid")
	}
	return nil
}

func removePlanKeys(plan Plan) ([]string, error) {
	value, ok := expectedPrecondition(plan, "remove.keys")
	if !ok {
		return nil, errors.New("Plan remove keys are missing")
	}
	var keys []string
	if err := json.Unmarshal([]byte(value), &keys); err != nil || !sort.StringsAreSorted(keys) {
		return nil, errors.New("Plan remove keys are invalid")
	}
	for index, key := range keys {
		if strings.TrimSpace(key) == "" || (index > 0 && keys[index-1] == key) {
			return nil, errors.New("Plan remove keys are empty or duplicated")
		}
	}
	return keys, nil
}

func validateRemoveConfirmation(plan Plan, confirmation RemoveConfirmation) error {
	clusterName, _ := expectedPrecondition(plan, "remove.clusterName")
	acknowledgement, _ := expectedPrecondition(plan, "remove.acknowledgement")
	leader, _ := expectedPrecondition(plan, "remove.leader")
	if confirmation.ClusterName != clusterName {
		return errors.New("cluster names specified do not match")
	}
	if confirmation.Acknowledgement != acknowledgement {
		return fmt.Errorf("confirmation must exactly equal %q", acknowledgement)
	}
	if confirmation.Leader != leader {
		return errors.New("confirmation does not specify the current leader")
	}
	return nil
}

func removeSendState(err error) SendState {
	if err == nil {
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
	return SendMaybeSent
}

func removeDeleteSummary(result dcs.RemoveResult, err error) string {
	if err == nil {
		return fmt.Sprintf("DCS exact-root delete received a response for %d keys", result.Deleted)
	}
	if removeSendState(err) == SendNotSent {
		return "DCS exact-root delete was not sent"
	}
	return "DCS exact-root delete ended without a verified response"
}

func (service *Service) verifyRemove(ctx context.Context, target model.Target) (bool, []string, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	var remaining []string
	var revision int64
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "remove readback canceled", Path: string(PathDCS)})
				return false, remaining, revision, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "remove readback failed", Path: string(PathDCS)})
			continue
		}
		revision = snapshot.Revision
		remaining = removeInventory(snapshot)
		summary := "cluster DCS root still contains keys"
		if len(remaining) == 0 {
			summary = "cluster DCS root is empty"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary,
			Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if len(remaining) == 0 {
			return true, nil, revision, evidence
		}
	}
	return false, remaining, revision, evidence
}

func removeSuccess(operationID string, target model.Target, data RemoveData, evidence []Evidence) Result[RemoveData] {
	return Result[RemoveData]{OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS, Data: data, Evidence: evidence}
}

func removeFailure(service *Service, operationID string, target model.Target, data RemoveData, category Category, retryable bool, message string, cause error, evidence []Evidence) Result[RemoveData] {
	data.Verification = VerifiedFailed
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: EvidenceDCS, ObservedAt: service.now(), Summary: message, Path: string(PathDCS), SendState: data.DCSSendState}}
	}
	return Result[RemoveData]{OperationID: operationID, Outcome: Failed, Target: target, Path: PathDCS, Data: data, Evidence: evidence,
		Error: NewError(category, "remove", target, retryable, message, cause, evidence...)}
}

func removeUnknown(service *Service, operationID string, target model.Target, data RemoveData, message string, cause error, evidence []Evidence) Result[RemoveData] {
	data.Verification = Unverified
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: EvidenceDCS, ObservedAt: service.now(), Summary: message, Path: string(PathDCS), SendState: data.DCSSendState}}
	}
	return Result[RemoveData]{OperationID: operationID, Outcome: Unknown, Target: target, Path: PathDCS, Data: data, Evidence: evidence,
		Error: NewError(CategoryUnknown, "remove", target, false, message, cause, evidence...)}
}
