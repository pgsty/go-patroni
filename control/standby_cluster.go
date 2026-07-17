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

const (
	primaryClusterRole = "primary"
	standbyLeaderRole  = "standby_leader"
)

// StandbyConfig is the Patroni standby_cluster payload accepted by
// demote-cluster. Values are deliberately excluded from default
// serialization and formatting because restore commands and endpoints can
// contain credentials. Adapters may collect and retain them only for the
// prepare/confirm/execute lifetime of one operation.
type StandbyConfig struct {
	Host            string `json:"-" yaml:"-"`
	Port            int    `json:"-" yaml:"-"`
	RestoreCommand  string `json:"-" yaml:"-"`
	PrimarySlotName string `json:"-" yaml:"-"`
}

func (configuration StandbyConfig) String() string {
	return fmt.Sprintf("control.StandbyConfig{host:%t,port:%t,restoreCommand:%t,primarySlotName:%t,values:[REDACTED]}",
		configuration.Host != "", configuration.Port != 0, configuration.RestoreCommand != "", configuration.PrimarySlotName != "")
}

func (configuration StandbyConfig) GoString() string { return configuration.String() }

type DemoteClusterRequest struct {
	Target  model.Target  `json:"target" yaml:"target"`
	Standby StandbyConfig `json:"standby" yaml:"standby"`
	Force   bool          `json:"force" yaml:"force"`
	Citus   bool          `json:"citus" yaml:"citus"`
}

func (request DemoteClusterRequest) String() string {
	return fmt.Sprintf("control.DemoteClusterRequest{target:%s,standby:%s,force:%t,citus:%t}",
		request.Target.Normalize().ClusterID(), request.Standby.String(), request.Force, request.Citus)
}

func (request DemoteClusterRequest) GoString() string { return request.String() }

type PromoteClusterRequest struct {
	Target model.Target `json:"target" yaml:"target"`
	Force  bool         `json:"force" yaml:"force"`
	Citus  bool         `json:"citus" yaml:"citus"`
}

func (request PromoteClusterRequest) String() string {
	return fmt.Sprintf("control.PromoteClusterRequest{target:%s,force:%t,citus:%t}", request.Target.Normalize().ClusterID(), request.Force, request.Citus)
}

func (request PromoteClusterRequest) GoString() string { return request.String() }

// ClusterRoleData is the normalized result of a standby-cluster role
// transition. Raw configuration values, REST endpoints, and response bodies
// remain transport-only data.
type ClusterRoleData struct {
	PreviousLeader    string       `json:"previousLeader" yaml:"previousLeader"`
	Leader            string       `json:"leader,omitempty" yaml:"leader,omitempty"`
	DesiredRole       string       `json:"desiredRole" yaml:"desiredRole"`
	RESTSendState     SendState    `json:"restSendState" yaml:"restSendState"`
	Verification      Verification `json:"verification" yaml:"verification"`
	HTTPStatus        int          `json:"httpStatus,omitempty" yaml:"httpStatus,omitempty"`
	DCSRevision       int64        `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	PendingConditions []string     `json:"pendingConditions,omitempty" yaml:"pendingConditions,omitempty"`
	Noop              bool         `json:"noop" yaml:"noop"`
}

func (data ClusterRoleData) Validate() error {
	if data.DesiredRole != primaryClusterRole && data.DesiredRole != standbyLeaderRole {
		return fmt.Errorf("standby-cluster desired role %q is invalid", data.DesiredRole)
	}
	if !data.RESTSendState.validOrEmpty() || data.RESTSendState == "" {
		return fmt.Errorf("standby-cluster REST send state %q is invalid", data.RESTSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("standby-cluster verification %q is invalid", data.Verification)
	}
	if data.HTTPStatus < 0 || data.DCSRevision < 0 {
		return errors.New("standby-cluster status and revision must be non-negative")
	}
	if !sort.StringsAreSorted(data.PendingConditions) {
		return errors.New("standby-cluster pending conditions must be deterministic")
	}
	if data.Noop && (data.RESTSendState != SendNotSent || data.Verification != VerifiedSucceeded) {
		return errors.New("standby-cluster no-op requires verified NOT_SENT success")
	}
	return nil
}

type standbyClusterIntent struct {
	operation string
	target    model.Target
	standby   *StandbyConfig
	force     bool
	citus     bool
	desired   string
}

type standbyClusterSelection struct {
	leader  dcs.Member
	baseURL string
}

func (service *Service) PrepareDemoteCluster(ctx context.Context, request DemoteClusterRequest) Result[Plan] {
	intent, err := normalizeDemoteClusterRequest(request)
	return service.prepareStandbyCluster(ctx, intent, err)
}

func (service *Service) ExecuteDemoteCluster(ctx context.Context, request DemoteClusterRequest, plan Plan) Result[ClusterRoleData] {
	intent, err := normalizeDemoteClusterRequest(request)
	return service.executeStandbyCluster(ctx, intent, err, plan)
}

func (service *Service) PreparePromoteCluster(ctx context.Context, request PromoteClusterRequest) Result[Plan] {
	intent, err := normalizePromoteClusterRequest(request)
	return service.prepareStandbyCluster(ctx, intent, err)
}

func (service *Service) ExecutePromoteCluster(ctx context.Context, request PromoteClusterRequest, plan Plan) Result[ClusterRoleData] {
	intent, err := normalizePromoteClusterRequest(request)
	return service.executeStandbyCluster(ctx, intent, err, plan)
}

func normalizeDemoteClusterRequest(request DemoteClusterRequest) (standbyClusterIntent, error) {
	configuration := request.Standby
	intent := standbyClusterIntent{
		operation: "demote-cluster", target: request.Target.Normalize(), standby: &configuration,
		force: request.Force, citus: request.Citus, desired: standbyLeaderRole,
	}
	if err := validateStandbyClusterTarget(intent.target); err != nil {
		return intent, err
	}
	if request.Citus && intent.target.Group != nil {
		return intent, errors.New("demote-cluster does not accept a Citus group; coordinator group 0 is selected")
	}
	// This is deliberately Patroni-source compatible: a primary slot alone is
	// insufficient, while any non-empty host, non-zero port, or restore command
	// makes the standby configuration actionable.
	if configuration.Host == "" && configuration.Port == 0 && configuration.RestoreCommand == "" {
		return intent, errors.New("at least host, port, or restore command is required")
	}
	return intent, nil
}

func normalizePromoteClusterRequest(request PromoteClusterRequest) (standbyClusterIntent, error) {
	intent := standbyClusterIntent{
		operation: "promote-cluster", target: request.Target.Normalize(), force: request.Force, citus: request.Citus, desired: primaryClusterRole,
	}
	if err := validateStandbyClusterTarget(intent.target); err != nil {
		return intent, err
	}
	if request.Citus && intent.target.Group != nil {
		return intent, errors.New("promote-cluster does not accept a Citus group; coordinator group 0 is selected")
	}
	return intent, nil
}

func validateStandbyClusterTarget(target model.Target) error {
	if target.Member != "" {
		return errors.New("standby-cluster transition target must be a cluster")
	}
	return target.Validate(true)
}

func (service *Service) prepareStandbyCluster(ctx context.Context, intent standbyClusterIntent, requestError error) Result[Plan] {
	operationID := service.operationID()
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" request is invalid", requestError)
	}
	snapshots, evidence, failure := service.standbyClusterSnapshots(ctx, intent)
	if failure != nil {
		return failedReadWithEvidence[Plan](service, operationID, intent.operation, intent.target, PathREST,
			failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	snapshot := snapshots[0]
	selection, category, selectionError := resolveStandbyClusterSelection(snapshot)
	if selectionError != nil {
		return failedReadWithEvidence[Plan](service, operationID, intent.operation, intent.target, PathREST, category, false,
			intent.operation+" leader selection failed", selectionError,
			append(evidence, snapshotEvidence(service, snapshot, intent.operation+" leader selection completed")))
	}
	if selection.leader.Data.Role == intent.desired {
		return failedReadWithEvidence[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryConflict, false,
			"cluster is already in the required state", nil,
			append(evidence, snapshotEvidence(service, snapshot, intent.operation+" observed current cluster role")))
	}
	requestBinding, err := service.standbyClusterRequestBinding(intent)
	if err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" request binding failed", err)
	}
	stateBinding, err := service.standbyClusterStateBinding(intent, selection)
	if err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" state binding failed", err)
	}
	endpointBinding, err := service.standbyClusterEndpointBinding(selection.baseURL)
	if err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" endpoint binding failed", err)
	}
	planBinding, err := service.standbyClusterPlanBinding(
		intent, snapshot.Revision, selection.leader.Name, selection.leader.Data.Role, selection.leader.Data.State,
		requestBinding, stateBinding, endpointBinding,
	)
	if err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" Plan binding failed", err)
	}
	leaderTarget := snapshot.Target.Normalize()
	leaderTarget.Member = selection.leader.Name
	plan := Plan{
		OperationID: operationID, Operation: intent.operation, Target: intent.target, Targets: []model.Target{leaderTarget.Normalize()},
		Path: PathREST, Risk: RiskAvailability, RetrySafety: UnsafeAfterSend,
		Summary: fmt.Sprintf("%s cluster through current leader %s and wait for %s/running convergence",
			intent.operation, selection.leader.Name, intent.desired),
		Preconditions: []Precondition{
			{Field: "dcs.revision", Expected: strconv.FormatInt(snapshot.Revision, 10), Source: EvidenceDCS},
			{Field: "cluster.leader", Expected: selection.leader.Name, Source: EvidenceDCS},
			{Field: "leader.role", Expected: selection.leader.Data.Role, Source: EvidenceDCS},
			{Field: "leader.state", Expected: selection.leader.Data.State, Source: EvidenceDCS},
			{Field: "desired.role", Expected: intent.desired, Source: EvidenceControl},
			{Field: "request.force", Expected: strconv.FormatBool(intent.force), Source: EvidenceControl},
			{Field: "selector.citus", Expected: strconv.FormatBool(intent.citus), Source: EvidenceControl},
			{Field: "citus.groups", Expected: snapshotGroupIDs(snapshots), Source: EvidenceDCS},
			{Field: "request.binding", Expected: requestBinding, Source: EvidenceControl},
			{Field: "state.binding", Expected: stateBinding, Source: EvidenceControl},
			{Field: "endpoint.binding", Expected: endpointBinding, Source: EvidenceControl},
			{Field: "plan.binding", Expected: planBinding, Source: EvidenceControl},
		},
	}
	if err := plan.Validate(); err != nil {
		return failedRead[Plan](service, operationID, intent.operation, intent.target, PathREST, CategoryInternal, false,
			intent.operation+" Plan construction failed", err,
			snapshotEvidence(service, snapshot, intent.operation+" leader selection completed"))
	}
	return Result[Plan]{
		OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: PathREST, Data: plan,
		Evidence: evidence,
	}
}

func (service *Service) standbyClusterSnapshots(ctx context.Context, intent standbyClusterIntent) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	if intent.citus {
		return service.citusCoordinatorSnapshot(ctx, intent.operation, intent.target, false)
	}
	return service.operationSnapshots(ctx, intent.operation, intent.target, false, false)
}

func (service *Service) executeStandbyCluster(
	ctx context.Context,
	intent standbyClusterIntent,
	requestError error,
	plan Plan,
) Result[ClusterRoleData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	if !validContext(ctx) {
		return failedRead[ClusterRoleData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" requires a context", nil)
	}
	if requestError != nil {
		return failedRead[ClusterRoleData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" request is invalid", requestError)
	}
	if err := service.validateStandbyClusterPlan(plan, intent); err != nil {
		return failedRead[ClusterRoleData](service, operationID, intent.operation, intent.target, PathREST, CategoryUsage, false,
			intent.operation+" Plan does not match the request", err)
	}
	previousLeader, _ := expectedPrecondition(plan, "cluster.leader")
	data := ClusterRoleData{PreviousLeader: previousLeader, DesiredRole: intent.desired, RESTSendState: SendNotSent, Verification: Unverified}
	snapshots, evidence, failure := service.standbyClusterSnapshots(ctx, intent)
	if failure != nil {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, failure.category, failure.retryable,
			failure.message, failure.cause, evidence)
	}
	snapshot := snapshots[0]
	plannedGroups, _ := expectedPrecondition(plan, "citus.groups")
	if snapshotGroupIDs(snapshots) != plannedGroups || plan.Targets[0].Normalize().ClusterID() != snapshot.Target.Normalize().ClusterID() {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryConflict, false,
			intent.operation+" was not sent because the coordinator group changed", nil, evidence)
	}
	evidence = append(evidence, snapshotEvidence(service, snapshot, intent.operation+" leader state revalidated before execution"))
	selection, _, selectionError := resolveStandbyClusterSelection(snapshot)
	// A concurrently completed transition is a verified no-op even when the
	// newly selected leader has no usable REST endpoint; no write is needed.
	if selection.leader.Name != "" && selection.leader.Data.Role == intent.desired {
		pending := standbyClusterPending(snapshot.Cluster, intent, previousLeader)
		data.Leader = selection.leader.Name
		data.DCSRevision = snapshot.Revision
		data.PendingConditions = pending
		if len(pending) == 0 {
			data.Noop = true
			data.Verification = VerifiedSucceeded
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: intent.operation + " desired role already converged", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent})
			return standbyClusterSuccess(operationID, intent, data, evidence)
		}
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryConflict, false,
			intent.operation+" was not sent because another role transition is incomplete", nil, evidence)
	}
	if selectionError != nil {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryConflict, false,
			intent.operation+" was not sent because the confirmed leader changed", selectionError, evidence)
	}
	plannedState, _ := expectedPrecondition(plan, "state.binding")
	currentState, bindingError := service.standbyClusterStateBinding(intent, selection)
	plannedEndpoint, _ := expectedPrecondition(plan, "endpoint.binding")
	currentEndpoint, endpointError := service.standbyClusterEndpointBinding(selection.baseURL)
	if bindingError != nil || endpointError != nil || currentState != plannedState || currentEndpoint != plannedEndpoint {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryConflict, false,
			intent.operation+" was not sent because confirmed leader state changed", errors.Join(bindingError, endpointError), evidence)
	}
	if err := ctx.Err(); err != nil {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryFailed, false,
			intent.operation+" was canceled before REST write", err, evidence)
	}
	if service.patroni == nil {
		data.Verification = VerifiedFailed
		return standbyClusterFailure(service, operationID, intent, data, CategoryConfig, false,
			intent.operation+" requires a Patroni REST client", nil, evidence)
	}
	payload := standbyClusterPayload(intent)
	response, callError := service.patroni.PatchConfig(ctx, selection.baseURL, payload)
	data.RESTSendState = patroniSendState(callError, response.StatusCode)
	data.HTTPStatus = response.StatusCode
	restEvidence := Evidence{Source: EvidencePatroni, ObservedAt: service.now(), Path: "/config", SendState: data.RESTSendState}
	if response.StatusCode == 200 {
		restEvidence.Summary = "Patroni accepted " + intent.operation + " configuration"
	} else if response.StatusCode != 0 {
		restEvidence.Summary = "Patroni rejected " + intent.operation + " configuration"
	} else {
		restEvidence.Summary = intent.operation + " transport ended without an HTTP response"
	}
	evidence = append(evidence, restEvidence)

	if response.StatusCode != 0 && response.StatusCode != 200 {
		data.Verification = VerifiedFailed
		category, retryable := classifyHTTPStatus(response.StatusCode)
		return standbyClusterFailure(service, operationID, intent, data, category, retryable,
			"Patroni rejected "+intent.operation, callError, evidence)
	}
	if response.StatusCode == 0 && data.RESTSendState == SendNotSent {
		data.Verification = VerifiedFailed
		category, retryable := classifyReadError(callError)
		return standbyClusterFailure(service, operationID, intent, data, category, retryable,
			intent.operation+" was definitely not sent", callError, evidence)
	}

	verified, leader, pending, revision, verificationEvidence := service.verifyStandbyCluster(ctx, intent, snapshot.Target, previousLeader)
	evidence = append(evidence, verificationEvidence...)
	data.Leader, data.PendingConditions, data.DCSRevision = leader, pending, revision
	if verified {
		data.Verification = VerifiedSucceeded
		return standbyClusterSuccess(operationID, intent, data, evidence)
	}
	cause := callError
	if ctx.Err() != nil {
		cause = errors.Join(callError, ctx.Err())
	}
	return standbyClusterUnknown(service, operationID, intent, data,
		intent.operation+" was sent or may have been sent but DCS convergence was not verified; inspect cluster role before retrying", cause, evidence)
}

func resolveStandbyClusterSelection(snapshot dcs.Snapshot) (standbyClusterSelection, Category, error) {
	selection := standbyClusterSelection{}
	if snapshot.Cluster.Leader == nil || strings.TrimSpace(snapshot.Cluster.Leader.Name) == "" {
		return selection, CategoryNotFound, errors.New("cluster has no leader")
	}
	for _, member := range snapshot.Cluster.Members {
		if member.Name == snapshot.Cluster.Leader.Name {
			selection.leader = member
			baseURL, err := patroniBaseURL(member.Data.APIURL)
			if err != nil {
				return selection, CategoryConfig, errors.New("current leader has no usable Patroni API URL")
			}
			selection.baseURL = baseURL
			return selection, "", nil
		}
	}
	return selection, CategoryNotFound, errors.New("current leader member is absent")
}

func standbyClusterPayload(intent standbyClusterIntent) patroni.DynamicConfig {
	if intent.standby == nil {
		return patroni.DynamicConfig{"standby_cluster": nil}
	}
	configuration := make(map[string]any, 4)
	if intent.standby.Host != "" {
		configuration["host"] = intent.standby.Host
	}
	if intent.standby.Port != 0 {
		configuration["port"] = intent.standby.Port
	}
	if intent.standby.PrimarySlotName != "" {
		configuration["primary_slot_name"] = intent.standby.PrimarySlotName
	}
	if intent.standby.RestoreCommand != "" {
		configuration["restore_command"] = intent.standby.RestoreCommand
	}
	return patroni.DynamicConfig{"standby_cluster": configuration}
}

func (service *Service) standbyClusterRequestBinding(intent standbyClusterIntent) (string, error) {
	material := struct {
		Operation string                `json:"operation"`
		Target    string                `json:"target"`
		Force     bool                  `json:"force"`
		Citus     bool                  `json:"citus"`
		Payload   patroni.DynamicConfig `json:"payload"`
	}{intent.operation, intent.target.ClusterID(), intent.force, intent.citus, standbyClusterPayload(intent)}
	return service.planToken("standby-cluster/request", material)
}

func (service *Service) standbyClusterStateBinding(intent standbyClusterIntent, selection standbyClusterSelection) (string, error) {
	material := struct {
		Operation string `json:"operation"`
		Target    string `json:"target"`
		Leader    string `json:"leader"`
		Role      string `json:"role"`
		State     string `json:"state"`
	}{intent.operation, intent.target.ClusterID(), selection.leader.Name, selection.leader.Data.Role, selection.leader.Data.State}
	return service.planToken("standby-cluster/state", material)
}

func (service *Service) standbyClusterEndpointBinding(baseURL string) (string, error) {
	return service.planToken("standby-cluster/endpoint", struct {
		BaseURL string `json:"baseUrl"`
	}{baseURL})
}

func (service *Service) standbyClusterPlanBinding(
	intent standbyClusterIntent,
	revision int64,
	leader string,
	role string,
	state string,
	requestBinding string,
	stateBinding string,
	endpointBinding string,
) (string, error) {
	material := struct {
		Operation       string `json:"operation"`
		Target          string `json:"target"`
		Revision        int64  `json:"revision"`
		Leader          string `json:"leader"`
		Role            string `json:"role"`
		State           string `json:"state"`
		RequestBinding  string `json:"requestBinding"`
		StateBinding    string `json:"stateBinding"`
		EndpointBinding string `json:"endpointBinding"`
	}{intent.operation, intent.target.ClusterID(), revision, leader, role, state, requestBinding, stateBinding, endpointBinding}
	return service.planToken("standby-cluster/plan", material)
}

func (service *Service) validateStandbyClusterPlan(plan Plan, intent standbyClusterIntent) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != intent.operation || plan.Path != PathREST || plan.Risk != RiskAvailability || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("Plan operation contract differs from standby-cluster transition")
	}
	if plan.Target.Normalize().ClusterID() != intent.target.ClusterID() || len(plan.Targets) != 1 {
		return errors.New("Plan cluster or leader target differs from request")
	}
	required := map[string]int{
		"dcs.revision": 0, "cluster.leader": 0, "leader.role": 0, "leader.state": 0,
		"desired.role": 0, "request.force": 0, "selector.citus": 0, "citus.groups": 0, "request.binding": 0, "state.binding": 0,
		"endpoint.binding": 0, "plan.binding": 0,
	}
	for _, precondition := range plan.Preconditions {
		if _, ok := required[precondition.Field]; ok {
			required[precondition.Field]++
		}
	}
	for field, count := range required {
		if count != 1 {
			return fmt.Errorf("Plan requires exactly one %s precondition", field)
		}
	}
	leader, leaderOK := expectedPrecondition(plan, "cluster.leader")
	revisionText, revisionOK := expectedPrecondition(plan, "dcs.revision")
	role, roleOK := expectedPrecondition(plan, "leader.role")
	state, stateOK := expectedPrecondition(plan, "leader.state")
	desired, desiredOK := expectedPrecondition(plan, "desired.role")
	force, forceOK := expectedPrecondition(plan, "request.force")
	citus, citusOK := expectedPrecondition(plan, "selector.citus")
	_, groupsOK := expectedPrecondition(plan, "citus.groups")
	requestBinding, bindingOK := expectedPrecondition(plan, "request.binding")
	stateBinding, stateBindingOK := expectedPrecondition(plan, "state.binding")
	endpointBinding, endpointOK := expectedPrecondition(plan, "endpoint.binding")
	planBinding, planBindingOK := expectedPrecondition(plan, "plan.binding")
	revision, revisionError := strconv.ParseInt(revisionText, 10, 64)
	if !leaderOK || leader == "" || !revisionOK || revisionError != nil || revision < 0 || !roleOK || !stateOK ||
		!desiredOK || desired != intent.desired || !forceOK || force != strconv.FormatBool(intent.force) ||
		!citusOK || citus != strconv.FormatBool(intent.citus) || !groupsOK ||
		!bindingOK || requestBinding == "" || !stateBindingOK || stateBinding == "" || !endpointOK || endpointBinding == "" ||
		!planBindingOK || planBinding == "" || plan.Targets[0].Member != leader {
		return errors.New("Plan standby-cluster preconditions are incomplete")
	}
	expectedBinding, err := service.standbyClusterRequestBinding(intent)
	if err != nil {
		return err
	}
	if requestBinding != expectedBinding {
		return errors.New("Plan request binding differs from request")
	}
	plannedSelection := standbyClusterSelection{leader: dcs.Member{Name: leader, Data: dcs.MemberData{Role: role, State: state}}}
	expectedStateBinding, err := service.standbyClusterStateBinding(intent, plannedSelection)
	if err != nil || stateBinding != expectedStateBinding {
		return errors.New("Plan leader-state binding is invalid")
	}
	expectedPlanBinding, err := service.standbyClusterPlanBinding(
		intent, revision, leader, role, state, requestBinding, stateBinding, endpointBinding,
	)
	if err != nil || planBinding != expectedPlanBinding {
		return errors.New("Plan binding is invalid")
	}
	return nil
}

func (service *Service) verifyStandbyCluster(
	ctx context.Context,
	intent standbyClusterIntent,
	effectiveTarget model.Target,
	previousLeader string,
) (bool, string, []string, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.standbyVerificationAttempts)
	var leader string
	var pending []string
	var revision int64
	for attempt := 0; attempt < service.standbyVerificationAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: intent.operation + " convergence verification canceled", Path: string(PathDCS)})
			return false, leader, pending, revision, evidence
		}
		if attempt > 0 {
			if err := service.wait(ctx, service.standbyVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: intent.operation + " convergence verification canceled", Path: string(PathDCS)})
				return false, leader, pending, revision, evidence
			}
		}
		effectiveTarget.Member = ""
		snapshot, err := service.snapshots.Snapshot(ctx, effectiveTarget)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: intent.operation + " convergence readback failed", Path: string(PathDCS)})
			continue
		}
		revision = snapshot.Revision
		if snapshot.Cluster.Leader != nil {
			leader = snapshot.Cluster.Leader.Name
		} else {
			leader = ""
		}
		pending = standbyClusterPending(snapshot.Cluster, intent, previousLeader)
		summary := intent.operation + " cluster role is not yet authoritative"
		if len(pending) == 0 {
			summary = intent.operation + " cluster role is authoritative"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary,
			Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if len(pending) == 0 {
			return true, leader, nil, revision, evidence
		}
	}
	return false, leader, pending, revision, evidence
}

func standbyClusterPending(cluster dcs.ClusterState, intent standbyClusterIntent, previousLeader string) []string {
	pending := make([]string, 0, 4)
	if cluster.Leader == nil || cluster.Leader.Name == "" {
		return []string{"leader"}
	}
	members := memberMap(cluster.Members)
	leader, ok := members[cluster.Leader.Name]
	if !ok {
		return []string{"leader.member"}
	}
	if leader.Data.Role != intent.desired {
		pending = append(pending, "leader.role")
	}
	if leader.Data.State != "running" {
		pending = append(pending, "leader.state")
	}
	if intent.operation == "demote-cluster" {
		oldLeader, present := members[previousLeader]
		if !present || oldLeader.Data.State != "running" {
			pending = append(pending, "previous-leader.state")
		}
	}
	sort.Strings(pending)
	return pending
}

func standbyClusterSuccess(operationID string, intent standbyClusterIntent, data ClusterRoleData, evidence []Evidence) Result[ClusterRoleData] {
	return Result[ClusterRoleData]{OperationID: operationID, Outcome: Succeeded, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence}
}

func standbyClusterFailure(
	service *Service,
	operationID string,
	intent standbyClusterIntent,
	data ClusterRoleData,
	category Category,
	retryable bool,
	message string,
	cause error,
	evidence []Evidence,
) Result[ClusterRoleData] {
	data.Verification = VerifiedFailed
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: EvidenceControl, ObservedAt: service.now(), Summary: message, Path: string(PathREST), SendState: data.RESTSendState}}
	}
	return Result[ClusterRoleData]{OperationID: operationID, Outcome: Failed, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence,
		Error: NewError(category, intent.operation, intent.target, retryable, message, cause, evidence...)}
}

func standbyClusterUnknown(
	service *Service,
	operationID string,
	intent standbyClusterIntent,
	data ClusterRoleData,
	message string,
	cause error,
	evidence []Evidence,
) Result[ClusterRoleData] {
	data.Verification = Unverified
	return Result[ClusterRoleData]{OperationID: operationID, Outcome: Unknown, Target: intent.target, Path: PathREST, Data: data, Evidence: evidence,
		Error: NewError(CategoryUnknown, intent.operation, intent.target, false, message, cause, evidence...)}
}
