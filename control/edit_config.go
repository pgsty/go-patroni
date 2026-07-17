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

// ConfigSetting is a parsed patronictl --set/--pg assignment. Adapters own
// YAML/file/editor input; control owns dotted-path application and CAS.
type ConfigSetting struct {
	Path  string `json:"path" yaml:"path"`
	Value any    `json:"-" yaml:"-"`
}

func (setting ConfigSetting) String() string {
	return fmt.Sprintf("control.ConfigSetting{path:%q,value:[REDACTED]}", strings.TrimSpace(setting.Path))
}

func (setting ConfigSetting) GoString() string { return setting.String() }

// EditConfigRequest preserves patronictl's operation order: replacement,
// recursive apply patch, then ordered dotted-path settings. A non-nil empty
// Replacement intentionally means replace the current config with an object.
type EditConfigRequest struct {
	Target      model.Target    `json:"target" yaml:"target"`
	Replacement map[string]any  `json:"-" yaml:"-"`
	Apply       map[string]any  `json:"-" yaml:"-"`
	Settings    []ConfigSetting `json:"settings,omitempty" yaml:"settings,omitempty"`
	Citus       bool            `json:"citus" yaml:"citus"`
}

func (request EditConfigRequest) String() string {
	replacement := request.Replacement != nil
	applyPaths := topLevelConfigPaths(request.Apply)
	settingPaths := make([]string, 0, len(request.Settings))
	for _, setting := range request.Settings {
		settingPaths = append(settingPaths, strings.TrimSpace(setting.Path))
	}
	return fmt.Sprintf("control.EditConfigRequest{target:%s,replacement:%t,applyPaths:%q,settingPaths:%q,citus:%t}",
		request.Target.Normalize().ClusterID(), replacement, applyPaths, settingPaths, request.Citus)
}

func (request EditConfigRequest) GoString() string { return request.String() }

// ConfigEditPreview is explicit caller-requested configuration data. Default
// formatting and serialization expose only paths, never configuration values.
type ConfigEditPreview struct {
	Before       map[string]any `json:"-" yaml:"-"`
	After        map[string]any `json:"-" yaml:"-"`
	ChangedPaths []string       `json:"changedPaths" yaml:"changedPaths"`
	Noop         bool           `json:"noop" yaml:"noop"`
}

func (preview ConfigEditPreview) String() string {
	return fmt.Sprintf("control.ConfigEditPreview{changedPaths:%q,noop:%t,before:[REDACTED],after:[REDACTED]}", preview.ChangedPaths, preview.Noop)
}

func (preview ConfigEditPreview) GoString() string { return preview.String() }

type ConfigEditData struct {
	ChangedPaths   []string     `json:"changedPaths" yaml:"changedPaths"`
	BeforeRevision int64        `json:"beforeRevision" yaml:"beforeRevision"`
	DCSRevision    int64        `json:"dcsRevision,omitempty" yaml:"dcsRevision,omitempty"`
	DCSSendState   SendState    `json:"dcsSendState" yaml:"dcsSendState"`
	Verification   Verification `json:"verification" yaml:"verification"`
	Noop           bool         `json:"noop" yaml:"noop"`
}

func (data ConfigEditData) Validate() error {
	if !data.DCSSendState.validOrEmpty() || data.DCSSendState == "" {
		return fmt.Errorf("edit-config DCS send state %q is invalid", data.DCSSendState)
	}
	if data.Verification != Unverified && data.Verification != VerifiedSucceeded && data.Verification != VerifiedFailed {
		return fmt.Errorf("edit-config verification %q is invalid", data.Verification)
	}
	if data.BeforeRevision < 0 || data.DCSRevision < 0 {
		return errors.New("edit-config revisions must be non-negative")
	}
	if !sort.StringsAreSorted(data.ChangedPaths) {
		return errors.New("edit-config changed paths must be deterministic")
	}
	if data.Noop && (data.DCSSendState != SendNotSent || data.Verification != VerifiedSucceeded) {
		return errors.New("edit-config no-op requires verified NOT_SENT success")
	}
	return nil
}

// PreviewEditConfig performs the source-compatible replacement/apply/set
// projection without I/O. It is used by CLI/Web adapters to render a diff
// before they call PrepareEditConfig.
func PreviewEditConfig(current map[string]any, request EditConfigRequest) (ConfigEditPreview, error) {
	if request.Replacement == nil && request.Apply == nil && len(request.Settings) == 0 {
		return ConfigEditPreview{}, errors.New("edit-config requires replacement, apply patch, or settings")
	}
	before, err := cloneConfigMap(current)
	if err != nil {
		return ConfigEditPreview{}, errors.New("current dynamic configuration is not JSON-compatible")
	}
	after, err := cloneConfigMap(current)
	if err != nil {
		return ConfigEditPreview{}, errors.New("current dynamic configuration is not JSON-compatible")
	}
	if request.Replacement != nil {
		after, err = cloneConfigMap(request.Replacement)
		if err != nil {
			return ConfigEditPreview{}, errors.New("replacement dynamic configuration is not JSON-compatible")
		}
	}
	if request.Apply != nil {
		patch, cloneError := cloneConfigMap(request.Apply)
		if cloneError != nil {
			return ConfigEditPreview{}, errors.New("dynamic configuration patch is not JSON-compatible")
		}
		applyPatroniConfigPatch(after, patch)
	}
	for _, setting := range request.Settings {
		if err := applyPatroniConfigSetting(after, setting); err != nil {
			return ConfigEditPreview{}, err
		}
	}
	changed := changedConfigPaths(before, after)
	return ConfigEditPreview{Before: before, After: after, ChangedPaths: changed, Noop: len(changed) == 0}, nil
}

func topLevelConfigPaths(configuration map[string]any) []string {
	paths := make([]string, 0, len(configuration))
	for path := range configuration {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func cloneConfigMap(input map[string]any) (map[string]any, error) {
	if input == nil {
		return map[string]any{}, nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		copyValue, err := cloneConfigValue(value)
		if err != nil {
			return nil, err
		}
		cloned[key] = copyValue
	}
	return cloned, nil
}

func cloneConfigValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil, string, bool, json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return typed, nil
	case map[string]any:
		return cloneConfigMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			copyValue, err := cloneConfigValue(item)
			if err != nil {
				return nil, err
			}
			cloned[index] = copyValue
		}
		return cloned, nil
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return nil, err
		}
		var cloned any
		decoder := json.NewDecoder(strings.NewReader(string(encoded)))
		decoder.UseNumber()
		if err := decoder.Decode(&cloned); err != nil {
			return nil, err
		}
		return cloned, nil
	}
}

func applyPatroniConfigPatch(configuration, patch map[string]any) {
	for key, value := range patch {
		if value == nil {
			delete(configuration, key)
			continue
		}
		patchMap, patchIsMap := value.(map[string]any)
		if patchIsMap {
			if currentMap, currentIsMap := configuration[key].(map[string]any); currentIsMap {
				applyPatroniConfigPatch(currentMap, patchMap)
			} else {
				cloned, _ := cloneConfigMap(patchMap)
				configuration[key] = cloned
			}
			continue
		}
		if current, exists := configuration[key]; exists && patroniConfigValuesEquivalent(current, value) {
			continue
		}
		cloned, _ := cloneConfigValue(value)
		configuration[key] = cloned
	}
}

func patroniConfigValuesEquivalent(left, right any) bool {
	if _, ok := left.(map[string]any); ok {
		return canonicalConfigEqual(left, right)
	}
	if _, ok := right.(map[string]any); ok {
		return false
	}
	if isConfigComposite(left) || isConfigComposite(right) {
		return canonicalConfigEqual(left, right)
	}
	return patroniScalarString(left) == patroniScalarString(right)
}

func isConfigComposite(value any) bool {
	switch value.(type) {
	case []any, map[string]any:
		return true
	default:
		return false
	}
}

func patroniScalarString(value any) string {
	switch typed := value.(type) {
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case nil:
		return "None"
	default:
		return fmt.Sprint(typed)
	}
}

func applyPatroniConfigSetting(configuration map[string]any, setting ConfigSetting) error {
	path := strings.TrimSpace(setting.Path)
	parts := strings.Split(path, ".")
	value, err := cloneConfigValue(setting.Value)
	if err != nil {
		return fmt.Errorf("edit-config setting %q is not JSON-compatible", path)
	}
	setPatroniConfigPath(configuration, parts, value, nil)
	return nil
}

func setPatroniConfigPath(configuration map[string]any, path []string, value any, prefix []string) {
	if len(prefix) == 2 && prefix[0] == "postgresql" && prefix[1] == "parameters" {
		path = []string{strings.Join(path, ".")}
	}
	key := path[0]
	if len(path) == 1 {
		if value == nil {
			delete(configuration, key)
		} else {
			configuration[key] = value
		}
		return
	}
	child, ok := configuration[key].(map[string]any)
	if !ok {
		child = map[string]any{}
		configuration[key] = child
	}
	setPatroniConfigPath(child, path[1:], value, append(prefix, key))
	if len(child) == 0 {
		delete(configuration, key)
	}
}

func canonicalConfigEqual(left, right any) bool {
	leftJSON, leftError := json.Marshal(left)
	rightJSON, rightError := json.Marshal(right)
	return leftError == nil && rightError == nil && string(leftJSON) == string(rightJSON)
}

func changedConfigPaths(before, after map[string]any) []string {
	changed := make([]string, 0)
	collectChangedConfigPaths(before, after, "", &changed)
	sort.Strings(changed)
	return changed
}

func collectChangedConfigPaths(before, after map[string]any, prefix string, changed *[]string) {
	keys := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		left, leftOK := before[key]
		right, rightOK := after[key]
		leftMap, leftMapOK := left.(map[string]any)
		rightMap, rightMapOK := right.(map[string]any)
		if leftOK && rightOK && leftMapOK && rightMapOK {
			collectChangedConfigPaths(leftMap, rightMap, path, changed)
			continue
		}
		if leftOK == rightOK && canonicalConfigEqual(left, right) {
			continue
		}
		if !leftOK && rightMapOK && len(rightMap) > 0 {
			collectChangedConfigPaths(map[string]any{}, rightMap, path, changed)
			continue
		}
		if !rightOK && leftMapOK && len(leftMap) > 0 {
			collectChangedConfigPaths(leftMap, map[string]any{}, path, changed)
			continue
		}
		*changed = append(*changed, path)
	}
}

func (service *Service) PrepareEditConfig(ctx context.Context, request EditConfigRequest) Result[Plan] {
	operationID := service.operationID()
	request, requestError := normalizeEditConfigRequest(request)
	if !validContext(ctx) {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config requires a context", nil)
	}
	if requestError != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config request is invalid", requestError)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, request.Target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, category, retryable, "edit-config cluster snapshot failed", err)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[Plan](service, operationID, "edit-config", request.Target, PathDCS, snapshot, versionError)
	}
	entry, err := configEntryForEdit(snapshot)
	if err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, configEntryCategory(err), false, err.Error(), err,
			snapshotEvidence(service, snapshot, "dynamic configuration lookup completed"))
	}
	preview, err := PreviewEditConfig(snapshot.Cluster.Config, request)
	if err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config mutation is invalid", err,
			snapshotEvidence(service, snapshot, "dynamic configuration read for preview"))
	}
	requestToken, err := service.configRequestToken(request)
	if err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config request is not JSON-compatible", err)
	}
	desiredToken, err := service.configValueToken(preview.After)
	if err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config desired configuration is not JSON-compatible", err)
	}
	changedJSON, _ := json.Marshal(preview.ChangedPaths)
	noOpValue := strconv.FormatBool(preview.Noop)
	citusValue := strconv.FormatBool(request.Citus)
	binding, err := service.configPlanBinding(request.Target, entry.ModRevision, requestToken, desiredToken, string(changedJSON), noOpValue, citusValue)
	if err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryInternal, false, "edit-config plan binding failed", err)
	}
	summary := "edit dynamic configuration"
	if len(preview.ChangedPaths) == 0 {
		summary += ": no changes"
	} else {
		summary += " paths: " + strings.Join(preview.ChangedPaths, ", ")
	}
	plan := Plan{
		OperationID: operationID, Operation: "edit-config", Target: request.Target, Path: PathDCS,
		Risk: RiskConfiguration, RetrySafety: UnsafeAfterSend, Summary: summary,
		Preconditions: []Precondition{
			{Field: "dcs.revision", Expected: strconv.FormatInt(snapshot.Revision, 10), Source: EvidenceDCS},
			{Field: "config.modRevision", Expected: strconv.FormatInt(entry.ModRevision, 10), Source: EvidenceDCS},
			{Field: "config.request", Expected: requestToken, Source: EvidenceControl},
			{Field: "config.desired", Expected: desiredToken, Source: EvidenceControl},
			{Field: "config.changedPaths", Expected: string(changedJSON), Source: EvidenceControl},
			{Field: "config.noop", Expected: noOpValue, Source: EvidenceControl},
			{Field: "request.citus", Expected: citusValue, Source: EvidenceControl},
			{Field: "config.binding", Expected: binding, Source: EvidenceControl},
		},
	}
	if err := plan.Validate(); err != nil {
		return failedRead[Plan](service, operationID, "edit-config", request.Target, PathDCS, CategoryInternal, false, "edit-config plan construction failed", err,
			snapshotEvidence(service, snapshot, "dynamic configuration preview completed"))
	}
	return Result[Plan]{OperationID: operationID, Outcome: Succeeded, Target: request.Target, Path: PathDCS, Data: plan,
		Evidence: []Evidence{snapshotEvidence(service, snapshot, "edit-config plan built from revisioned dynamic configuration")}}
}

func (service *Service) ExecuteEditConfig(ctx context.Context, request EditConfigRequest, plan Plan) Result[ConfigEditData] {
	operationID := strings.TrimSpace(plan.OperationID)
	if operationID == "" {
		operationID = service.operationID()
	}
	request, requestError := normalizeEditConfigRequest(request)
	if !validContext(ctx) {
		return failedRead[ConfigEditData](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config requires a context", nil)
	}
	if requestError != nil {
		return failedRead[ConfigEditData](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config request is invalid", requestError)
	}
	if err := service.validateEditConfigPlan(plan, request); err != nil {
		return failedRead[ConfigEditData](service, operationID, "edit-config", request.Target, PathDCS, CategoryUsage, false, "edit-config plan does not match the request", err)
	}
	expectedRevision, _ := editConfigPlanRevision(plan)
	plannedDesired, _ := expectedPrecondition(plan, "config.desired")
	plannedPaths, _ := editConfigPlanPaths(plan)
	data := ConfigEditData{ChangedPaths: plannedPaths, BeforeRevision: expectedRevision, DCSSendState: SendNotSent, Verification: Unverified}

	snapshot, err := service.snapshots.Snapshot(ctx, request.Target)
	if err != nil {
		category, retryable := classifyReadError(err)
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, category, retryable, "edit-config execution snapshot failed", err, nil)
	}
	if versionError := service.checkSnapshotPatroniVersion(snapshot, false); versionError != nil {
		return unsupportedVersionResult[ConfigEditData](service, operationID, "edit-config", request.Target, PathDCS, snapshot, versionError)
	}
	evidence := []Evidence{snapshotEvidence(service, snapshot, "edit-config CAS preconditions revalidated")}
	entry, err := configEntryForEdit(snapshot)
	if err != nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryConflict, false, "edit-config was not sent because the config key changed", err, evidence)
	}
	currentToken, err := service.configValueToken(snapshot.Cluster.Config)
	if err != nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryConfig, false, "current dynamic configuration is not JSON-compatible", err, evidence)
	}
	if entry.ModRevision != expectedRevision {
		data.DCSRevision = snapshot.Revision
		if hmac.Equal([]byte(currentToken), []byte(plannedDesired)) {
			data.Noop = true
			data.Verification = VerifiedSucceeded
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "concurrent writer already established the confirmed configuration", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent})
			return editConfigSuccess(operationID, request.Target, data, evidence)
		}
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryConflict, false, "edit-config was not sent because the config revision changed", nil, evidence)
	}
	preview, err := PreviewEditConfig(snapshot.Cluster.Config, request)
	if err != nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryUsage, false, "edit-config mutation is invalid", err, evidence)
	}
	desiredToken, err := service.configValueToken(preview.After)
	if err != nil || !hmac.Equal([]byte(desiredToken), []byte(plannedDesired)) || !reflectStringSlices(preview.ChangedPaths, plannedPaths) {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryUsage, false, "edit-config desired state differs from the confirmed plan", err, evidence)
	}
	plannedNoop, _ := expectedPrecondition(plan, "config.noop")
	if plannedNoop != strconv.FormatBool(preview.Noop) {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryUsage, false, "edit-config no-op state differs from the confirmed plan", nil, evidence)
	}
	if preview.Noop {
		data.Noop = true
		data.DCSRevision = snapshot.Revision
		data.Verification = VerifiedSucceeded
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "dynamic configuration already matches the request", Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix, SendState: SendNotSent})
		return editConfigSuccess(operationID, request.Target, data, evidence)
	}
	if err := ctx.Err(); err != nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryFailed, false, "edit-config was canceled before DCS CAS", err, evidence)
	}
	if service.configDCS == nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryConfig, false, "edit-config requires a DCS config CAS capability", nil, evidence)
	}
	payload, err := json.Marshal(preview.After)
	if err != nil {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryUsage, false, "edit-config payload is not JSON-compatible", err, evidence)
	}
	writeResult, writeError := service.configDCS.CompareAndSwapConfig(ctx, request.Target, payload, &expectedRevision)
	data.DCSSendState = dcsSendState(writeError, writeResult.Applied)
	data.DCSRevision = writeResult.Revision
	evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: editConfigWriteSummary(writeResult, writeError),
		Revision: strconv.FormatInt(writeResult.Revision, 10), Path: string(PathDCS), SendState: data.DCSSendState})
	verified, verificationRevision, verificationEvidence := service.verifyEditConfig(ctx, request.Target, plannedDesired)
	evidence = append(evidence, verificationEvidence...)
	if verificationRevision > data.DCSRevision {
		data.DCSRevision = verificationRevision
	}
	if verified {
		data.Verification = VerifiedSucceeded
		if data.DCSSendState == SendNotSent {
			data.Noop = true
		}
		return editConfigSuccess(operationID, request.Target, data, evidence)
	}
	var conflict *dcs.ConflictError
	if errors.As(writeError, &conflict) {
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, CategoryConflict, false, "edit-config CAS lost a concurrent modification", writeError, evidence)
	}
	if data.DCSSendState == SendNotSent {
		category, retryable := classifyReadError(writeError)
		data.Verification = VerifiedFailed
		return editConfigFailure(service, operationID, request.Target, data, category, retryable, "edit-config was definitely not sent", writeError, evidence)
	}
	cause := writeError
	if ctx.Err() != nil {
		cause = errors.Join(writeError, ctx.Err())
	}
	return editConfigUnknown(service, operationID, request.Target, data, "edit-config was not verified; inspect DCS before retrying", cause, evidence)
}

func normalizeEditConfigRequest(request EditConfigRequest) (EditConfigRequest, error) {
	request.Target = request.Target.Normalize()
	if request.Target.Member != "" {
		return request, errors.New("edit-config target must be a cluster")
	}
	if err := request.Target.Validate(true); err != nil {
		return request, err
	}
	if request.Replacement == nil && request.Apply == nil && len(request.Settings) == 0 {
		return request, errors.New("edit-config requires replacement, apply patch, or settings")
	}
	return request, nil
}

func configEntryForEdit(snapshot dcs.Snapshot) (dcs.Entry, error) {
	entry, ok := snapshot.Entry("config")
	if !ok {
		return dcs.Entry{}, errors.New("dynamic config key does not exist")
	}
	for _, issue := range snapshot.Issues {
		if issue.RelativePath == "config" {
			return dcs.Entry{}, errors.New("dynamic config key is not a JSON object")
		}
	}
	return entry, nil
}

func configEntryCategory(err error) Category {
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return CategoryNotFound
	}
	return CategoryConfig
}

func (service *Service) configRequestToken(request EditConfigRequest) (string, error) {
	settings := make([]struct {
		Path  string `json:"path"`
		Value any    `json:"value"`
	}, len(request.Settings))
	for index, setting := range request.Settings {
		settings[index].Path = strings.TrimSpace(setting.Path)
		settings[index].Value = setting.Value
	}
	material := struct {
		HasReplacement bool           `json:"hasReplacement"`
		Replacement    map[string]any `json:"replacement"`
		HasApply       bool           `json:"hasApply"`
		Apply          map[string]any `json:"apply"`
		Settings       any            `json:"settings"`
		Citus          bool           `json:"citus"`
	}{request.Replacement != nil, request.Replacement, request.Apply != nil, request.Apply, settings, request.Citus}
	return service.planToken("edit-config/request", material)
}

func (service *Service) configValueToken(value map[string]any) (string, error) {
	return service.planToken("edit-config/desired", value)
}

func (service *Service) configPlanBinding(target model.Target, revision int64, requestToken, desiredToken, changedPaths, noop, citus string) (string, error) {
	material := struct {
		Target       string `json:"target"`
		Revision     int64  `json:"revision"`
		Request      string `json:"request"`
		Desired      string `json:"desired"`
		ChangedPaths string `json:"changedPaths"`
		Noop         string `json:"noop"`
		Citus        string `json:"citus"`
	}{target.Normalize().ClusterID(), revision, requestToken, desiredToken, changedPaths, noop, citus}
	return service.planToken("edit-config/plan", material)
}

func (service *Service) validateEditConfigPlan(plan Plan, request EditConfigRequest) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Operation != "edit-config" || plan.Path != PathDCS || plan.Risk != RiskConfiguration || plan.RetrySafety != UnsafeAfterSend {
		return errors.New("plan operation contract differs from edit-config")
	}
	if plan.Target.Normalize().ClusterID() != request.Target.ClusterID() {
		return errors.New("plan cluster differs from request")
	}
	requestToken, err := service.configRequestToken(request)
	if err != nil {
		return err
	}
	plannedRequest, ok := expectedPrecondition(plan, "config.request")
	if !ok || !hmac.Equal([]byte(plannedRequest), []byte(requestToken)) {
		return errors.New("plan configuration request differs from request")
	}
	citus, ok := expectedPrecondition(plan, "request.citus")
	if !ok || citus != strconv.FormatBool(request.Citus) {
		return errors.New("plan Citus selector differs from request")
	}
	revision, err := editConfigPlanRevision(plan)
	if err != nil {
		return err
	}
	desired, ok := expectedPrecondition(plan, "config.desired")
	if !ok || len(desired) != sha256.Size*2 {
		return errors.New("plan desired configuration token is invalid")
	}
	paths, err := editConfigPlanPaths(plan)
	if err != nil {
		return err
	}
	pathsJSON, _ := json.Marshal(paths)
	noop, ok := expectedPrecondition(plan, "config.noop")
	if !ok || (noop != "true" && noop != "false") {
		return errors.New("plan no-op precondition is invalid")
	}
	plannedBinding, ok := expectedPrecondition(plan, "config.binding")
	if !ok || len(plannedBinding) != sha256.Size*2 {
		return errors.New("plan binding is invalid")
	}
	binding, err := service.configPlanBinding(request.Target, revision, requestToken, desired, string(pathsJSON), noop, citus)
	if err != nil || !hmac.Equal([]byte(plannedBinding), []byte(binding)) {
		return errors.New("plan configuration binding is invalid")
	}
	return nil
}

func editConfigPlanRevision(plan Plan) (int64, error) {
	value, ok := expectedPrecondition(plan, "config.modRevision")
	if !ok {
		return 0, errors.New("plan config revision is missing")
	}
	revision, err := strconv.ParseInt(value, 10, 64)
	if err != nil || revision <= 0 {
		return 0, errors.New("plan config revision is invalid")
	}
	return revision, nil
}

func editConfigPlanPaths(plan Plan) ([]string, error) {
	value, ok := expectedPrecondition(plan, "config.changedPaths")
	if !ok {
		return nil, errors.New("plan changed paths are missing")
	}
	var paths []string
	if err := json.Unmarshal([]byte(value), &paths); err != nil || !sort.StringsAreSorted(paths) {
		return nil, errors.New("plan changed paths are invalid")
	}
	return paths, nil
}

func reflectStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func editConfigWriteSummary(result dcs.WriteResult, err error) string {
	if err == nil && result.Applied {
		return "DCS dynamic configuration CAS was applied"
	}
	var conflict *dcs.ConflictError
	if errors.As(err, &conflict) {
		return "DCS dynamic configuration CAS conflicted"
	}
	return "DCS dynamic configuration CAS ended without a verified apply"
}

func (service *Service) verifyEditConfig(ctx context.Context, target model.Target, desiredToken string) (bool, int64, []Evidence) {
	evidence := make([]Evidence, 0, service.verificationAttempts)
	var revision int64
	for attempt := 0; attempt < service.verificationAttempts; attempt++ {
		if attempt > 0 {
			if err := service.wait(ctx, restartVerificationInterval); err != nil {
				evidence = append(evidence, Evidence{Source: EvidenceControl, ObservedAt: service.now(), Summary: "edit-config readback canceled", Path: string(PathDCS)})
				return false, revision, evidence
			}
		}
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "edit-config readback failed", Path: string(PathDCS)})
			continue
		}
		revision = snapshot.Revision
		_, entryError := configEntryForEdit(snapshot)
		observedToken, tokenError := service.configValueToken(snapshot.Cluster.Config)
		matched := entryError == nil && tokenError == nil && hmac.Equal([]byte(observedToken), []byte(desiredToken))
		summary := "dynamic configuration does not match the confirmed desired state"
		if matched {
			summary = "dynamic configuration matches the confirmed desired state"
		}
		evidence = append(evidence, Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: summary,
			Revision: strconv.FormatInt(snapshot.Revision, 10), Path: snapshot.Prefix})
		if matched {
			return true, revision, evidence
		}
	}
	return false, revision, evidence
}

func editConfigSuccess(operationID string, target model.Target, data ConfigEditData, evidence []Evidence) Result[ConfigEditData] {
	return Result[ConfigEditData]{OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS, Data: data, Evidence: evidence}
}

func editConfigFailure(service *Service, operationID string, target model.Target, data ConfigEditData, category Category, retryable bool, message string, cause error, evidence []Evidence) Result[ConfigEditData] {
	data.Verification = VerifiedFailed
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: EvidenceDCS, ObservedAt: service.now(), Summary: message, Path: string(PathDCS), SendState: data.DCSSendState}}
	}
	return Result[ConfigEditData]{OperationID: operationID, Outcome: Failed, Target: target, Path: PathDCS, Data: data, Evidence: evidence,
		Error: NewError(category, "edit-config", target, retryable, message, cause, evidence...)}
}

func editConfigUnknown(service *Service, operationID string, target model.Target, data ConfigEditData, message string, cause error, evidence []Evidence) Result[ConfigEditData] {
	data.Verification = Unverified
	if len(evidence) == 0 {
		evidence = []Evidence{{Source: EvidenceDCS, ObservedAt: service.now(), Summary: message, Path: string(PathDCS), SendState: data.DCSSendState}}
	}
	return Result[ConfigEditData]{OperationID: operationID, Outcome: Unknown, Target: target, Path: PathDCS, Data: data, Evidence: evidence,
		Error: NewError(CategoryUnknown, "edit-config", target, false, message, cause, evidence...)}
}
