package cli

import (
	"fmt"
	"reflect"

	"github.com/pgsty/go-patroni/control"
)

const (
	machineKindDSN                    = "DSN"
	machineKindQueryResult            = "QueryResult"
	machineKindEffectiveConfiguration = "EffectiveConfiguration"
	machineKindClusterDiscovery       = "ClusterDiscovery"
	machineKindClusterList            = "ClusterList"
	machineKindClusterTopologyList    = "ClusterTopologyList"
	machineKindClusterTopology        = "ClusterTopology"
	machineKindDynamicConfig          = "DynamicConfig"
	machineKindVersionInfo            = "VersionInfo"
	machineKindClusterHistory         = "ClusterHistory"
	machineKindRemoveResult           = "RemoveResult"
	machineKindReloadResult           = "ReloadResult"
	machineKindRestartResult          = "RestartResult"
	machineKindReinitializeResult     = "ReinitializeResult"
	machineKindFailoverResult         = "FailoverResult"
	machineKindSwitchoverResult       = "SwitchoverResult"
	machineKindFlushResult            = "FlushResult"
	machineKindPauseResult            = "PauseResult"
	machineKindResumeResult           = "ResumeResult"
	machineKindConfigEditResult       = "ConfigEditResult"
	machineKindDemoteClusterResult    = "DemoteClusterResult"
	machineKindPromoteClusterResult   = "PromoteClusterResult"
)

var machineSuccessTypes = map[string]reflect.Type{
	machineKindDSN:                    reflect.TypeOf(control.DSNData{}),
	machineKindQueryResult:            reflect.TypeOf(control.QueryData{}),
	machineKindEffectiveConfiguration: reflect.TypeOf(control.ConfigurationInspection{}),
	machineKindClusterDiscovery:       reflect.TypeOf(machineDiscovery{}),
	machineKindClusterList:            reflect.TypeOf(machineClusterList{}),
	machineKindClusterTopologyList:    reflect.TypeOf(machineTopologyList{}),
	machineKindClusterTopology:        reflect.TypeOf(control.TopologyData{}),
	machineKindDynamicConfig:          reflect.TypeOf(control.ConfigData{}),
	machineKindVersionInfo:            reflect.TypeOf(machineVersionInfo{}),
	machineKindClusterHistory:         reflect.TypeOf(control.HistoryData{}),
	machineKindRemoveResult:           reflect.TypeOf(control.RemoveData{}),
	machineKindReloadResult:           reflect.TypeOf(control.BatchWriteData{}),
	machineKindRestartResult:          reflect.TypeOf(control.BatchWriteData{}),
	machineKindReinitializeResult:     reflect.TypeOf(control.BatchWriteData{}),
	machineKindFailoverResult:         reflect.TypeOf(control.ClusterWriteData{}),
	machineKindSwitchoverResult:       reflect.TypeOf(control.ClusterWriteData{}),
	machineKindFlushResult:            reflect.TypeOf(control.FlushData{}),
	machineKindPauseResult:            reflect.TypeOf(control.PauseData{}),
	machineKindResumeResult:           reflect.TypeOf(control.PauseData{}),
	machineKindConfigEditResult:       reflect.TypeOf(control.ConfigEditData{}),
	machineKindDemoteClusterResult:    reflect.TypeOf(control.ClusterRoleData{}),
	machineKindPromoteClusterResult:   reflect.TypeOf(control.ClusterRoleData{}),
}

// MachineSchemaCatalog returns an independent kind-to-data-type inventory for
// the repository-owned schema generator. The package is internal, so this does
// not add a public SDK surface or couple core control DTOs to JSON Schema.
func MachineSchemaCatalog() map[string]reflect.Type {
	result := make(map[string]reflect.Type, len(machineSuccessTypes))
	for kind, dataType := range machineSuccessTypes {
		result[kind] = dataType
	}
	return result
}

// MachineSchemaEnvelopeTypes exposes only reflection metadata for the
// repository-owned generator: CLI metadata and the adapter-specific safe error
// object. Callers cannot construct either unexported DTO through this API.
func MachineSchemaEnvelopeTypes() (reflect.Type, reflect.Type) {
	return reflect.TypeOf(machineMetadata{}), reflect.TypeOf(machineError{})
}

func newMachineSuccessEnvelope(kind string, metadata machineMetadata, data any) (any, error) {
	expected, ok := machineSuccessTypes[kind]
	if !ok {
		return nil, fmt.Errorf("machine success kind %q is not registered", kind)
	}
	actual := reflect.TypeOf(data)
	if actual != expected {
		return nil, fmt.Errorf("machine success kind %q requires %s, got %v", kind, expected, actual)
	}
	return successEnvelope[any]{APIVersion: machineAPIVersion, Kind: kind, Metadata: metadata, Data: data}, nil
}
