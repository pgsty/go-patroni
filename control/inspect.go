package control

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

// ConfigurationServiceOptions controls the local-only control.Service used by
// configuration inspection. The returned Service never opens DCS, Patroni, or
// PostgreSQL connections. Other use cases fail with a structured CONFIG result
// because their required capabilities are intentionally unavailable.
type ConfigurationServiceOptions struct {
	Clock          func() time.Time
	NewOperationID func() string
}

// NewConfigurationService creates the same high-level Service boundary used by
// every adapter without requiring network capabilities for a local diagnostic.
func NewConfigurationService(options ConfigurationServiceOptions) (*Service, error) {
	return NewService(ServiceOptions{
		Snapshots:      configurationSnapshotReader{},
		Clock:          options.Clock,
		NewOperationID: options.NewOperationID,
	})
}

type configurationSnapshotReader struct{}

func (configurationSnapshotReader) Snapshot(ctx context.Context, _ model.Target) (dcs.Snapshot, error) {
	if ctx == nil {
		return dcs.Snapshot{}, dcs.NewError(dcs.ErrorConfiguration, "snapshot", "", fmt.Errorf("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return dcs.Snapshot{}, dcs.NewError(dcs.ErrorCanceled, "snapshot", "", err)
	}
	return dcs.Snapshot{}, dcs.NewError(dcs.ErrorConfiguration, "snapshot", "", fmt.Errorf("configuration-only service has no DCS capability"))
}

type InspectConfigurationRequest struct {
	Resolved config.Resolved `json:"-" yaml:"-"`
}

func (request InspectConfigurationRequest) String() string {
	return fmt.Sprintf("control.InspectConfigurationRequest{resolved:%s}", request.Resolved.String())
}

func (request InspectConfigurationRequest) GoString() string { return request.String() }

type ConfigurationSource struct {
	Field string       `json:"field" yaml:"field"`
	Layer config.Layer `json:"layer" yaml:"layer"`
	Name  string       `json:"name" yaml:"name"`
}

// NetworkTimeoutInspection is the versioned machine-safe representation of
// effective network deadlines. Milliseconds are explicit so time.Duration is
// never serialized as an undocumented nanosecond integer.
type NetworkTimeoutInspection struct {
	DNSLookupMilliseconds      int64 `json:"dnsLookupMilliseconds" yaml:"dnsLookupMilliseconds"`
	DCSDialMilliseconds        int64 `json:"dcsDialMilliseconds" yaml:"dcsDialMilliseconds"`
	DCSRequestMilliseconds     int64 `json:"dcsRequestMilliseconds" yaml:"dcsRequestMilliseconds"`
	PatroniRequestMilliseconds int64 `json:"patroniRequestMilliseconds" yaml:"patroniRequestMilliseconds"`
	PostgresQueryMilliseconds  int64 `json:"postgresQueryMilliseconds" yaml:"postgresQueryMilliseconds"`
	PostgresCloseMilliseconds  int64 `json:"postgresCloseMilliseconds" yaml:"postgresCloseMilliseconds"`
}

type ConfigurationInspection struct {
	Target          model.Target             `json:"target" yaml:"target"`
	Effective       map[string]any           `json:"effective" yaml:"effective"`
	NetworkTimeouts NetworkTimeoutInspection `json:"networkTimeouts" yaml:"networkTimeouts"`
	Sources         []ConfigurationSource    `json:"sources" yaml:"sources"`
	Warnings        []config.Warning         `json:"warnings" yaml:"warnings"`
}

// InspectConfiguration returns a secret-safe, deterministic view of the
// selected configuration without performing network I/O.
func (service *Service) InspectConfiguration(ctx context.Context, request InspectConfigurationRequest) Result[ConfigurationInspection] {
	operationID := service.operationID()
	resolved := request.Resolved
	target := (model.Target{
		Context: resolved.Context, Namespace: resolved.Namespace, Scope: resolved.Scope, Group: resolved.Group,
	}).Normalize()
	if !validContext(ctx) {
		return failedRead[ConfigurationInspection](service, operationID, "inspect-config", target, PathLocal, CategoryUsage, false, "inspect-config requires a context", nil)
	}
	if err := ctx.Err(); err != nil {
		return failedRead[ConfigurationInspection](service, operationID, "inspect-config", target, PathLocal, CategoryFailed, false, "configuration inspection was canceled", err)
	}
	if err := target.Validate(false); err != nil {
		return failedRead[ConfigurationInspection](service, operationID, "inspect-config", target, PathLocal, CategoryUsage, false, "configuration target is invalid", err)
	}
	if err := resolved.Validate(config.OperationInspect, ""); err != nil {
		return failedRead[ConfigurationInspection](service, operationID, "inspect-config", target, PathLocal, CategoryConfig, false, "configuration inspection validation failed", err)
	}

	rawSources := resolved.Sources()
	sources := make([]ConfigurationSource, 0, len(rawSources))
	for field, source := range rawSources {
		sources = append(sources, ConfigurationSource{Field: field, Layer: source.Layer, Name: source.Name})
	}
	sort.Slice(sources, func(left, right int) bool {
		if sources[left].Field != sources[right].Field {
			return sources[left].Field < sources[right].Field
		}
		if sources[left].Layer != sources[right].Layer {
			return sources[left].Layer < sources[right].Layer
		}
		return sources[left].Name < sources[right].Name
	})
	warnings := append([]config.Warning{}, resolved.Warnings...)
	data := ConfigurationInspection{
		Target: target, Effective: resolved.Effective(), NetworkTimeouts: inspectNetworkTimeouts(resolved.Network),
		Sources: sources, Warnings: warnings,
	}
	return Result[ConfigurationInspection]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathLocal, Data: data,
		Evidence: []Evidence{{
			Source: EvidenceLocal, ObservedAt: service.now(),
			Summary: "effective configuration projected with secret redaction and source attribution", Path: "configuration",
		}},
	}
}

func inspectNetworkTimeouts(network config.NetworkConfig) NetworkTimeoutInspection {
	return NetworkTimeoutInspection{
		DNSLookupMilliseconds:      network.DNSLookupTimeout.Milliseconds(),
		DCSDialMilliseconds:        network.DCSDialTimeout.Milliseconds(),
		DCSRequestMilliseconds:     network.DCSRequestTimeout.Milliseconds(),
		PatroniRequestMilliseconds: network.PatroniTimeout.Milliseconds(),
		PostgresQueryMilliseconds:  network.PostgresTimeout.Milliseconds(),
		PostgresCloseMilliseconds:  network.PostgresCloseTimeout.Milliseconds(),
	}
}
