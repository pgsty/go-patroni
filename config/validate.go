package config

import "strings"

type Operation string

const (
	OperationLocalVersion Operation = "local-version"
	OperationInspect      Operation = "inspect-config"
	OperationDiscover     Operation = "discover"
	OperationClusterRead  Operation = "cluster-read"
	OperationRESTWrite    Operation = "rest-write"
	OperationQuery        Operation = "query"
)

// Validate enforces only requirements needed by the current operation. An
// explicit scope satisfies cluster selection without requiring root scope.
func (resolved Resolved) Validate(operation Operation, explicitScope string) error {
	needDCS := false
	needScope := false
	switch operation {
	case OperationLocalVersion, OperationInspect:
		return nil
	case OperationDiscover:
		needDCS = true
	case OperationClusterRead, OperationRESTWrite, OperationQuery:
		needDCS, needScope = true, true
	default:
		return &ValidationError{Operation: operation, Field: "operation", Reason: "unknown operation contract"}
	}
	if needDCS && !resolved.Etcd3.Configured {
		source, _ := resolved.Source("etcd3")
		return &ValidationError{
			Operation: operation, Field: "etcd3", Source: source,
			Reason: "an etcd3 url, proxy, srv, hosts, or host locator is required; other DCS backends are unsupported",
		}
	}
	if needScope && strings.TrimSpace(explicitScope) == "" && strings.TrimSpace(resolved.Scope) == "" {
		source, _ := resolved.Source("scope")
		return &ValidationError{
			Operation: operation, Field: "scope", Source: source,
			Reason: "configure scope or provide an explicit cluster target",
		}
	}
	return nil
}
