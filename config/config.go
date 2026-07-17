// Package config loads Patroni YAML tolerantly, retains its raw yaml.Node, and
// projects only the fields required by a selected Patroni operation.
package config

import (
	"fmt"
	"maps"
	"sort"

	"go.yaml.in/yaml/v3"
)

type Layer string

const (
	LayerDefault     Layer = "default"
	LayerFile        Layer = "file"
	LayerContext     Layer = "context"
	LayerEnvironment Layer = "environment"
	LayerFlag        Layer = "flag"
)

type Source struct {
	Layer Layer  `json:"layer" yaml:"layer"`
	Name  string `json:"name" yaml:"name"`
}

type WarningCode string

const (
	WarningSchemeLessDCSURL WarningCode = "SCHEMELESS_DCS_URL"
	WarningInsecureRESTTLS  WarningCode = "INSECURE_REST_TLS"
)

type Warning struct {
	Code    WarningCode `json:"code" yaml:"code"`
	Field   string      `json:"field" yaml:"field"`
	Message string      `json:"message" yaml:"message"`
}

type Environment interface {
	Lookup(key string) (string, bool)
}

type MapEnvironment map[string]string

func (environment MapEnvironment) Lookup(key string) (string, bool) {
	value, ok := environment[key]
	return value, ok
}

type Overrides struct {
	Context   *string
	DCSURL    *string
	Namespace *string
	Scope     *string
	Group     *int
	Insecure  *bool
}

type ResolveRequest struct {
	Context     string
	Environment Environment
	Overrides   Overrides
}

type LoadRequest struct {
	Path        string
	Environment Environment
}

type LocatorKind string

const (
	LocatorNone  LocatorKind = ""
	LocatorURL   LocatorKind = "url"
	LocatorProxy LocatorKind = "proxy"
	LocatorSRV   LocatorKind = "srv"
	LocatorHosts LocatorKind = "hosts"
	LocatorHost  LocatorKind = "host"
)

type TLSConfig struct {
	CAFile      string
	CertFile    string
	KeyFile     string
	KeyPassword Secret
}

type Etcd3Config struct {
	Configured bool
	Locator    LocatorKind
	Endpoints  []string
	URL        string
	Proxy      string
	SRV        string
	Protocol   string
	TLS        TLSConfig
	Username   string
	Password   Secret
}

type RESTConfig struct {
	Insecure    bool
	CAFile      string
	CertFile    string
	KeyFile     string
	KeyPassword Secret
	Username    string
	Password    Secret
}

type Document struct {
	raw            *yaml.Node
	root           map[string]any
	contexts       map[string]map[string]any
	network        any
	server         any
	defaultContext string
	sourceName     string
}

type Resolved struct {
	Context   string
	Namespace string
	Scope     string
	Citus     bool
	Group     *int
	Etcd3     Etcd3Config
	REST      RESTConfig
	Network   NetworkConfig `json:"-" yaml:"-"`
	Warnings  []Warning

	effective map[string]any
	sources   map[string]Source
}

func (document *Document) RawNode() *yaml.Node {
	if document == nil || document.raw == nil {
		return nil
	}
	return cloneYAMLNode(document.raw, make(map[*yaml.Node]*yaml.Node))
}

func (document *Document) DefaultContext() string {
	if document == nil || document.defaultContext == "" {
		return "default"
	}
	return document.defaultContext
}

func (document *Document) ContextNames() []string {
	if document == nil {
		return nil
	}
	set := map[string]struct{}{"default": {}}
	for name := range document.contexts {
		set[name] = struct{}{}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (document *Document) String() string {
	if document == nil {
		return "config.Document<nil>"
	}
	return fmt.Sprintf("config.Document{source:%q,defaultContext:%q,contexts:%d,raw:[REDACTED]}",
		document.sourceName, document.DefaultContext(), len(document.ContextNames()))
}

func (document *Document) GoString() string { return document.String() }

func (resolved Resolved) String() string {
	return fmt.Sprintf("config.Resolved{context:%q,namespace:%q,scope:%q,citus:%t,group:%v,etcd3Configured:%t,restAuth:%t,network:%s,effective:[REDACTED]}",
		resolved.Context, resolved.Namespace, resolved.Scope, resolved.Citus, resolved.Group, resolved.Etcd3.Configured, resolved.REST.Password.IsSet(), resolved.Network.String())
}

func (resolved Resolved) GoString() string { return resolved.String() }

func (resolved Resolved) Source(path string) (Source, bool) {
	source, ok := resolved.sources[path]
	return source, ok
}

func (resolved Resolved) Sources() map[string]Source {
	return maps.Clone(resolved.sources)
}

func cloneYAMLNode(node *yaml.Node, seen map[*yaml.Node]*yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}
	if clone, ok := seen[node]; ok {
		return clone
	}
	clone := *node
	clone.Content = make([]*yaml.Node, len(node.Content))
	seen[node] = &clone
	for index, child := range node.Content {
		clone.Content[index] = cloneYAMLNode(child, seen)
	}
	clone.Alias = cloneYAMLNode(node.Alias, seen)
	return &clone
}
