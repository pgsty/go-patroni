package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

var patroniDCSSections = []string{"etcd", "etcd3", "consul", "zookeeper", "exhibitor", "kubernetes", "raft"}

func (document *Document) Resolve(request ResolveRequest) (Resolved, error) {
	if document == nil {
		return Resolved{}, newError(ErrorProjection, "", "", "document is nil", nil)
	}
	environment := request.Environment
	if environment == nil {
		environment = osEnvironment{}
	}
	contextName := document.DefaultContext()
	if selected, ok := environment.Lookup("BOAR_CONTEXT"); ok && strings.TrimSpace(selected) != "" {
		contextName = strings.TrimSpace(selected)
	}
	if strings.TrimSpace(request.Context) != "" {
		contextName = strings.TrimSpace(request.Context)
	}
	if request.Overrides.Context != nil && strings.TrimSpace(*request.Overrides.Context) != "" {
		contextName = strings.TrimSpace(*request.Overrides.Context)
	}

	layers, err := document.contextLayers(contextName, nil)
	if err != nil {
		return Resolved{}, err
	}
	effective := map[string]any{}
	sources := map[string]Source{}
	applyLayer(effective, sources, resolutionLayer{
		values: map[string]any{"namespace": "service"},
		source: Source{Layer: LayerDefault, Name: "Patroni default"},
	})
	for _, layer := range layers {
		applyLayer(effective, sources, layer)
	}

	warnings := make([]Warning, 0, 2)
	if value, ok := environment.Lookup("DCS_URL"); ok && strings.TrimSpace(value) != "" {
		warning, err := applyDCSURL(effective, sources, value, Source{Layer: LayerEnvironment, Name: "DCS_URL"})
		if err != nil {
			return Resolved{}, err
		}
		if warning != nil {
			warnings = append(warnings, *warning)
		}
	}
	if request.Overrides.DCSURL != nil {
		warning, err := applyDCSURL(effective, sources, *request.Overrides.DCSURL, Source{Layer: LayerFlag, Name: "--dcs-url"})
		if err != nil {
			return Resolved{}, err
		}
		if warning != nil {
			warnings = append(warnings, *warning)
		}
	}
	if request.Overrides.Namespace != nil {
		replaceTopLevel(effective, sources, "namespace", *request.Overrides.Namespace, Source{Layer: LayerFlag, Name: "--namespace"})
	}
	if request.Overrides.Scope != nil {
		replaceTopLevel(effective, sources, "scope", *request.Overrides.Scope, Source{Layer: LayerFlag, Name: "--scope"})
	}
	if request.Overrides.Group != nil {
		applyLayer(effective, sources, resolutionLayer{
			values: map[string]any{"citus": map[string]any{"group": *request.Overrides.Group}},
			source: Source{Layer: LayerFlag, Name: "--group"},
		})
	}
	if request.Overrides.Insecure != nil {
		applyLayer(effective, sources, resolutionLayer{
			values: map[string]any{"ctl": map[string]any{"insecure": *request.Overrides.Insecure}},
			source: Source{Layer: LayerFlag, Name: "--insecure"},
		})
	}

	resolved, err := project(contextName, effective, sources)
	if err != nil {
		return Resolved{}, err
	}
	resolved.Network, err = document.NetworkConfig()
	if err != nil {
		return Resolved{}, err
	}
	if resolved.REST.Insecure {
		warnings = append(warnings, Warning{
			Code: WarningInsecureRESTTLS, Field: "ctl.insecure",
			Message: "Patroni REST TLS certificate verification is disabled; use only in an explicitly isolated environment",
		})
	}
	resolved.Warnings = warnings
	return resolved, nil
}

func (document *Document) contextLayers(name string, stack []string) ([]resolutionLayer, error) {
	for _, existing := range stack {
		if existing == name {
			return nil, newError(ErrorContext, "boar.contexts."+name+".extends", document.sourceName, "context inheritance cycle", nil)
		}
	}
	if name == "default" {
		layers := []resolutionLayer{{
			values: cloneMap(document.root), source: Source{Layer: LayerFile, Name: document.sourceName},
		}}
		if overlay, ok := document.contexts["default"]; ok {
			if extends, present := overlay["extends"]; present && extends != nil {
				return nil, newError(ErrorContext, "boar.contexts.default.extends", document.sourceName, "default context cannot extend another context", nil)
			}
			layers = append(layers, resolutionLayer{values: withoutExtends(overlay), source: Source{Layer: LayerContext, Name: "default"}})
		}
		return layers, nil
	}
	contextMap, ok := document.contexts[name]
	if !ok {
		return nil, newError(ErrorContext, "boar.contexts."+name, document.sourceName, "named context does not exist", nil)
	}
	layers := []resolutionLayer{}
	if rawParent, present := contextMap["extends"]; present && rawParent != nil {
		parent, ok := rawParent.(string)
		if !ok || strings.TrimSpace(parent) == "" {
			return nil, newError(ErrorContext, "boar.contexts."+name+".extends", document.sourceName, "must be a non-empty context name", nil)
		}
		parentLayers, err := document.contextLayers(strings.TrimSpace(parent), append(stack, name))
		if err != nil {
			return nil, err
		}
		layers = append(layers, parentLayers...)
	}
	layers = append(layers, resolutionLayer{values: withoutExtends(contextMap), source: Source{Layer: LayerContext, Name: name}})
	return layers, nil
}

func withoutExtends(input map[string]any) map[string]any {
	output := cloneMap(input)
	delete(output, "extends")
	return output
}

func applyDCSURL(effective map[string]any, sources map[string]Source, value string, source Source) (*Warning, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return nil, newError(ErrorProjection, "DCS_URL", source.Name, "must not be empty", nil)
	}
	schemeLess := !strings.Contains(raw, "://")
	parsedValue := raw
	if schemeLess {
		parsedValue = "etcd3://" + raw
	}
	parsed, err := url.Parse(parsedValue)
	if err != nil {
		return nil, newError(ErrorProjection, "DCS_URL", source.Name, "cannot parse DCS locator", err)
	}
	if parsed.Scheme != "etcd3" {
		return nil, newError(ErrorUnsupported, "DCS_URL", source.Name, "only etcd3:// is supported", nil)
	}
	if parsed.User != nil {
		return nil, newError(ErrorProjection, "DCS_URL", source.Name, "userinfo credentials are not accepted; use protected config fields", nil)
	}
	if parsed.Hostname() == "" {
		return nil, newError(ErrorProjection, "DCS_URL", source.Name, "etcd3 host is required", nil)
	}
	port := parsed.Port()
	if port == "" {
		port = "2379"
	}
	endpoint := net.JoinHostPort(parsed.Hostname(), port)
	if !strings.Contains(parsed.Hostname(), ":") {
		endpoint = parsed.Hostname() + ":" + port
	}
	for _, section := range patroniDCSSections {
		deleteTopLevel(effective, sources, section)
	}
	etcd := map[string]any{"host": endpoint, "protocol": "http"}
	replaceTopLevel(effective, sources, "etcd3", etcd, source)
	if namespace := strings.Trim(parsed.Path, "/"); namespace != "" {
		replaceTopLevel(effective, sources, "namespace", namespace, source)
	}
	if schemeLess {
		return &Warning{
			Code: WarningSchemeLessDCSURL, Field: "DCS_URL",
			Message: "scheme-less DCS_URL is interpreted as etcd3 for compatibility",
		}, nil
	}
	return nil, nil
}

func normalizeNamespace(value string) string {
	rawParts := strings.FieldsFunc(strings.TrimSpace(value), func(r rune) bool { return r == '/' })
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "/service"
	}
	return "/" + strings.Join(parts, "/")
}

func sourceDescription(sources map[string]Source, field string) string {
	if source, ok := sources[field]; ok {
		return fmt.Sprintf("%s:%s", source.Layer, source.Name)
	}
	return "unknown"
}
