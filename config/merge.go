package config

import (
	"fmt"
	"strings"

	internalsecret "github.com/pgsty/go-patroni/internal/secret"
	"go.yaml.in/yaml/v3"
)

type resolutionLayer struct {
	values map[string]any
	source Source
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		output := make([]any, len(typed))
		for index, item := range typed {
			output[index] = cloneValue(item)
		}
		return output
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}

// applyLayer deep-merges maps, replaces lists/scalars, and preserves explicit
// null as a clearing value. Source attribution follows the winning leaf.
func applyLayer(destination map[string]any, sources map[string]Source, layer resolutionLayer) {
	applyMap(destination, layer.values, "", sources, layer.source)
}

func applyMap(destination, overlay map[string]any, prefix string, sources map[string]Source, source Source) {
	for key, value := range overlay {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		overlayMap, overlayIsMap := value.(map[string]any)
		destinationMap, destinationIsMap := destination[key].(map[string]any)
		if overlayIsMap && destinationIsMap {
			if len(overlayMap) == 0 {
				clearSourcePrefix(sources, path)
				sources[path] = source
			}
			applyMap(destinationMap, overlayMap, path, sources, source)
			continue
		}
		destination[key] = cloneValue(value)
		clearSourcePrefix(sources, path)
		assignSources(value, path, sources, source)
	}
}

func assignSources(value any, path string, sources map[string]Source, source Source) {
	if object, ok := value.(map[string]any); ok && len(object) > 0 {
		for key, nested := range object {
			assignSources(nested, path+"."+key, sources, source)
		}
		return
	}
	sources[path] = source
}

func clearSourcePrefix(sources map[string]Source, path string) {
	for existing := range sources {
		if existing == path || strings.HasPrefix(existing, path+".") {
			delete(sources, existing)
		}
	}
}

func replaceTopLevel(destination map[string]any, sources map[string]Source, key string, value any, source Source) {
	destination[key] = cloneValue(value)
	clearSourcePrefix(sources, key)
	assignSources(value, key, sources, source)
}

func deleteTopLevel(destination map[string]any, sources map[string]Source, key string) {
	delete(destination, key)
	clearSourcePrefix(sources, key)
}

// Effective returns a deep, redacted diagnostic projection. It never returns
// the retained raw node because that node may legitimately contain
// credentials. Both secret-shaped fields and credential-bearing strings are
// sanitized, including values duplicated into otherwise unknown fields.
func (resolved Resolved) Effective() map[string]any {
	known := resolved.knownSecretValues()
	redacted := redactMap(cloneMap(resolved.effective))
	return redactTextValues(redacted, known).(map[string]any)
}

// EffectiveYAML returns Effective encoded as deterministic YAML.
func (resolved Resolved) EffectiveYAML() ([]byte, error) {
	known := resolved.knownSecretValues()
	encoded, err := yaml.Marshal(resolved.Effective())
	if err != nil {
		return nil, fmt.Errorf("marshal effective configuration: %w", err)
	}
	return []byte(internalsecret.Redact(string(encoded), known...)), nil
}

func (resolved Resolved) knownSecretValues() []internalsecret.Value {
	return []internalsecret.Value{
		internalsecret.New(resolved.Etcd3.Password.Reveal()),
		internalsecret.New(resolved.Etcd3.TLS.KeyPassword.Reveal()),
		internalsecret.New(resolved.REST.Password.Reveal()),
		internalsecret.New(resolved.REST.KeyPassword.Reveal()),
	}
}

func redactTextValues(value any, known []internalsecret.Value) any {
	switch typed := value.(type) {
	case string:
		return internalsecret.Redact(typed, known...)
	case map[string]any:
		for key, nested := range typed {
			typed[key] = redactTextValues(nested, known)
		}
		return typed
	case []any:
		for index, nested := range typed {
			typed[index] = redactTextValues(nested, known)
		}
		return typed
	case []string:
		for index, nested := range typed {
			typed[index] = internalsecret.Redact(nested, known...)
		}
		return typed
	default:
		return typed
	}
}

// RedactMap returns a deep copy suitable for authenticated diagnostics and
// adapter responses. Secret-shaped keys are replaced without mutating input.
// Callers needing raw values must use their source-specific authenticated
// transport instead of this helper.
func RedactMap(input map[string]any) map[string]any {
	return redactMap(cloneMap(input))
}

func redactMap(input map[string]any) map[string]any {
	for key, value := range input {
		if isSecretKey(key) {
			if value == nil {
				continue
			}
			input[key] = Redacted
			continue
		}
		switch nested := value.(type) {
		case map[string]any:
			input[key] = redactMap(nested)
		case []any:
			input[key] = redactList(nested)
		}
	}
	return input
}

func redactList(input []any) []any {
	for index, value := range input {
		switch nested := value.(type) {
		case map[string]any:
			input[index] = redactMap(nested)
		case []any:
			input[index] = redactList(nested)
		}
	}
	return input
}

func isSecretKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	return strings.Contains(normalized, "password") || strings.Contains(normalized, "passwd") || normalized == "pwd" ||
		strings.Contains(normalized, "token") || strings.Contains(normalized, "secret") ||
		strings.Contains(normalized, "credential") || normalized == "auth" || normalized == "authorization" ||
		strings.Contains(normalized, "api_key") || strings.Contains(normalized, "access_key") ||
		strings.Contains(normalized, "private_key") || strings.Contains(normalized, "session_key") ||
		normalized == "cookie" || normalized == "set_cookie"
}
