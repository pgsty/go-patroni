package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

func project(contextName string, effective map[string]any, sources map[string]Source) (Resolved, error) {
	resolved := Resolved{
		Context: contextName, Namespace: "/service", effective: cloneMap(effective), sources: cloneSources(sources),
	}
	if value, ok := effective["namespace"]; ok && value != nil {
		namespace, ok := value.(string)
		if !ok {
			return Resolved{}, projectionError("namespace", sources, "must be a string")
		}
		resolved.Namespace = normalizeNamespace(namespace)
	}
	if value, ok := effective["scope"]; ok && value != nil {
		scope, ok := value.(string)
		if !ok {
			return Resolved{}, projectionError("scope", sources, "must be a string")
		}
		resolved.Scope = strings.TrimSpace(scope)
	}
	if citus, ok, err := optionalMap(effective, "citus", sources); err != nil {
		return Resolved{}, err
	} else if ok {
		resolved.Citus = true
		if value, present := citus["group"]; present && value != nil {
			group, ok := integer(value)
			if !ok || group < 0 {
				return Resolved{}, projectionError("citus.group", sources, "must be a non-negative integer")
			}
			resolved.Group = &group
		}
	}
	etcd, err := projectEtcd3(effective, sources)
	if err != nil {
		return Resolved{}, err
	}
	resolved.Etcd3 = etcd
	rest, err := projectREST(effective, sources)
	if err != nil {
		return Resolved{}, err
	}
	resolved.REST = rest
	return resolved, nil
}

func projectEtcd3(effective map[string]any, sources map[string]Source) (Etcd3Config, error) {
	output := Etcd3Config{Protocol: "http"}
	etcd, ok, err := optionalMap(effective, "etcd3", sources)
	if err != nil || !ok {
		return output, err
	}
	if value, present := etcd["protocol"]; present && value != nil {
		protocol, ok := value.(string)
		if !ok || protocol != "http" && protocol != "https" {
			return output, projectionError("etcd3.protocol", sources, "must be http or https")
		}
		output.Protocol = protocol
	} else {
		sources["etcd3.protocol"] = Source{Layer: LayerDefault, Name: "Patroni default"}
	}
	for _, candidate := range []struct {
		name string
		kind LocatorKind
	}{{"url", LocatorURL}, {"proxy", LocatorProxy}, {"srv", LocatorSRV}} {
		if value, present := etcd[candidate.name]; present && value != nil {
			text, ok := value.(string)
			if !ok || strings.TrimSpace(text) == "" {
				return output, projectionError("etcd3."+candidate.name, sources, "must be a non-empty string")
			}
			output.Configured, output.Locator = true, candidate.kind
			switch candidate.kind {
			case LocatorURL:
				if err := rejectURLUserinfo(text); err != nil {
					return output, projectionError("etcd3.url", sources, err.Error())
				}
				output.URL = text
			case LocatorProxy:
				if err := rejectURLUserinfo(text); err != nil {
					return output, projectionError("etcd3.proxy", sources, err.Error())
				}
				output.Proxy = text
			case LocatorSRV:
				output.SRV = text
			}
			break
		}
	}
	if !output.Configured {
		for _, candidate := range []struct {
			name string
			kind LocatorKind
		}{{"hosts", LocatorHosts}, {"host", LocatorHost}} {
			value, present := etcd[candidate.name]
			if !present || value == nil {
				continue
			}
			endpoints, err := endpointList(value)
			if err != nil || len(endpoints) == 0 {
				return output, projectionError("etcd3."+candidate.name, sources, "must contain one or more host endpoints")
			}
			output.Configured, output.Locator, output.Endpoints = true, candidate.kind, endpoints
			break
		}
	}
	output.TLS.CAFile, err = optionalString(etcd, "cacert", "etcd3.cacert", sources)
	if err != nil {
		return output, err
	}
	output.TLS.CertFile, err = optionalString(etcd, "cert", "etcd3.cert", sources)
	if err != nil {
		return output, err
	}
	output.TLS.KeyFile, err = optionalString(etcd, "key", "etcd3.key", sources)
	if err != nil {
		return output, err
	}
	keyPassword, err := optionalString(etcd, "key_password", "etcd3.key_password", sources)
	if err != nil {
		return output, err
	}
	output.TLS.KeyPassword = newSecret(keyPassword)
	output.Username, err = optionalString(etcd, "username", "etcd3.username", sources)
	if err != nil {
		return output, err
	}
	password, err := optionalString(etcd, "password", "etcd3.password", sources)
	if err != nil {
		return output, err
	}
	output.Password = newSecret(password)
	return output, nil
}

func projectREST(effective map[string]any, sources map[string]Source) (RESTConfig, error) {
	ctl, _, err := optionalMap(effective, "ctl", sources)
	if err != nil {
		return RESTConfig{}, err
	}
	restapi, _, err := optionalMap(effective, "restapi", sources)
	if err != nil {
		return RESTConfig{}, err
	}
	output := RESTConfig{}
	if value, present := ctl["insecure"]; present && value != nil {
		insecure, ok := value.(bool)
		if !ok {
			return output, projectionError("ctl.insecure", sources, "must be a boolean")
		}
		output.Insecure = insecure
	}
	output.CAFile, err = firstString(sources,
		fieldValue{ctl, "cacert", "ctl.cacert"}, fieldValue{restapi, "cafile", "restapi.cafile"})
	if err != nil {
		return output, err
	}
	output.CertFile, err = firstString(sources,
		fieldValue{ctl, "certfile", "ctl.certfile"}, fieldValue{restapi, "certfile", "restapi.certfile"})
	if err != nil {
		return output, err
	}
	output.KeyFile, err = firstString(sources,
		fieldValue{ctl, "keyfile", "ctl.keyfile"}, fieldValue{restapi, "keyfile", "restapi.keyfile"})
	if err != nil {
		return output, err
	}
	keyPassword, err := firstString(sources,
		fieldValue{ctl, "keyfile_password", "ctl.keyfile_password"}, fieldValue{restapi, "keyfile_password", "restapi.keyfile_password"})
	if err != nil {
		return output, err
	}
	output.KeyPassword = newSecret(keyPassword)
	username, password, err := firstAuthentication(ctl, restapi, sources)
	if err != nil {
		return output, err
	}
	output.Username, output.Password = username, newSecret(password)
	return output, nil
}

type fieldValue struct {
	object map[string]any
	key    string
	path   string
}

func firstString(sources map[string]Source, fields ...fieldValue) (string, error) {
	for _, field := range fields {
		value, present := field.object[field.key]
		if !present || value == nil || value == "" {
			continue
		}
		text, ok := value.(string)
		if !ok {
			return "", projectionError(field.path, sources, "must be a string")
		}
		return text, nil
	}
	return "", nil
}

func firstAuthentication(ctl, restapi map[string]any, sources map[string]Source) (string, string, error) {
	for _, field := range []fieldValue{{ctl, "auth", "ctl.auth"}, {restapi, "auth", "restapi.auth"}} {
		if value, present := field.object[field.key]; present && value != nil && value != "" {
			text, ok := value.(string)
			if !ok {
				return "", "", projectionError(field.path, sources, "must be username:password")
			}
			username, password, found := strings.Cut(text, ":")
			if !found {
				return "", "", projectionError(field.path, sources, "must be username:password")
			}
			return username, password, nil
		}
	}
	for _, field := range []fieldValue{{ctl, "authentication", "ctl.authentication"}, {restapi, "authentication", "restapi.authentication"}} {
		value, present := field.object[field.key]
		if !present || value == nil {
			continue
		}
		authentication, ok := value.(map[string]any)
		if !ok {
			return "", "", projectionError(field.path, sources, "must be a mapping")
		}
		username, err := optionalString(authentication, "username", field.path+".username", sources)
		if err != nil {
			return "", "", err
		}
		password, err := optionalString(authentication, "password", field.path+".password", sources)
		if err != nil {
			return "", "", err
		}
		if username != "" || password != "" {
			return username, password, nil
		}
	}
	return "", "", nil
}

func optionalMap(object map[string]any, key string, sources map[string]Source) (map[string]any, bool, error) {
	value, present := object[key]
	if !present || value == nil {
		return map[string]any{}, false, nil
	}
	mapping, ok := value.(map[string]any)
	if !ok {
		return nil, false, projectionError(key, sources, "must be a mapping")
	}
	return mapping, true, nil
}

func optionalString(object map[string]any, key, path string, sources map[string]Source) (string, error) {
	value, present := object[key]
	if !present || value == nil {
		return "", nil
	}
	text, ok := value.(string)
	if !ok {
		return "", projectionError(path, sources, "must be a string")
	}
	return text, nil
}

func endpointList(value any) ([]string, error) {
	var raw []string
	switch typed := value.(type) {
	case string:
		raw = strings.Split(typed, ",")
	case []any:
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("endpoint is not a string")
			}
			raw = append(raw, text)
		}
	case []string:
		raw = append(raw, typed...)
	default:
		return nil, fmt.Errorf("unsupported endpoint list type %T", value)
	}
	endpoints := make([]string, 0, len(raw))
	for _, endpoint := range raw {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		normalized, err := normalizeEndpoint(endpoint)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, normalized)
	}
	return endpoints, nil
}

func normalizeEndpoint(endpoint string) (string, error) {
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Hostname() == "" {
			return "", fmt.Errorf("invalid endpoint URL")
		}
		if parsed.User != nil {
			return "", fmt.Errorf("endpoint URL userinfo is not accepted")
		}
		endpoint = parsed.Host
	}
	if host, port, err := net.SplitHostPort(endpoint); err == nil {
		if host == "" || port == "" {
			return "", fmt.Errorf("endpoint host and port are required")
		}
		if err := validatePort(port); err != nil {
			return "", err
		}
		return endpoint, nil
	}
	if strings.Count(endpoint, ":") > 1 {
		return net.JoinHostPort(strings.Trim(endpoint, "[]"), "2379"), nil
	}
	if strings.Contains(endpoint, ":") {
		host, port, found := strings.Cut(endpoint, ":")
		if !found || host == "" || port == "" {
			return "", fmt.Errorf("endpoint host and port are required")
		}
		if err := validatePort(port); err != nil {
			return "", err
		}
		return endpoint, nil
	}
	return endpoint + ":2379", nil
}

func validatePort(value string) error {
	port, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("endpoint port must be numeric")
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("endpoint port must be between 1 and 65535")
	}
	return nil
}

func rejectURLUserinfo(value string) error {
	if !strings.Contains(value, "://") {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("must be a valid URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("userinfo credentials are not accepted; use protected username/password fields")
	}
	return nil
}

func integer(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case uint64:
		return int(typed), uint64(int(typed)) == typed
	default:
		return 0, false
	}
}

func cloneSources(input map[string]Source) map[string]Source {
	output := make(map[string]Source, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func projectionError(field string, sources map[string]Source, message string) *Error {
	return newError(ErrorProjection, field, sourceDescription(sources, field), message, nil)
}
