package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

type osEnvironment struct{}

func (osEnvironment) Lookup(key string) (string, bool) { return os.LookupEnv(key) }

// DefaultConfigPath mirrors Patroni's platform app-dir/patronictl.yaml shape.
func DefaultConfigPath() (string, error) {
	directory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config directory: %w", err)
	}
	return filepath.Join(directory, "patroni", "patronictl.yaml"), nil
}

// Load reads exactly one Patroni YAML document. PATRONICTL_CONFIG_FILE selects
// the file and is not treated as a merge layer.
func Load(ctx context.Context, request LoadRequest) (*Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	environment := request.Environment
	if environment == nil {
		environment = osEnvironment{}
	}
	path := request.Path
	explicit := path != ""
	if path == "" {
		if selected, ok := environment.Lookup("PATRONICTL_CONFIG_FILE"); ok && selected != "" {
			path = selected
			explicit = true
		}
	}
	if path == "" {
		var err error
		path, err = DefaultConfigPath()
		if err != nil {
			return nil, newError(ErrorNotFound, "PATRONICTL_CONFIG_FILE", "default", "cannot determine default patronictl config path", err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !explicit {
			return Parse(nil, path)
		}
		return nil, newError(ErrorNotFound, "PATRONICTL_CONFIG_FILE", path, "configuration file is not readable", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return Parse(data, path)
}

// Parse retains the complete raw yaml.Node while decoding a tolerant map for
// later typed projection. Unknown Patroni fields and tags are not rejected.
func Parse(data []byte, sourceName string) (*Document, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var raw yaml.Node
	err := decoder.Decode(&raw)
	if errors.Is(err, io.EOF) {
		raw = emptyDocumentNode()
	} else if err != nil {
		return nil, newError(ErrorSyntax, "", sourceName, "invalid YAML", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err == nil {
		return nil, newError(ErrorMultipleDocuments, "", sourceName, "multiple YAML documents are not supported", nil)
	} else if !errors.Is(err, io.EOF) {
		return nil, newError(ErrorSyntax, "", sourceName, "invalid trailing YAML", err)
	}
	if raw.Kind != yaml.DocumentNode || len(raw.Content) != 1 {
		return nil, newError(ErrorRootType, "", sourceName, "root must be one YAML mapping document", nil)
	}
	rootNode := raw.Content[0]
	if rootNode.Kind == yaml.ScalarNode && rootNode.Tag == "!!null" {
		raw = emptyDocumentNode()
		rootNode = raw.Content[0]
	}
	if rootNode.Kind != yaml.MappingNode {
		return nil, newError(ErrorRootType, "", sourceName, "root must be a YAML mapping", nil)
	}
	root := map[string]any{}
	if err := rootNode.Decode(&root); err != nil {
		return nil, newError(ErrorProjection, "", sourceName, "root mapping cannot be projected", err)
	}
	document := &Document{
		raw: cloneYAMLNode(&raw, make(map[*yaml.Node]*yaml.Node)), root: cloneMap(root),
		contexts: make(map[string]map[string]any), extensionName: "go_patroni", defaultContext: "default", sourceName: sourceName,
	}
	if err := document.extractSDKConfig(); err != nil {
		return nil, err
	}
	return document, nil
}

func emptyDocumentNode() yaml.Node {
	return yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode, Tag: "!!map"}}}
}

func (document *Document) extractSDKConfig() error {
	publicValue, publicOK := document.root["go_patroni"]
	legacyValue, legacyOK := document.root["boar"]
	if publicOK && publicValue != nil && legacyOK && legacyValue != nil {
		return newError(ErrorProjection, "go_patroni", document.sourceName, "cannot be combined with the legacy boar extension", nil)
	}
	name, value, ok := "go_patroni", publicValue, publicOK
	if (!ok || value == nil) && legacyOK {
		name, value, ok = "boar", legacyValue, true
	}
	document.extensionName = name
	if !ok || value == nil {
		delete(document.root, "go_patroni")
		delete(document.root, "boar")
		return nil
	}
	extension, ok := value.(map[string]any)
	if !ok {
		return newError(ErrorProjection, name, document.sourceName, "must be a mapping", nil)
	}
	if value, ok := extension["default_context"]; ok && value != nil {
		name, ok := value.(string)
		if !ok || name == "" {
			return newError(ErrorProjection, document.extensionField("default_context"), document.sourceName, "must be a non-empty string", nil)
		}
		document.defaultContext = name
	}
	if value, ok := extension["contexts"]; ok && value != nil {
		contexts, ok := value.(map[string]any)
		if !ok {
			return newError(ErrorProjection, document.extensionField("contexts"), document.sourceName, "must be a mapping", nil)
		}
		for name, rawContext := range contexts {
			contextMap, ok := rawContext.(map[string]any)
			if !ok {
				return newError(ErrorProjection, document.extensionField("contexts."+name), document.sourceName, "must be a mapping", nil)
			}
			document.contexts[name] = cloneMap(contextMap)
		}
	}
	if value, ok := extension["network"]; ok {
		document.network = cloneValue(value)
	}
	delete(document.root, "go_patroni")
	delete(document.root, "boar")
	return nil
}
