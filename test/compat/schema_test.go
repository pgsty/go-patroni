package compat_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestMachineSchemaSkeletons(t *testing.T) {
	for _, name := range []string{"result", "error", "cluster-list", "cluster-discovery", "cluster-topology-list", "effective-configuration", "version-info"} {
		path := filepath.Join(repositoryRoot(t), "schema", "machine", "v1alpha1", name+".schema.json")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var schema map[string]any
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
			t.Errorf("%s does not use JSON Schema 2020-12", name)
		}
		if !strings.Contains(stringValue(schema["$id"]), "/v1alpha1/") {
			t.Errorf("%s lacks versioned schema id", name)
		}
		required, ok := schema["required"].([]any)
		if !ok || !containsStrings(required, "apiVersion", "kind", "metadata") {
			t.Errorf("%s lacks required envelope fields", name)
		}
	}
}

func TestMachineGoldenDocumentsValidateAgainstPublishedSchemas(t *testing.T) {
	root := repositoryRoot(t)
	tests := []struct {
		schema string
		golden string
	}{
		{"error", "machine-usage-error.golden.json"},
		{"error", "machine-runtime-error.golden.json"},
		{"cluster-list", "machine-cluster-list.golden.json"},
		{"cluster-list", "machine-cluster-list-all.golden.json"},
		{"cluster-discovery", "machine-cluster-discovery.golden.json"},
		{"cluster-topology-list", "machine-cluster-topology-list.golden.json"},
		{"effective-configuration", "machine-effective-configuration.golden.json"},
		{"version-info", "machine-local-version.golden.json"},
		{"version-info", "machine-cluster-version.golden.json"},
	}
	compiled := make(map[string]*jsonschema.Schema)
	for _, test := range tests {
		t.Run(test.schema+"/"+test.golden, func(t *testing.T) {
			schema := compiled[test.schema]
			if schema == nil {
				schema = compileMachineSchema(t, root, test.schema)
				compiled[test.schema] = schema
			}
			path := filepath.Join(root, "internal", "cli", "testdata", test.golden)
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			instance, err := jsonschema.UnmarshalJSON(file)
			closeErr := file.Close()
			if err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
			if closeErr != nil {
				t.Fatalf("close %s: %v", path, closeErr)
			}
			if err := schema.Validate(instance); err != nil {
				t.Fatalf("%s does not match %s: %v", test.golden, test.schema, err)
			}
		})
	}
}

func TestMachineErrorSchemaRejectsContractDrift(t *testing.T) {
	root := repositoryRoot(t)
	schema := compileMachineSchema(t, root, "error")
	data, err := os.ReadFile(filepath.Join(root, "internal", "cli", "testdata", "machine-runtime-error.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	document["data"] = map[string]any{}
	if err := schema.Validate(document); err == nil {
		t.Fatal("error schema accepted both error and data")
	}
	delete(document, "data")
	document["metadata"].(map[string]any)["warnings"] = nil
	if err := schema.Validate(document); err == nil {
		t.Fatal("error schema accepted null warnings")
	}
}

type machineContract struct {
	SchemaVersion string                `json:"schemaVersion"`
	APIVersion    string                `json:"apiVersion"`
	Kinds         []machineKindContract `json:"kinds"`
	Enums         []machineEnumContract `json:"enums"`
	ExitCodes     []machineExitContract `json:"exitCodes"`
}

type machineKindContract struct {
	Kind     string                 `json:"kind"`
	DataType string                 `json:"dataType"`
	Golden   string                 `json:"golden"`
	Fields   []machineFieldContract `json:"fields"`
}

type machineFieldContract struct {
	Path     string   `json:"path"`
	Type     string   `json:"type"`
	Required bool     `json:"required"`
	Format   string   `json:"format,omitempty"`
	Constant any      `json:"const,omitempty"`
	Enum     []string `json:"enum,omitempty"`
}

type machineEnumContract struct {
	Type   string   `json:"type"`
	Values []string `json:"values"`
}

type machineExitContract struct {
	Category string `json:"category"`
	Code     int    `json:"code"`
}

func TestEveryMachineSuccessGoldenValidatesAgainstExhaustiveSchema(t *testing.T) {
	root := repositoryRoot(t)
	contract := readMachineContract(t, filepath.Join(root, "schema", "machine", "v1alpha1", "contract.json"))
	schema := compileMachineSchema(t, root, "result")
	seenGoldens := make(map[string]bool)
	successKinds := 0
	for _, kind := range contract.Kinds {
		if kind.Kind == "Error" {
			continue
		}
		successKinds++
		if kind.Golden == "" || seenGoldens[kind.Golden] {
			t.Fatalf("kind %s has missing or duplicate golden path %q", kind.Kind, kind.Golden)
		}
		seenGoldens[kind.Golden] = true
		path := filepath.Join(root, filepath.FromSlash(kind.Golden))
		instance := readMachineJSON(t, path)
		if instance["kind"] != kind.Kind {
			t.Errorf("%s kind=%v want=%s", kind.Golden, instance["kind"], kind.Kind)
		}
		metadata, ok := instance["metadata"].(map[string]any)
		if !ok || metadata["warnings"] == nil {
			t.Errorf("%s has absent or null metadata.warnings", kind.Golden)
		}
		if err := schema.Validate(instance); err != nil {
			t.Errorf("%s does not match exhaustive result schema: %v", kind.Golden, err)
		}
	}
	if successKinds != 22 {
		t.Fatalf("machine success contract kind count=%d want=22", successKinds)
	}
	directory := filepath.Join(root, "internal", "cli", "testdata", "machine-success")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	actualGoldens := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".golden.json") {
			continue
		}
		actualGoldens++
		relative := filepath.ToSlash(filepath.Join("internal", "cli", "testdata", "machine-success", entry.Name()))
		if !seenGoldens[relative] {
			t.Errorf("generated success golden %s has no contract kind", relative)
		}
	}
	if actualGoldens != successKinds {
		t.Errorf("machine success golden count=%d want=%d", actualGoldens, successKinds)
	}
}

func TestMachineResultSchemaRejectsUnknownKindAndKindDataMismatch(t *testing.T) {
	root := repositoryRoot(t)
	schema := compileMachineSchema(t, root, "result")
	dsnPath := filepath.Join(root, "internal", "cli", "testdata", "machine-success", "dsn.golden.json")
	queryPath := filepath.Join(root, "internal", "cli", "testdata", "machine-success", "query-result.golden.json")
	reloadPath := filepath.Join(root, "internal", "cli", "testdata", "machine-success", "reload-result.golden.json")

	unknown := readMachineJSON(t, dsnPath)
	unknown["kind"] = "UnregisteredResult"
	if err := schema.Validate(unknown); err == nil {
		t.Fatal("result schema accepted an unregistered kind")
	}

	mismatch := readMachineJSON(t, dsnPath)
	mismatch["data"] = readMachineJSON(t, queryPath)["data"]
	if err := schema.Validate(mismatch); err == nil {
		t.Fatal("result schema accepted QueryResult data under DSN kind")
	}

	nullCollection := readMachineJSON(t, reloadPath)
	nullCollection["data"].(map[string]any)["members"] = nil
	if err := schema.Validate(nullCollection); err == nil {
		t.Fatal("result schema accepted null required members collection")
	}

	invalidTimestamp := readMachineJSON(t, dsnPath)
	invalidTimestamp["metadata"].(map[string]any)["observedAt"] = "not-a-timestamp"
	if err := schema.Validate(invalidTimestamp); err == nil {
		t.Fatal("result schema accepted invalid metadata.observedAt")
	}

	invalidRevision := readMachineJSON(t, dsnPath)
	invalidRevision["metadata"].(map[string]any)["revision"] = true
	if err := schema.Validate(invalidRevision); err == nil {
		t.Fatal("result schema accepted a non-integer, non-string metadata.revision")
	}
}

func TestMachinePatchCompatibilityBaseline(t *testing.T) {
	root := repositoryRoot(t)
	directory := filepath.Join(root, "schema", "machine", "v1alpha1")
	baseline := readMachineContract(t, filepath.Join(directory, "compatibility-baseline.json"))
	current := readMachineContract(t, filepath.Join(directory, "contract.json"))
	if err := assertMachinePatchCompatible(baseline, current); err != nil {
		t.Fatalf("current machine contract is not patch-compatible with baseline: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*machineContract)
	}{
		{"removed kind", func(contract *machineContract) { contract.Kinds = contract.Kinds[1:] }},
		{"removed field", func(contract *machineContract) { contract.Kinds[0].Fields = contract.Kinds[0].Fields[1:] }},
		{"changed field type", func(contract *machineContract) { contract.Kinds[0].Fields[0].Type = "number" }},
		{"changed requiredness", func(contract *machineContract) {
			contract.Kinds[0].Fields[0].Required = !contract.Kinds[0].Fields[0].Required
		}},
		{"removed enum value", func(contract *machineContract) { contract.Enums[0].Values = contract.Enums[0].Values[1:] }},
		{"changed exit code", func(contract *machineContract) { contract.ExitCodes[0].Code = 17 }},
		{"reused exit code meaning", func(contract *machineContract) {
			contract.ExitCodes = append(contract.ExitCodes, machineExitContract{Category: "NEW_MEANING", Code: contract.ExitCodes[0].Code})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneMachineContract(t, current)
			test.mutate(&candidate)
			if err := assertMachinePatchCompatible(baseline, candidate); err == nil {
				t.Fatalf("compatibility check accepted %s", test.name)
			}
		})
	}

	additive := cloneMachineContract(t, current)
	additive.Kinds = append(additive.Kinds, machineKindContract{Kind: "FutureResult"})
	additive.Kinds[0].Fields = append(additive.Kinds[0].Fields, machineFieldContract{Path: "/data/future", Type: "string"})
	if err := assertMachinePatchCompatible(baseline, additive); err != nil {
		t.Fatalf("compatibility check rejected additive kind/field: %v", err)
	}
}

func assertMachinePatchCompatible(baseline, current machineContract) error {
	if baseline.SchemaVersion != current.SchemaVersion {
		return fmt.Errorf("schema version changed from %q to %q", baseline.SchemaVersion, current.SchemaVersion)
	}
	if baseline.APIVersion != current.APIVersion {
		return fmt.Errorf("API version changed from %q to %q", baseline.APIVersion, current.APIVersion)
	}
	currentKinds, err := indexMachineKinds(current.Kinds)
	if err != nil {
		return err
	}
	for _, oldKind := range baseline.Kinds {
		newKind, ok := currentKinds[oldKind.Kind]
		if !ok {
			return fmt.Errorf("kind %s was removed", oldKind.Kind)
		}
		newFields, fieldErr := indexMachineFields(newKind.Fields)
		if fieldErr != nil {
			return fmt.Errorf("kind %s: %w", oldKind.Kind, fieldErr)
		}
		for _, oldField := range oldKind.Fields {
			newField, exists := newFields[oldField.Path]
			if !exists {
				return fmt.Errorf("kind %s field %s was removed or renamed", oldKind.Kind, oldField.Path)
			}
			if oldField.Type != newField.Type || oldField.Required != newField.Required || oldField.Format != newField.Format || !reflect.DeepEqual(oldField.Constant, newField.Constant) {
				return fmt.Errorf("kind %s field %s changed type, requiredness, format, or constant", oldKind.Kind, oldField.Path)
			}
			if !stringSubset(oldField.Enum, newField.Enum) {
				return fmt.Errorf("kind %s field %s removed enum values", oldKind.Kind, oldField.Path)
			}
		}
	}

	currentEnums, err := indexMachineEnums(current.Enums)
	if err != nil {
		return err
	}
	for _, oldEnum := range baseline.Enums {
		newEnum, ok := currentEnums[oldEnum.Type]
		if !ok {
			return fmt.Errorf("enum %s was removed", oldEnum.Type)
		}
		if !stringSubset(oldEnum.Values, newEnum.Values) {
			return fmt.Errorf("enum %s removed values", oldEnum.Type)
		}
	}

	baselineExits, baselineMeanings, err := indexMachineExits(baseline.ExitCodes)
	if err != nil {
		return fmt.Errorf("baseline exit contract: %w", err)
	}
	currentExits, currentMeanings, err := indexMachineExits(current.ExitCodes)
	if err != nil {
		return fmt.Errorf("current exit contract: %w", err)
	}
	for category, oldCode := range baselineExits {
		newCode, ok := currentExits[category]
		if !ok || newCode != oldCode {
			return fmt.Errorf("exit category %s changed from code %d", category, oldCode)
		}
	}
	for code, oldCategories := range baselineMeanings {
		if !reflect.DeepEqual(oldCategories, currentMeanings[code]) {
			return fmt.Errorf("exit code %d meaning changed from %v to %v", code, oldCategories, currentMeanings[code])
		}
	}
	return nil
}

func indexMachineKinds(kinds []machineKindContract) (map[string]machineKindContract, error) {
	result := make(map[string]machineKindContract, len(kinds))
	for _, kind := range kinds {
		if kind.Kind == "" {
			return nil, fmt.Errorf("machine kind is empty")
		}
		if _, exists := result[kind.Kind]; exists {
			return nil, fmt.Errorf("duplicate machine kind %s", kind.Kind)
		}
		result[kind.Kind] = kind
	}
	return result, nil
}

func indexMachineFields(fields []machineFieldContract) (map[string]machineFieldContract, error) {
	result := make(map[string]machineFieldContract, len(fields))
	for _, field := range fields {
		if field.Path == "" {
			return nil, fmt.Errorf("machine field path is empty")
		}
		if _, exists := result[field.Path]; exists {
			return nil, fmt.Errorf("duplicate machine field %s", field.Path)
		}
		result[field.Path] = field
	}
	return result, nil
}

func indexMachineEnums(enums []machineEnumContract) (map[string]machineEnumContract, error) {
	result := make(map[string]machineEnumContract, len(enums))
	for _, enum := range enums {
		if enum.Type == "" {
			return nil, fmt.Errorf("machine enum type is empty")
		}
		if _, exists := result[enum.Type]; exists {
			return nil, fmt.Errorf("duplicate machine enum %s", enum.Type)
		}
		result[enum.Type] = enum
	}
	return result, nil
}

func indexMachineExits(exits []machineExitContract) (map[string]int, map[int][]string, error) {
	byCategory := make(map[string]int, len(exits))
	byCode := make(map[int][]string)
	for _, exit := range exits {
		if exit.Category == "" {
			return nil, nil, fmt.Errorf("exit category is empty")
		}
		if _, exists := byCategory[exit.Category]; exists {
			return nil, nil, fmt.Errorf("duplicate exit category %s", exit.Category)
		}
		byCategory[exit.Category] = exit.Code
		byCode[exit.Code] = append(byCode[exit.Code], exit.Category)
	}
	for code := range byCode {
		categories := byCode[code]
		sort.Strings(categories)
		byCode[code] = categories
	}
	return byCategory, byCode, nil
}

func stringSubset(old, current []string) bool {
	values := make(map[string]bool, len(current))
	for _, value := range current {
		values[value] = true
	}
	for _, value := range old {
		if !values[value] {
			return false
		}
	}
	return true
}

func readMachineContract(t *testing.T, path string) machineContract {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var contract machineContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return contract
}

func cloneMachineContract(t *testing.T, contract machineContract) machineContract {
	t.Helper()
	data, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	var clone machineContract
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func readMachineJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

func compileMachineSchema(t *testing.T, root, name string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	directory := filepath.Join(root, "schema", "machine", "v1alpha1")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		document, decodeErr := jsonschema.UnmarshalJSON(file)
		closeErr := file.Close()
		if decodeErr != nil {
			t.Fatalf("decode schema %s: %v", path, decodeErr)
		}
		if closeErr != nil {
			t.Fatalf("close schema %s: %v", path, closeErr)
		}
		resource := "https://patroni.pgsty.com/schema/machine/v1alpha1/" + entry.Name()
		if err := compiler.AddResource(resource, document); err != nil {
			t.Fatalf("register schema %s: %v", path, err)
		}
	}
	schema, err := compiler.Compile("https://patroni.pgsty.com/schema/machine/v1alpha1/" + name + ".schema.json")
	if err != nil {
		t.Fatalf("compile %s schema: %v", name, err)
	}
	return schema
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func containsStrings(values []any, required ...string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			seen[text] = true
		}
	}
	for _, value := range required {
		if !seen[value] {
			return false
		}
	}
	return true
}
