// Command machineschema generates the exhaustive Go patronictl CLI machine-result
// schema, per-kind canonical examples, and a structural compatibility
// contract from the adapter-owned kind/type catalog.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	pathpkg "path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	sdkcli "github.com/pgsty/go-patroni/internal/cli"
	"github.com/pgsty/go-patroni/model"
)

const (
	machineAPIVersion = "patroni.pgsty.com/v1alpha1"
	schemaVersion     = "patroni.pgsty.com/machine-contract/v1alpha1"
	schemaBase        = "https://patroni.pgsty.com/schema/machine/v1alpha1/"
)

type options struct {
	directory      string
	goldenDir      string
	check          bool
	writeBaseline  bool
	baselineOutput string
}

type schemaGenerator struct {
	defs    map[string]any
	aliases map[reflect.Type]string
	enums   map[reflect.Type][]string
}

type contractDocument struct {
	SchemaVersion string             `json:"schemaVersion"`
	APIVersion    string             `json:"apiVersion"`
	GeneratedBy   string             `json:"generatedBy"`
	Kinds         []kindContract     `json:"kinds"`
	Enums         []enumContract     `json:"enums"`
	ExitCodes     []exitCodeContract `json:"exitCodes"`
}

type kindContract struct {
	Kind     string          `json:"kind"`
	DataType string          `json:"dataType"`
	Golden   string          `json:"golden"`
	Fields   []fieldContract `json:"fields"`
}

type fieldContract struct {
	Path     string   `json:"path"`
	Type     string   `json:"type"`
	Required bool     `json:"required"`
	Format   string   `json:"format,omitempty"`
	Constant any      `json:"const,omitempty"`
	Enum     []string `json:"enum,omitempty"`
}

type enumContract struct {
	Type   string   `json:"type"`
	Values []string `json:"values"`
}

type exitCodeContract struct {
	Category string `json:"category"`
	Code     int    `json:"code"`
}

type exampleEnvelope struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   exampleMetadata `json:"metadata"`
	Data       any             `json:"data"`
}

type exampleMetadata struct {
	RequestID  string   `json:"requestId"`
	ObservedAt string   `json:"observedAt"`
	Warnings   []string `json:"warnings"`
}

func main() {
	configuration := options{}
	flag.StringVar(&configuration.directory, "directory", "schema/machine/v1alpha1", "machine schema output directory")
	flag.StringVar(&configuration.goldenDir, "golden-directory", "internal/cli/testdata/machine-success", "per-kind golden output directory")
	flag.BoolVar(&configuration.check, "check", false, "verify generated files without writing")
	flag.BoolVar(&configuration.writeBaseline, "write-baseline", false, "write the initial compatibility baseline explicitly")
	flag.StringVar(&configuration.baselineOutput, "baseline", "", "compatibility baseline path")
	flag.Parse()
	if configuration.baselineOutput == "" {
		configuration.baselineOutput = filepath.Join(configuration.directory, "compatibility-baseline.json")
	}
	if err := run(configuration); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(configuration options) error {
	catalog := sdkcli.MachineSchemaCatalog()
	metadataType, errorType := sdkcli.MachineSchemaEnvelopeTypes()
	if len(catalog) == 0 {
		return fmt.Errorf("machine schema catalog is empty")
	}
	generator := &schemaGenerator{
		defs:    make(map[string]any),
		aliases: map[reflect.Type]string{metadataType: "metadata"},
		enums:   enumValues(),
	}
	resultSchema, err := generator.resultSchema(catalog, metadataType)
	if err != nil {
		return err
	}
	resultBytes, err := marshalDocument(resultSchema)
	if err != nil {
		return fmt.Errorf("encode result schema: %w", err)
	}
	contract := generator.contract(catalog, metadataType, errorType, configuration.goldenDir)
	contractBytes, err := marshalDocument(contract)
	if err != nil {
		return fmt.Errorf("encode machine contract: %w", err)
	}

	outputs := map[string][]byte{
		filepath.Join(configuration.directory, "result.schema.json"): resultBytes,
		filepath.Join(configuration.directory, "contract.json"):      contractBytes,
	}
	for kind, dataType := range catalog {
		data, populateErr := exampleValue(dataType, "data", generator.enums)
		if populateErr != nil {
			return fmt.Errorf("build %s example: %w", kind, populateErr)
		}
		document := exampleEnvelope{
			APIVersion: machineAPIVersion,
			Kind:       kind,
			Metadata: exampleMetadata{
				RequestID:  "machine-schema-example",
				ObservedAt: "2026-07-13T12:34:56Z",
				Warnings:   []string{},
			},
			Data: data.Interface(),
		}
		encoded, encodeErr := marshalGolden(document)
		if encodeErr != nil {
			return fmt.Errorf("encode %s example: %w", kind, encodeErr)
		}
		outputs[filepath.Join(configuration.goldenDir, goldenName(kind))] = encoded
	}

	if configuration.check {
		if configuration.writeBaseline {
			return fmt.Errorf("-check and -write-baseline are mutually exclusive")
		}
		if err := checkOutputs(outputs); err != nil {
			return err
		}
		return checkGoldenDirectory(configuration.goldenDir, outputs)
	}
	for name, data := range outputs {
		if err := writeGenerated(name, data); err != nil {
			return err
		}
	}
	if err := removeStaleGoldens(configuration.goldenDir, outputs); err != nil {
		return err
	}
	if configuration.writeBaseline {
		if _, err := os.Stat(configuration.baselineOutput); err == nil {
			return fmt.Errorf("refusing to overwrite existing compatibility baseline %s", configuration.baselineOutput)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := writeGenerated(configuration.baselineOutput, contractBytes); err != nil {
			return err
		}
	}
	return nil
}

func (generator *schemaGenerator) resultSchema(catalog map[string]reflect.Type, metadataType reflect.Type) (map[string]any, error) {
	kinds := sortedKeys(catalog)
	metadataSchema, err := generator.schemaFor(metadataType, "metadata")
	if err != nil {
		return nil, err
	}
	branches := make([]any, 0, len(kinds))
	for _, kind := range kinds {
		dataSchema, schemaErr := generator.schemaFor(catalog[kind], "data")
		if schemaErr != nil {
			return nil, fmt.Errorf("schema %s: %w", kind, schemaErr)
		}
		branches = append(branches, map[string]any{
			"required": []string{"kind", "data"},
			"properties": map[string]any{
				"kind": map[string]any{"const": kind},
				"data": dataSchema,
			},
		})
	}
	return map[string]any{
		"$schema":              "https://json-schema.org/draft/2020-12/schema",
		"$id":                  schemaBase + "result.schema.json",
		"title":                "Go patronictl v1alpha1 exhaustive success envelope",
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"apiVersion", "kind", "metadata", "data"},
		"properties": map[string]any{
			"apiVersion": map[string]any{"const": machineAPIVersion},
			"kind":       map[string]any{"enum": kinds},
			"metadata":   metadataSchema,
			"data":       true,
		},
		"oneOf": branches,
		"$defs": generator.defs,
	}, nil
}

func (generator *schemaGenerator) schemaFor(dataType reflect.Type, path string) (any, error) {
	for dataType.Kind() == reflect.Pointer {
		dataType = dataType.Elem()
	}
	if dataType == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}, nil
	}
	if values, ok := generator.enums[dataType]; ok {
		return map[string]any{"type": "string", "enum": append([]string(nil), values...)}, nil
	}
	switch dataType.Kind() {
	case reflect.Interface:
		return true, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.String:
		return map[string]any{"type": "string"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"type": "integer"}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		schema := map[string]any{"type": "integer", "minimum": 0}
		if dataType.Kind() == reflect.Uint16 {
			schema["maximum"] = 65535
		}
		return schema, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, nil
	case reflect.Slice, reflect.Array:
		items, err := generator.schemaFor(dataType.Elem(), path+"/*")
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case reflect.Map:
		if dataType.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map %s has non-string key", dataType)
		}
		values, err := generator.schemaFor(dataType.Elem(), path+"/*")
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "object", "additionalProperties": values}, nil
	case reflect.Struct:
		name := generator.definitionName(dataType)
		if _, exists := generator.defs[name]; !exists {
			generator.defs[name] = map[string]any{}
			definition, err := generator.structDefinition(dataType, path)
			if err != nil {
				return nil, err
			}
			generator.defs[name] = definition
		}
		return map[string]any{"$ref": "#/$defs/" + name}, nil
	default:
		return nil, fmt.Errorf("unsupported Go kind %s for %s", dataType.Kind(), dataType)
	}
}

func (generator *schemaGenerator) structDefinition(dataType reflect.Type, path string) (map[string]any, error) {
	properties := make(map[string]any)
	required := make([]string, 0, dataType.NumField())
	for index := 0; index < dataType.NumField(); index++ {
		field := dataType.Field(index)
		name, omitEmpty, include := jsonField(field)
		if !include {
			continue
		}
		fieldSchema, err := generator.schemaFor(field.Type, path+"/"+name)
		if err != nil {
			return nil, err
		}
		fieldSchema = applyFieldSchema(fieldSchema, dataType, name)
		properties[name] = fieldSchema
		if !omitEmpty {
			required = append(required, name)
		}
	}
	sort.Strings(required)
	definition := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if len(required) > 0 {
		definition["required"] = required
	}
	return definition, nil
}

func applyFieldSchema(schema any, parent reflect.Type, name string) any {
	if parent.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && parent.Name() == "machineMetadata" && name == "revision" {
		return map[string]any{"oneOf": []any{map[string]any{"type": "integer"}, map[string]any{"type": "string", "minLength": 1}}}
	}
	object, ok := schema.(map[string]any)
	if !ok {
		return schema
	}
	copySchema := make(map[string]any, len(object)+2)
	for key, value := range object {
		copySchema[key] = value
	}
	if isRFC3339Field(parent, name) {
		copySchema["format"] = "date-time"
	}
	if parent.PkgPath() == "github.com/pgsty/go-patroni/model" && parent.Name() == "Target" && name == "namespace" {
		copySchema["pattern"] = "^/"
	}
	if name == "httpStatus" {
		copySchema["minimum"] = 0
		copySchema["maximum"] = 599
	}
	if parent.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && parent.Name() == "machineVersionInfo" {
		switch name {
		case "supportedPatroni":
			copySchema["const"] = ">=3.0.0,<5.0.0"
		case "machineSchema":
			copySchema["const"] = machineAPIVersion
		}
	}
	if parent.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && parent.Name() == "machineError" && name == "cause" {
		copySchema["enum"] = machineCauses()
	}
	return copySchema
}

func (generator *schemaGenerator) definitionName(dataType reflect.Type) string {
	if alias, ok := generator.aliases[dataType]; ok {
		return alias
	}
	pkg := pathpkg.Base(dataType.PkgPath())
	if pkg == "." || pkg == "/" || pkg == "" {
		pkg = "builtin"
	}
	return sanitizeName(pkg + "_" + dataType.Name())
}

func (generator *schemaGenerator) contract(
	catalog map[string]reflect.Type,
	metadataType reflect.Type,
	errorType reflect.Type,
	goldenDirectory string,
) contractDocument {
	kinds := make([]kindContract, 0, len(catalog)+1)
	for _, kind := range sortedKeys(catalog) {
		fields := envelopeFields(kind, "data", metadataType, catalog[kind], generator.enums)
		kinds = append(kinds, kindContract{
			Kind: kind, DataType: typeName(catalog[kind]), Golden: filepath.ToSlash(filepath.Join(goldenDirectory, goldenName(kind))), Fields: fields,
		})
	}
	kinds = append(kinds, kindContract{
		Kind: "Error", DataType: typeName(errorType), Golden: "internal/cli/testdata/machine-runtime-error.golden.json",
		Fields: envelopeFields("Error", "error", metadataType, errorType, generator.enums),
	})
	sort.Slice(kinds, func(left, right int) bool { return kinds[left].Kind < kinds[right].Kind })
	enums := make([]enumContract, 0, len(generator.enums))
	for dataType, values := range generator.enums {
		enums = append(enums, enumContract{Type: typeName(dataType), Values: append([]string(nil), values...)})
	}
	sort.Slice(enums, func(left, right int) bool { return enums[left].Type < enums[right].Type })
	return contractDocument{
		SchemaVersion: schemaVersion,
		APIVersion:    machineAPIVersion,
		GeneratedBy:   "tools/machineschema",
		Kinds:         kinds,
		Enums:         enums,
		ExitCodes: []exitCodeContract{
			{Category: "SUCCESS", Code: 0},
			{Category: string(control.CategoryFailed), Code: control.ExitCode(control.CategoryFailed)},
			{Category: string(control.CategoryUsage), Code: control.ExitCode(control.CategoryUsage)},
			{Category: string(control.CategoryConfig), Code: control.ExitCode(control.CategoryConfig)},
			{Category: string(control.CategoryUnsupported), Code: control.ExitCode(control.CategoryUnsupported)},
			{Category: string(control.CategoryAuth), Code: control.ExitCode(control.CategoryAuth)},
			{Category: string(control.CategoryTLS), Code: control.ExitCode(control.CategoryTLS)},
			{Category: string(control.CategoryNotFound), Code: control.ExitCode(control.CategoryNotFound)},
			{Category: string(control.CategoryConflict), Code: control.ExitCode(control.CategoryConflict)},
			{Category: string(control.CategoryUnreachable), Code: control.ExitCode(control.CategoryUnreachable)},
			{Category: string(control.CategoryUnknown), Code: control.ExitCode(control.CategoryUnknown)},
			{Category: string(control.CategoryInternal), Code: control.ExitCode(control.CategoryInternal)},
		},
	}
}

func envelopeFields(kind, payloadName string, metadataType, payloadType reflect.Type, enums map[reflect.Type][]string) []fieldContract {
	fields := []fieldContract{
		{Path: "/apiVersion", Type: "string", Required: true, Constant: machineAPIVersion},
		{Path: "/kind", Type: "string", Required: true, Constant: kind},
		{Path: "/metadata", Type: "object", Required: true},
		{Path: "/" + payloadName, Type: typeDescription(payloadType), Required: true},
	}
	flattenFields(&fields, "/metadata", metadataType, true, enums, map[reflect.Type]int{})
	flattenFields(&fields, "/"+payloadName, payloadType, true, enums, map[reflect.Type]int{})
	sort.Slice(fields, func(left, right int) bool { return fields[left].Path < fields[right].Path })
	return fields
}

func flattenFields(
	output *[]fieldContract,
	prefix string,
	dataType reflect.Type,
	parentPresent bool,
	enums map[reflect.Type][]string,
	stack map[reflect.Type]int,
) {
	for dataType.Kind() == reflect.Pointer {
		dataType = dataType.Elem()
	}
	if dataType == reflect.TypeOf(time.Time{}) || stack[dataType] > 1 {
		return
	}
	switch dataType.Kind() {
	case reflect.Struct:
		stack[dataType]++
		defer func() { stack[dataType]-- }()
		for index := 0; index < dataType.NumField(); index++ {
			field := dataType.Field(index)
			name, omitEmpty, include := jsonField(field)
			if !include {
				continue
			}
			path := prefix + "/" + name
			contract := fieldContract{Path: path, Type: typeDescription(field.Type), Required: parentPresent && !omitEmpty}
			if dataType.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && dataType.Name() == "machineMetadata" && name == "revision" {
				contract.Type = "integer|string"
			}
			base := field.Type
			for base.Kind() == reflect.Pointer {
				base = base.Elem()
			}
			if values, ok := enums[base]; ok {
				contract.Enum = append([]string(nil), values...)
			}
			if base == reflect.TypeOf(time.Time{}) || isRFC3339Field(dataType, name) {
				contract.Format = "date-time"
			}
			if dataType.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && dataType.Name() == "machineVersionInfo" {
				switch name {
				case "supportedPatroni":
					contract.Constant = ">=3.0.0,<5.0.0"
				case "machineSchema":
					contract.Constant = machineAPIVersion
				}
			}
			if dataType.PkgPath() == "github.com/pgsty/go-patroni/internal/cli" && dataType.Name() == "machineError" && name == "cause" {
				contract.Enum = machineCauses()
			}
			*output = append(*output, contract)
			flattenFields(output, path, field.Type, parentPresent && !omitEmpty, enums, stack)
		}
	case reflect.Slice, reflect.Array:
		flattenFields(output, prefix+"/*", dataType.Elem(), parentPresent, enums, stack)
	}
}

func exampleValue(dataType reflect.Type, fieldName string, enums map[reflect.Type][]string) (reflect.Value, error) {
	if dataType.Kind() == reflect.Pointer {
		if dataType == reflect.TypeOf((*control.Error)(nil)) || dataType == reflect.TypeOf((*control.QueryError)(nil)) {
			return reflect.Zero(dataType), nil
		}
		value, err := exampleValue(dataType.Elem(), fieldName, enums)
		if err != nil {
			return reflect.Value{}, err
		}
		pointer := reflect.New(dataType.Elem())
		pointer.Elem().Set(value)
		return pointer, nil
	}
	if dataType == reflect.TypeOf(time.Time{}) {
		return reflect.ValueOf(time.Date(2026, 7, 13, 12, 34, 56, 0, time.UTC)), nil
	}
	if values, ok := enums[dataType]; ok && len(values) > 0 {
		value := reflect.New(dataType).Elem()
		switch dataType {
		case reflect.TypeOf(control.SendState("")):
			value.SetString(string(control.SendAccepted))
		case reflect.TypeOf(control.Verification("")):
			value.SetString(string(control.VerifiedSucceeded))
		default:
			value.SetString(values[0])
		}
		return value, nil
	}
	value := reflect.New(dataType).Elem()
	switch dataType.Kind() {
	case reflect.Interface:
		value.Set(reflect.ValueOf("example"))
	case reflect.Bool:
		value.SetBool(false)
	case reflect.String:
		value.SetString(exampleString(dataType, fieldName))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(1)
	case reflect.Float32, reflect.Float64:
		value.SetFloat(1)
	case reflect.Struct:
		for index := 0; index < dataType.NumField(); index++ {
			field := dataType.Field(index)
			name, _, include := jsonField(field)
			if !include || !value.Field(index).CanSet() {
				continue
			}
			item, err := exampleValue(field.Type, name, enums)
			if err != nil {
				return reflect.Value{}, err
			}
			value.Field(index).Set(item)
		}
	case reflect.Slice:
		value = reflect.MakeSlice(dataType, 1, 1)
		item, err := exampleValue(dataType.Elem(), fieldName, enums)
		if err != nil {
			return reflect.Value{}, err
		}
		value.Index(0).Set(item)
	case reflect.Array:
		for index := 0; index < dataType.Len(); index++ {
			item, err := exampleValue(dataType.Elem(), fieldName, enums)
			if err != nil {
				return reflect.Value{}, err
			}
			value.Index(index).Set(item)
		}
	case reflect.Map:
		if dataType.Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("unsupported example map %s", dataType)
		}
		value = reflect.MakeMapWithSize(dataType, 1)
		item, err := exampleValue(dataType.Elem(), fieldName, enums)
		if err != nil {
			return reflect.Value{}, err
		}
		value.SetMapIndex(reflect.ValueOf("example").Convert(dataType.Key()), item)
	default:
		return reflect.Value{}, fmt.Errorf("unsupported example type %s", dataType)
	}
	return value, nil
}

func exampleString(parent reflect.Type, name string) string {
	if isRFC3339Field(parent, name) {
		return "2026-07-13T12:34:56Z"
	}
	switch name {
	case "observedAt", "schedule", "at", "timestamp", "scheduledAt":
		return "2026-07-13T12:34:56Z"
	case "context":
		return "default"
	case "namespace":
		return "/service"
	case "scope":
		return "alpha"
	case "member", "name", "leader", "candidate", "previousLeader", "newLeader", "parent":
		return "node-a"
	case "host":
		return "node-a.example.invalid"
	case "supportedPatroni":
		return ">=3.0.0,<5.0.0"
	case "machineSchema":
		return machineAPIVersion
	case "cause":
		return machineCauses()[0]
	case "apiUrl":
		return "https://node-a.example.invalid:8008"
	case "path":
		return "/patroni"
	default:
		return "example"
	}
}

func jsonField(field reflect.StructField) (string, bool, bool) {
	if field.PkgPath != "" {
		return "", false, false
	}
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	if len(parts) > 0 && parts[0] == "-" {
		return "", false, false
	}
	name := field.Name
	if len(parts) > 0 && parts[0] != "" {
		name = parts[0]
	}
	omitEmpty := false
	for _, option := range parts[1:] {
		if option == "omitempty" || option == "omitzero" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, true
}

func enumValues() map[reflect.Type][]string {
	return map[reflect.Type][]string{
		reflect.TypeOf(config.Layer("")): {
			string(config.LayerDefault), string(config.LayerFile), string(config.LayerContext), string(config.LayerEnvironment), string(config.LayerFlag),
		},
		reflect.TypeOf(config.WarningCode("")): {
			string(config.WarningSchemeLessDCSURL), string(config.WarningInsecureRESTTLS),
		},
		reflect.TypeOf(control.Category("")): {
			string(control.CategoryUsage), string(control.CategoryConfig), string(control.CategoryUnsupported), string(control.CategoryAuth),
			string(control.CategoryTLS), string(control.CategoryNotFound), string(control.CategoryConflict), string(control.CategoryUnreachable),
			string(control.CategoryFailed), string(control.CategoryUnknown), string(control.CategoryInternal),
		},
		reflect.TypeOf(control.Outcome("")): {
			string(control.Succeeded), string(control.Failed), string(control.Unknown),
		},
		reflect.TypeOf(control.SendState("")): {
			string(control.SendNotSent), string(control.SendMaybeSent), string(control.SendAccepted),
		},
		reflect.TypeOf(control.Verification("")): {
			string(control.Unverified), string(control.VerifiedSucceeded), string(control.VerifiedFailed),
		},
		reflect.TypeOf(control.EvidenceSource("")): {
			string(control.EvidenceLocal), string(control.EvidenceDCS), string(control.EvidencePatroni), string(control.EvidencePostgres), string(control.EvidenceControl),
		},
		reflect.TypeOf(control.Path("")): {
			string(control.PathLocal), string(control.PathDCS), string(control.PathREST), string(control.PathPostgres), string(control.PathRESTToDCS),
		},
		reflect.TypeOf(control.Role("")): {
			string(control.RoleLeader), string(control.RolePrimary), string(control.RoleStandbyLeader), string(control.RoleReplica), string(control.RoleStandby), string(control.RoleAny),
		},
		reflect.TypeOf(control.FlushEvent("")): {
			string(control.FlushRestart), string(control.FlushSwitchover),
		},
		reflect.TypeOf(control.QueryErrorKind("")): {
			string(control.QueryErrorNoConnection), string(control.QueryErrorDatabase), string(control.QueryErrorRoleMismatch),
		},
		reflect.TypeOf(model.MemberRole("")): {
			string(model.RoleLeader), string(model.RoleStandbyLeader), string(model.RoleSyncStandby), string(model.RoleQuorumStandby), string(model.RoleReplica),
		},
		reflect.TypeOf(model.DiscoveryState("")): {
			string(model.DiscoveryDiscovered), string(model.DiscoveryConfiguredOnly), string(model.DiscoveryAbsent),
		},
		reflect.TypeOf(model.ManagementState("")): {
			string(model.ManagementExplicit), string(model.ManagementAllSelected), string(model.ManagementUnmanaged),
		},
		reflect.TypeOf(model.ReachabilityState("")): {
			string(model.ReachabilityReachable), string(model.ReachabilityPartiallyReachable), string(model.ReachabilityUnreachable), string(model.ReachabilityUnknown),
		},
		reflect.TypeOf(model.HealthState("")): {
			string(model.HealthHealthy), string(model.HealthDegraded), string(model.HealthUnhealthy), string(model.HealthUnknown),
		},
	}
}

func machineCauses() []string {
	return []string{
		"INVALID_INPUT", "INVALID_CONFIGURATION", "UNSUPPORTED_PATRONI_VERSION", "AUTHENTICATION_REJECTED",
		"TLS_CONFIGURATION_OR_VERIFICATION_FAILED", "TARGET_NOT_FOUND", "CONCURRENT_STATE_CONFLICT",
		"UPSTREAM_UNREACHABLE", "OPERATION_REJECTED", "OUTCOME_UNCONFIRMED", "INTERNAL_INVARIANT",
	}
}

func isRFC3339Field(parent reflect.Type, name string) bool {
	key := parent.PkgPath() + "." + parent.Name() + "." + name
	switch key {
	case "github.com/pgsty/go-patroni/internal/cli.machineMetadata.observedAt",
		"github.com/pgsty/go-patroni/model.ScheduledRestart.schedule",
		"github.com/pgsty/go-patroni/model.ScheduledSwitchover.at",
		"github.com/pgsty/go-patroni/control.HistoryEntry.timestamp",
		"github.com/pgsty/go-patroni/control.ClusterWriteData.scheduledAt":
		return true
	default:
		return false
	}
}

func typeDescription(dataType reflect.Type) string {
	for dataType.Kind() == reflect.Pointer {
		dataType = dataType.Elem()
	}
	if dataType == reflect.TypeOf(time.Time{}) {
		return "string"
	}
	switch dataType.Kind() {
	case reflect.Interface:
		return "any"
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Struct:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map:
		return "object-map"
	default:
		return dataType.Kind().String()
	}
}

func typeName(dataType reflect.Type) string {
	for dataType.Kind() == reflect.Pointer {
		dataType = dataType.Elem()
	}
	if dataType.PkgPath() == "" {
		return dataType.String()
	}
	return pathpkg.Base(dataType.PkgPath()) + "." + dataType.Name()
}

func goldenName(kind string) string {
	var output []rune
	input := []rune(kind)
	for index, value := range input {
		if unicode.IsUpper(value) && index > 0 && (unicode.IsLower(input[index-1]) || index+1 < len(input) && unicode.IsLower(input[index+1])) {
			output = append(output, '-')
		}
		output = append(output, unicode.ToLower(value))
	}
	return string(output) + ".golden.json"
}

func sanitizeName(value string) string {
	return strings.Map(func(character rune) rune {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || character == '_' || character == '-' {
			return character
		}
		return '_'
	}, value)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func marshalDocument(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func marshalGolden(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func writeGenerated(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(name, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

func checkOutputs(outputs map[string][]byte) error {
	for _, name := range sortedKeys(outputs) {
		actual, err := os.ReadFile(name)
		if err != nil {
			return fmt.Errorf("generated file %s is missing or unreadable: %w", name, err)
		}
		if !bytes.Equal(actual, outputs[name]) {
			return fmt.Errorf("generated file %s is stale; run make generate", name)
		}
	}
	return nil
}

func checkGoldenDirectory(directory string, outputs map[string][]byte) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".golden.json") {
			continue
		}
		name := filepath.Join(directory, entry.Name())
		if _, ok := outputs[name]; !ok {
			return fmt.Errorf("stale generated golden %s; run make generate", name)
		}
	}
	return nil
}

func removeStaleGoldens(directory string, outputs map[string][]byte) error {
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".golden.json") {
			continue
		}
		name := filepath.Join(directory, entry.Name())
		if _, ok := outputs[name]; ok {
			continue
		}
		if err := os.Remove(name); err != nil {
			return fmt.Errorf("remove stale generated golden %s: %w", name, err)
		}
	}
	return nil
}
