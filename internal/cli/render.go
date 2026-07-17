package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/control"
	internalsecret "github.com/pgsty/go-patroni/internal/secret"
	"github.com/pgsty/go-patroni/model"
	"go.yaml.in/yaml/v3"
)

const machineAPIVersion = "boar.pgsty.com/v1alpha1"

type machineMetadata struct {
	RequestID  string   `json:"requestId" yaml:"requestId"`
	ObservedAt string   `json:"observedAt" yaml:"observedAt"`
	Revision   any      `json:"revision,omitempty" yaml:"revision,omitempty"`
	Warnings   []string `json:"warnings" yaml:"warnings"`
}

type successEnvelope[T any] struct {
	APIVersion string          `json:"apiVersion" yaml:"apiVersion"`
	Kind       string          `json:"kind" yaml:"kind"`
	Metadata   machineMetadata `json:"metadata" yaml:"metadata"`
	Data       T               `json:"data" yaml:"data"`
}

type errorEnvelope struct {
	APIVersion string          `json:"apiVersion" yaml:"apiVersion"`
	Kind       string          `json:"kind" yaml:"kind"`
	Metadata   machineMetadata `json:"metadata" yaml:"metadata"`
	Error      *machineError   `json:"error" yaml:"error"`
}

// machineError exposes a safe stable cause code. The wrapped infrastructure
// error remains in control.Error for errors.Is/As and is never serialized.
type machineError struct {
	Category    control.Category   `json:"category" yaml:"category"`
	Operation   string             `json:"operation" yaml:"operation"`
	Target      model.Target       `json:"target" yaml:"target"`
	Retryable   bool               `json:"retryable" yaml:"retryable"`
	Cause       string             `json:"cause" yaml:"cause"`
	Message     string             `json:"message" yaml:"message"`
	Evidence    []control.Evidence `json:"evidence" yaml:"evidence"`
	NextActions []string           `json:"nextActions" yaml:"nextActions"`
}

func newMachineError(err *control.Error) *machineError {
	if err == nil {
		return nil
	}
	return &machineError{
		Category: err.Category, Operation: err.Operation, Target: err.Target.Normalize(), Retryable: err.Retryable,
		Cause: machineCause(err.Category), Message: err.Message, Evidence: append([]control.Evidence{}, err.Evidence...),
		NextActions: machineNextActions(err.Category),
	}
}

func machineCause(category control.Category) string {
	switch category {
	case control.CategoryUsage:
		return "INVALID_INPUT"
	case control.CategoryConfig:
		return "INVALID_CONFIGURATION"
	case control.CategoryUnsupported:
		return "UNSUPPORTED_PATRONI_VERSION"
	case control.CategoryAuth:
		return "AUTHENTICATION_REJECTED"
	case control.CategoryTLS:
		return "TLS_CONFIGURATION_OR_VERIFICATION_FAILED"
	case control.CategoryNotFound:
		return "TARGET_NOT_FOUND"
	case control.CategoryConflict:
		return "CONCURRENT_STATE_CONFLICT"
	case control.CategoryUnreachable:
		return "UPSTREAM_UNREACHABLE"
	case control.CategoryFailed:
		return "OPERATION_REJECTED"
	case control.CategoryUnknown:
		return "OUTCOME_UNCONFIRMED"
	default:
		return "INTERNAL_INVARIANT"
	}
}

func machineNextActions(category control.Category) []string {
	switch category {
	case control.CategoryUnknown:
		return []string{
			"Do not retry blindly",
			"Re-read authoritative cluster state and operation evidence before deciding whether to retry",
		}
	case control.CategoryConflict:
		return []string{"Re-read authoritative cluster state and prepare a new plan"}
	case control.CategoryUnsupported:
		return []string{"Use Patroni >=4.0.0,<5.0.0 or limit the command to an explicitly allowed read"}
	case control.CategoryUnreachable:
		return []string{"Verify endpoint reachability and retry only when evidence shows the request was not sent"}
	default:
		return []string{}
	}
}

func finishResult[T any](
	application *adapter,
	commandOutput string,
	runtime *commandRuntime,
	result control.Result[T],
	kind string,
	compatible func(io.Writer, T) error,
) error {
	if err := result.Validate(); err != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "control returned an invalid result", cause: err}
	}
	if commandOutput == "" || commandOutput == "human" {
		if result.Outcome == control.Succeeded {
			if err := compatible(application.stdout, result.Data); err != nil {
				return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "rendering command output failed", cause: err}
			}
			return nil
		}
		return exitForControl(result.Error, false)
	}
	metadata := machineMetadata{
		RequestID: result.OperationID, ObservedAt: application.now().Format(time.RFC3339Nano),
		Warnings: append([]string{}, runtime.warnings...),
	}
	var (
		document any
		err      error
	)
	if result.Outcome == control.Succeeded {
		document, err = newMachineSuccessEnvelope(kind, metadata, result.Data)
		if err != nil {
			return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "constructing machine success output failed", cause: err}
		}
	} else {
		document = errorEnvelope{APIVersion: machineAPIVersion, Kind: "Error", Metadata: metadata, Error: newMachineError(result.Error)}
	}
	if err := encodeDocument(application.stdout, commandOutput, document); err != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "encoding machine output failed", cause: err}
	}
	if result.Outcome != control.Succeeded {
		return exitForControl(result.Error, true)
	}
	return nil
}

func (application *adapter) renderCommandError(command interface{ Name() string }, args []string, err error) error {
	if err == nil || errorWasRendered(err) || application.root.output == "" || application.root.output == "human" {
		return err
	}
	if application.root.output != "json" && application.root.output != "yaml" {
		return err
	}
	category := categoryForExitError(err)
	operation := command.Name()
	if operation == "" || operation == "boar" {
		operation = "cli"
	}
	scope := ""
	if len(args) > 0 {
		scope = args[0]
	}
	target := (model.Target{Context: application.root.context, Scope: scope}).Normalize()
	publicError := control.NewError(category, operation, target, false, err.Error(), err)
	if validationError := publicError.Validate(); validationError != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "constructing machine error output failed", cause: validationError}
	}
	document := errorEnvelope{
		APIVersion: machineAPIVersion,
		Kind:       "Error",
		Metadata: machineMetadata{
			RequestID: application.requestID(), ObservedAt: application.now().Format(time.RFC3339Nano), Warnings: []string{},
		},
		Error: newMachineError(publicError),
	}
	if encodeError := encodeDocument(application.stdout, application.root.output, document); encodeError != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "encoding machine error output failed", cause: encodeError}
	}
	return &exitError{category: category, code: control.ExitCode(category), message: err.Error(), cause: err, rendered: true}
}

func categoryForExitError(err error) control.Category {
	var publicError *control.Error
	if errors.As(err, &publicError) {
		return publicError.Category
	}
	var typed *exitError
	if errors.As(err, &typed) {
		if typed.category != "" {
			return typed.category
		}
		switch typed.code {
		case control.ExitCode(control.CategoryFailed):
			return control.CategoryFailed
		case control.ExitCode(control.CategoryUsage):
			return control.CategoryUsage
		case control.ExitCode(control.CategoryUnsupported):
			return control.CategoryUnsupported
		case control.ExitCode(control.CategoryAuth):
			return control.CategoryAuth
		case control.ExitCode(control.CategoryNotFound):
			return control.CategoryNotFound
		case control.ExitCode(control.CategoryConflict):
			return control.CategoryConflict
		case control.ExitCode(control.CategoryUnreachable):
			return control.CategoryUnreachable
		case control.ExitCode(control.CategoryUnknown):
			return control.CategoryUnknown
		case control.ExitCode(control.CategoryInternal):
			return control.CategoryInternal
		}
	}
	return control.CategoryInternal
}

func encodeDocument(writer io.Writer, format string, value any) error {
	var encoded []byte
	var err error
	switch format {
	case "json":
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(value)
		encoded = buffer.Bytes()
	case "yaml", "yml":
		encoded, err = yaml.Marshal(value)
	default:
		return fmt.Errorf("unsupported document format %q", format)
	}
	if err != nil {
		return err
	}
	encoded = []byte(internalsecret.Redact(string(encoded)))
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		encoded = append(encoded, '\n')
	}
	_, err = writer.Write(encoded)
	return err
}

func renderRows(writer io.Writer, headers []string, rows [][]any, format, delimiter, title string) error {
	switch format {
	case "json":
		objects := rowObjects(headers, rows, title != "")
		return encodeDocument(writer, "json", objects)
	case "yaml", "yml":
		objects := rowObjects(headers, rows, title != "")
		return encodeDocument(writer, "yaml", objects)
	case "tsv":
		if delimiter == "" {
			delimiter = "\t"
		}
		if _, err := fmt.Fprintln(writer, strings.Join(headers, delimiter)); err != nil {
			return err
		}
		for _, row := range rows {
			fields := make([]string, len(headers))
			for index := range fields {
				if index < len(row) {
					fields[index] = tabularString(row[index])
				}
			}
			if _, err := fmt.Fprintln(writer, strings.Join(fields, delimiter)); err != nil {
				return err
			}
		}
		return nil
	case "pretty", "topology":
		return renderPretty(writer, headers, rows, title)
	default:
		return fmt.Errorf("unsupported compatible format %q", format)
	}
}

func rowObjects(headers []string, rows [][]any, omitEmptyStrings bool) []map[string]any {
	objects := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		object := make(map[string]any, len(headers))
		for index, header := range headers {
			if index >= len(row) {
				continue
			}
			if text, ok := row[index].(string); omitEmptyStrings && ok && text == "" {
				continue
			}
			object[header] = row[index]
		}
		objects = append(objects, object)
	}
	return objects
}

func renderPretty(writer io.Writer, headers []string, rows [][]any, title string) error {
	widths := make([]int, len(headers))
	for index, header := range headers {
		widths[index] = len(header)
	}
	textRows := make([][]string, len(rows))
	for rowIndex, row := range rows {
		textRows[rowIndex] = make([]string, len(headers))
		for column := range headers {
			if column < len(row) {
				textRows[rowIndex][column] = tabularString(row[column])
			}
			for _, line := range strings.Split(textRows[rowIndex][column], "\n") {
				if len(line) > widths[column] {
					widths[column] = len(line)
				}
			}
		}
	}
	border := func() string {
		parts := make([]string, len(widths))
		for index, width := range widths {
			parts[index] = strings.Repeat("-", width+2)
		}
		return "+" + strings.Join(parts, "+") + "+"
	}()
	if title != "" {
		if _, err := fmt.Fprintln(writer, title); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, border); err != nil {
		return err
	}
	if err := renderPrettyLine(writer, headers, widths); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, border); err != nil {
		return err
	}
	for _, row := range textRows {
		if err := renderPrettyLine(writer, row, widths); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(writer, border)
	return err
}

func renderPrettyLine(writer io.Writer, values []string, widths []int) error {
	fields := make([]string, len(widths))
	for index, width := range widths {
		value := ""
		if index < len(values) {
			value = strings.ReplaceAll(values[index], "\n", " ")
		}
		fields[index] = " " + value + strings.Repeat(" ", width-len(value)+1)
	}
	_, err := fmt.Fprintln(writer, "|"+strings.Join(fields, "|")+"|")
	return err
}

func tabularString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(encoded)
	}
}

func sortedMapKeys[V any](mapping map[string]V) []string {
	keys := make([]string, 0, len(mapping))
	for key := range mapping {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
