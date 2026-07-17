package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	internalsecret "github.com/pgsty/go-patroni/internal/secret"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
)

func (application *adapter) runEditConfig(command *cobra.Command, args []string, options *editConfigOptions) (returnedError error) {
	if options.applyFile != "" && options.replaceFile != "" && options.applyFile == "-" && options.replaceFile == "-" {
		return usageError("--apply - and --replace - cannot both consume standard input")
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group), useConfiguredGroup: true,
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)

	currentResult := runtime.service.ShowConfig(command.Context(), control.ShowConfigRequest{Target: runtime.target})
	if currentResult.Outcome != control.Succeeded {
		return finishResult(application, application.root.output, runtime, currentResult, "DynamicConfig", func(io.Writer, control.ConfigData) error { return nil })
	}
	request := control.EditConfigRequest{Target: runtime.target, Citus: runtime.resolved.Citus}
	if options.replaceFile != "" {
		request.Replacement, err = application.readConfigMap(command.Context(), options.replaceFile)
		if err != nil {
			return err
		}
	}
	if options.applyFile != "" {
		request.Apply, err = application.readConfigMap(command.Context(), options.applyFile)
		if err != nil {
			return err
		}
	}
	request.Settings, err = parseConfigSettings(options.settings, options.pgSettings)
	if err != nil {
		return err
	}
	if request.Replacement == nil && request.Apply == nil && len(request.Settings) == 0 {
		request.Replacement, err = application.editConfigInEditor(command.Context(), runtime.target.Scope, currentResult.Data.Config)
		if err != nil {
			return err
		}
	}
	preview, err := control.PreviewEditConfig(currentResult.Data.Config, request)
	if err != nil {
		return usageError(err.Error())
	}
	if !options.quiet && (application.root.output == "" || application.root.output == "human") {
		if err := renderConfigDiff(application.stdout, preview); err != nil {
			return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "rendering configuration diff failed", cause: err}
		}
	}
	stdinMutation := options.applyFile == "-" || options.replaceFile == "-"
	if stdinMutation && !options.force {
		if application.root.output != "" && application.root.output != "human" {
			return usageError("--force is required when machine output consumes configuration from standard input")
		}
		_, err := fmt.Fprintln(application.stdout, "Use --force option to apply changes")
		return err
	}
	prepared := runtime.service.PrepareEditConfig(command.Context(), request)
	plan, err := application.preparedPlan(runtime, prepared)
	if err != nil {
		return err
	}
	if !preview.Noop && !options.force {
		if err := application.confirmPlan(plan, "Apply these changes?"); err != nil {
			return err
		}
	}
	result := runtime.service.ExecuteEditConfig(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "ConfigEditResult", func(writer io.Writer, data control.ConfigEditData) error {
		if data.Noop {
			_, err := fmt.Fprintln(writer, "Not changed")
			return err
		}
		if result.Outcome == control.Succeeded {
			_, err := fmt.Fprintln(writer, "Configuration changed")
			return err
		}
		return nil
	})
}

func (application *adapter) readConfigMap(ctx context.Context, filename string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "configuration input canceled", cause: err}
	}
	var data []byte
	var err error
	if filename == "-" {
		data, err = io.ReadAll(application.input)
	} else {
		data, err = os.ReadFile(filename)
	}
	if err != nil {
		return nil, &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "configuration file is not readable", cause: err}
	}
	configuration := make(map[string]any)
	if len(strings.TrimSpace(string(data))) != 0 {
		if err := yaml.Unmarshal(data, &configuration); err != nil {
			return nil, &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "configuration file is not valid YAML", cause: err}
		}
	}
	return configuration, nil
}

func parseConfigSettings(settings, postgresSettings []string) ([]control.ConfigSetting, error) {
	all := make([]string, 0, len(settings)+len(postgresSettings))
	all = append(all, settings...)
	for _, setting := range postgresSettings {
		all = append(all, "postgresql.parameters."+strings.TrimLeft(setting, " \t"))
	}
	result := make([]control.ConfigSetting, 0, len(all))
	for _, setting := range all {
		path, raw, ok := strings.Cut(setting, "=")
		path = strings.TrimSpace(path)
		if !ok || path == "" {
			return nil, usageError(fmt.Sprintf("invalid parameter setting %q", setting))
		}
		var value any
		if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
			return nil, usageError(fmt.Sprintf("invalid YAML value for setting %q", path))
		}
		result = append(result, control.ConfigSetting{Path: path, Value: value})
	}
	return result, nil
}

func (application *adapter) editConfigInEditor(ctx context.Context, scope string, current map[string]any) (map[string]any, error) {
	if err := application.promptUnavailableError(); err != nil {
		return nil, usageError("configuration editing requires a terminal and human output; use --apply or --replace")
	}
	editor := strings.TrimSpace(os.Getenv("EDITOR"))
	if editor == "" {
		for _, candidate := range []string{"editor", "vi"} {
			if path, err := exec.LookPath(candidate); err == nil {
				editor = path
				break
			}
		}
	}
	if editor == "" {
		return nil, &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "EDITOR is not set and editor or vi is not available"}
	}
	encoded, err := yaml.Marshal(current)
	if err != nil {
		return nil, &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "dynamic configuration cannot be encoded for editing", cause: err}
	}
	prefix := strings.NewReplacer("/", "_", "\\", "_").Replace(scope) + "-config-"
	file, err := os.CreateTemp("", prefix+"*.yaml")
	if err != nil {
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "temporary editor file could not be created", cause: err}
	}
	name := file.Name()
	defer func() { _ = os.Remove(name) }()
	if chmodError := file.Chmod(0o600); chmodError != nil {
		_ = file.Close()
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "temporary editor file permissions could not be secured", cause: chmodError}
	}
	if _, err = file.Write(encoded); err == nil {
		err = file.Close()
	} else {
		_ = file.Close()
	}
	if err != nil {
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "temporary editor file could not be written", cause: err}
	}
	if err := exec.CommandContext(ctx, editor, name).Run(); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "configuration editor canceled", cause: ctx.Err()}
		}
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "configuration editor failed", cause: err}
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "edited configuration could not be read", cause: err}
	}
	configuration := make(map[string]any)
	if err := yaml.Unmarshal(data, &configuration); err != nil {
		return nil, &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "edited configuration is not valid YAML", cause: err}
	}
	return configuration, nil
}

func renderConfigDiff(writer io.Writer, preview control.ConfigEditPreview) error {
	if preview.Noop {
		_, err := fmt.Fprintln(writer, "Not changed")
		return err
	}
	before, err := yaml.Marshal(preview.Before)
	if err != nil {
		return err
	}
	after, err := yaml.Marshal(preview.After)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "--- before"); err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(internalsecret.Redact(string(before)), "\n"), "\n") {
		if _, err := fmt.Fprintln(writer, "-"+line); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, "+++ after"); err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(internalsecret.Redact(string(after)), "\n"), "\n") {
		if _, err := fmt.Fprintln(writer, "+"+line); err != nil {
			return err
		}
	}
	return nil
}
