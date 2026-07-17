package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	app "github.com/pgsty/go-patroni/runtime"
	"github.com/spf13/cobra"
)

type runtimeRequest struct {
	operation          config.Operation
	explicitScope      string
	explicitGroup      *int
	useConfiguredGroup bool
	allowMissingScope  bool
}

type runtimeInvocation struct {
	configPathSet bool
	configPath    string
	dcsURLSet     bool
	dcsURL        string
	insecureSet   bool
	insecure      bool
	context       string
	request       runtimeRequest
}

type commandRuntime struct {
	service  *control.Service
	resolved config.Resolved
	target   model.Target
	warnings []string
	close    func() error
}

func (runtime *commandRuntime) Close() error {
	if runtime == nil || runtime.close == nil {
		return nil
	}
	return runtime.close()
}

func closeCommandRuntime(runtime *commandRuntime, returnedError *error) {
	if runtime == nil || returnedError == nil {
		return
	}
	if err := runtime.Close(); err != nil {
		closeError := &exitError{
			category: control.CategoryInternal,
			code:     control.ExitCode(control.CategoryInternal),
			message:  "closing command runtime failed",
			cause:    err,
		}
		*returnedError = errors.Join(*returnedError, closeError)
	}
}

type runtimeFactory func(context.Context, runtimeInvocation) (*commandRuntime, error)

func resolveDefaultConfigPath() (string, error) { return config.DefaultConfigPath() }

func (application *adapter) openRuntime(command *cobra.Command, request runtimeRequest) (*commandRuntime, error) {
	root, err := application.rootInvocation(command)
	if err != nil {
		return nil, err
	}
	invocation := runtimeInvocation{
		configPathSet: root.ConfigFileSet, configPath: root.ConfigFile,
		dcsURLSet: root.DCSURLSet, dcsURL: root.DCSURL,
		insecureSet: root.InsecureSet, insecure: root.Insecure,
		context: root.Context, request: request,
	}
	runtime, err := application.factory(command.Context(), invocation)
	if err != nil {
		var typed *exitError
		if errors.As(err, &typed) {
			return nil, err
		}
		category := runtimeErrorCategory(err)
		return nil, &exitError{category: category, code: control.ExitCode(category), message: safeRuntimeError(err), cause: err}
	}
	if application.root.output == "" || application.root.output == "human" {
		for _, warning := range runtime.warnings {
			if _, writeErr := fmt.Fprintf(application.stderr, "WARNING: %s\n", warning); writeErr != nil {
				_ = runtime.Close()
				return nil, &exitError{
					category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal),
					message: "rendering configuration warning failed", cause: writeErr,
				}
			}
		}
	}
	return runtime, nil
}

func runtimeErrorCategory(err error) control.Category {
	var configuration *config.Error
	if errors.As(err, &configuration) {
		if configuration.Kind == config.ErrorUnsupported {
			return control.CategoryUnsupported
		}
		return control.CategoryConfig
	}
	var validation *config.ValidationError
	if errors.As(err, &validation) {
		return control.CategoryConfig
	}
	var tlsError *patroni.TLSConfigError
	if errors.As(err, &tlsError) {
		return control.CategoryTLS
	}
	var dcsError *dcs.Error
	if errors.As(err, &dcsError) {
		switch dcsError.Kind {
		case dcs.ErrorConfiguration, dcs.ErrorDecode, dcs.ErrorLimit:
			return control.CategoryConfig
		case dcs.ErrorConflict, dcs.ErrorCompacted:
			return control.CategoryConflict
		case dcs.ErrorCanceled:
			return control.CategoryFailed
		case dcs.ErrorTransport, dcs.ErrorDeadline:
			return control.CategoryUnreachable
		}
	}
	var parsedURL *url.Error
	if errors.As(err, &parsedURL) {
		return control.CategoryUnreachable
	}
	return control.CategoryConfig
}

func defaultRuntimeFactory(ctx context.Context, invocation runtimeInvocation) (*commandRuntime, error) {
	return openDefaultRuntime(ctx, app.EnvironmentOptions{}, invocation)
}

func runtimeFactoryWithOptions(options app.EnvironmentOptions) runtimeFactory {
	options = cloneEnvironmentOptions(options)
	return func(ctx context.Context, invocation runtimeInvocation) (*commandRuntime, error) {
		return openDefaultRuntime(ctx, options, invocation)
	}
}

func openDefaultRuntime(ctx context.Context, options app.EnvironmentOptions, invocation runtimeInvocation) (*commandRuntime, error) {
	options = cloneEnvironmentOptions(options)
	load := options.Load
	if invocation.configPathSet {
		load.Path = invocation.configPath
		// An explicit CLI path has higher precedence than an injected parsed
		// document, just as it does over an embedding application's load default.
		options.Document = nil
	}
	overrides := options.Overrides
	if invocation.dcsURLSet {
		overrides.DCSURL = &invocation.dcsURL
	}
	if invocation.insecureSet {
		overrides.Insecure = &invocation.insecure
	}
	options.Load = load
	options.Overrides = overrides
	environment, err := app.NewEnvironment(ctx, options)
	if err != nil {
		return nil, err
	}
	if invocation.request.operation == config.OperationInspect {
		runtime, openErr := environment.OpenConfiguration(ctx, invocation.context)
		if openErr != nil {
			return nil, openErr
		}
		return &commandRuntime{
			service: runtime.Service, resolved: runtime.Resolved, target: runtime.Target, warnings: runtime.Warnings,
			close: runtime.Close,
		}, nil
	}
	runtime, err := environment.Open(ctx, app.RuntimeOptions{
		Context: invocation.context, Operation: invocation.request.operation,
		ExplicitScope: invocation.request.explicitScope, ExplicitGroup: invocation.request.explicitGroup,
		UseConfiguredGroup: invocation.request.useConfiguredGroup, AllowMissingScope: invocation.request.allowMissingScope,
	})
	if err != nil {
		return nil, err
	}
	return &commandRuntime{
		service: runtime.Service, resolved: runtime.Resolved, target: runtime.Target, warnings: runtime.Warnings,
		close: runtime.Close,
	}, nil
}

func cloneEnvironmentOptions(options app.EnvironmentOptions) app.EnvironmentOptions {
	options.Overrides = cloneOverrides(options.Overrides)
	if options.SupportedPatroniRange != nil {
		copyRange := *options.SupportedPatroniRange
		options.SupportedPatroniRange = &copyRange
	}
	return options
}

func cloneOverrides(overrides config.Overrides) config.Overrides {
	overrides.Context = cloneStringPointer(overrides.Context)
	overrides.DCSURL = cloneStringPointer(overrides.DCSURL)
	overrides.Namespace = cloneStringPointer(overrides.Namespace)
	overrides.Scope = cloneStringPointer(overrides.Scope)
	overrides.Group = cloneIntPointer(overrides.Group)
	overrides.Insecure = cloneBoolPointer(overrides.Insecure)
	return overrides
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func safeRuntimeError(err error) string {
	if err == nil {
		return "runtime initialization failed"
	}
	var validation *config.ValidationError
	if errors.As(err, &validation) {
		return validation.Error()
	}
	var configError *config.Error
	if errors.As(err, &configError) {
		return configError.Error()
	}
	var tlsError *patroni.TLSConfigError
	if errors.As(err, &tlsError) {
		return tlsError.Error()
	}
	var dcsError *dcs.Error
	if errors.As(err, &dcsError) {
		if dcsError.Kind == dcs.ErrorTransport || dcsError.Kind == dcs.ErrorDeadline {
			return "configured DCS endpoint is unreachable"
		}
		return "DCS runtime initialization failed"
	}
	var parsedURL *url.Error
	if errors.As(err, &parsedURL) {
		return "configured endpoint is invalid or unreachable"
	}
	return "Patroni runtime initialization failed"
}
