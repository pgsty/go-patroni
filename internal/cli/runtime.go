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
	root := command.Root()
	dcsURL := application.root.dcsURL
	dcsURLSet := root.Flags().Changed("dcs-url")
	if root.Flags().Changed("dcs") {
		if dcsURLSet && application.root.dcsAlias != dcsURL {
			return nil, usageError("--dcs-url and --dcs specify different values")
		}
		dcsURL, dcsURLSet = application.root.dcsAlias, true
	}
	invocation := runtimeInvocation{
		configPathSet: root.Flags().Changed("config-file"), configPath: application.root.configFile,
		dcsURLSet: dcsURLSet, dcsURL: dcsURL,
		insecureSet: root.Flags().Changed("insecure"), insecure: application.root.insecure,
		context: application.root.context, request: request,
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
	load := config.LoadRequest{}
	if invocation.configPathSet {
		load.Path = invocation.configPath
	}
	overrides := config.Overrides{}
	if invocation.dcsURLSet {
		overrides.DCSURL = &invocation.dcsURL
	}
	if invocation.insecureSet {
		overrides.Insecure = &invocation.insecure
	}
	environment, err := app.NewEnvironment(ctx, app.EnvironmentOptions{Load: load, Overrides: overrides})
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
