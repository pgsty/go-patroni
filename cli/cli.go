// Package cli composes the go-patroni patronictl-compatible command suite into
// standalone binaries and larger applications. Core SDK packages remain free
// of Cobra; applications that only need programmatic control should continue to
// use control and runtime directly.
package cli

import (
	"context"
	"io"

	"github.com/pgsty/go-patroni/control"
	internalcli "github.com/pgsty/go-patroni/internal/cli"
	patroniruntime "github.com/pgsty/go-patroni/runtime"
	"github.com/spf13/cobra"
)

// MachineAPIVersion is the canonical machine-output contract shared by the
// standalone patronictl adapter and every embedded command tree.
const MachineAPIVersion = "patroni.pgsty.com/v1alpha1"

// Application customizes the host command's presentation and request IDs.
// Info is optional host metadata; it never replaces canonical SDK metadata in
// machine output.
type Application struct {
	Name            string
	Short           string
	Version         string
	RequestIDPrefix string
	Info            *ApplicationInfo
}

// ApplicationInfo identifies an embedding binary in version machine output.
type ApplicationInfo struct {
	Name             string
	Version          string
	Commit           string
	BuildTime        string
	GoVersion        string
	SupportedPatroni string
}

// Options controls construction of an embeddable Patroni CLI tree. Zero-valued
// I/O uses the process streams, and a zero-valued Application preserves the
// standalone patronictl identity.
type Options struct {
	Stdin         io.Reader
	Stdout        io.Writer
	Stderr        io.Writer
	Application   Application
	Environment   patroniruntime.EnvironmentOptions
	Extensions    []Extension
	IsInteractive func() bool
}

// Extension adds one product-owned top-level command. It is called once while
// the tree is constructed; returning nil skips registration.
type Extension func(ExtensionContext) *cobra.Command

// ExtensionContext provides stable root-state and error helpers to extensions.
// Its zero value is invalid and is only supplied by NewRootCommand.
type ExtensionContext struct {
	inner internalcli.ExtensionContext
}

// RootInvocation is the normalized root flag state visible to extensions.
// Set fields distinguish explicit CLI overrides from displayed defaults.
type RootInvocation struct {
	ConfigFile    string
	ConfigFileSet bool
	DCSURL        string
	DCSURLSet     bool
	Insecure      bool
	InsecureSet   bool
	Context       string
	Output        string
}

// Invocation resolves root aliases and returns the effective extension view.
func (extension ExtensionContext) Invocation(command *cobra.Command) (RootInvocation, error) {
	invocation, err := extension.inner.Invocation(command)
	if err != nil {
		return RootInvocation{}, err
	}
	return RootInvocation{
		ConfigFile: invocation.ConfigFile, ConfigFileSet: invocation.ConfigFileSet,
		DCSURL: invocation.DCSURL, DCSURLSet: invocation.DCSURLSet,
		Insecure: invocation.Insecure, InsecureSet: invocation.InsecureSet,
		Context: invocation.Context, Output: invocation.Output,
	}, nil
}

// UsageError returns an error with the CLI's stable usage exit category.
func (extension ExtensionContext) UsageError(message string) error {
	return extension.inner.UsageError(message)
}

// ExitError returns an error with a stable control category and exit code.
func (extension ExtensionContext) ExitError(category control.Category, message string, cause error) error {
	return extension.inner.ExitError(category, message, cause)
}

// NewRootCommand constructs the full patronictl-compatible tree plus any
// product-owned extensions without using process-global I/O.
func NewRootCommand(options Options) *cobra.Command {
	return internalcli.NewRootCommandWithOptions(internalOptions(options))
}

// Execute owns process interrupt handling and runs the composed command tree.
func Execute(options Options) int {
	return internalcli.ExecuteWithOptions(internalOptions(options))
}

// ExecuteContext runs the composed tree with caller-owned cancellation.
func ExecuteContext(ctx context.Context, options Options) int {
	return internalcli.ExecuteContext(ctx, internalOptions(options))
}

// ExitCode returns the process status that Execute would use for err.
func ExitCode(err error) int {
	return internalcli.ExitCode(err)
}

func internalOptions(options Options) internalcli.CommandOptions {
	converted := internalcli.CommandOptions{
		Stdin: options.Stdin, Stdout: options.Stdout, Stderr: options.Stderr,
		Application: internalApplication(options.Application), Environment: options.Environment,
		IsInteractive: options.IsInteractive,
	}
	converted.Extensions = make([]internalcli.Extension, 0, len(options.Extensions))
	for _, extension := range options.Extensions {
		if extension == nil {
			converted.Extensions = append(converted.Extensions, nil)
			continue
		}
		extend := extension
		converted.Extensions = append(converted.Extensions, func(context internalcli.ExtensionContext) *cobra.Command {
			return extend(ExtensionContext{inner: context})
		})
	}
	return converted
}

func internalApplication(application Application) internalcli.Application {
	converted := internalcli.Application{
		Name: application.Name, Short: application.Short, Version: application.Version,
		RequestIDPrefix: application.RequestIDPrefix,
	}
	if application.Info != nil {
		converted.Info = &internalcli.ApplicationInfo{
			Name: application.Info.Name, Version: application.Info.Version, Commit: application.Info.Commit,
			BuildTime: application.Info.BuildTime, GoVersion: application.Info.GoVersion,
			SupportedPatroni: application.Info.SupportedPatroni,
		}
	}
	return converted
}
