// Package cli is the standalone patronictl Cobra adapter. Business algorithms stay
// in public control packages; this package owns parsing, prompting, rendering,
// signal cancellation, and process exit mapping.
package cli

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	stdlibruntime "runtime"
	"strings"
	"syscall"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/internal/version"
	patroniruntime "github.com/pgsty/go-patroni/runtime"
	"github.com/spf13/cobra"
)

// Application describes an embedding command's presentation and optional
// build identity. The Patroni machine contract remains owned by go-patroni;
// Info identifies the host application without replacing SDK metadata.
type Application struct {
	Name            string
	Short           string
	Version         string
	RequestIDPrefix string
	Info            *ApplicationInfo
}

// ApplicationInfo is emitted as optional host metadata by the version command.
type ApplicationInfo struct {
	Name             string
	Version          string
	Commit           string
	BuildTime        string
	GoVersion        string
	SupportedPatroni string
}

// CommandOptions controls construction of an embeddable Patroni CLI tree.
// Extensions add product-specific commands without importing this internal
// implementation directly; the public cli package is the supported facade.
type CommandOptions struct {
	Stdin         io.Reader
	Stdout        io.Writer
	Stderr        io.Writer
	Application   Application
	Environment   patroniruntime.EnvironmentOptions
	Extensions    []Extension
	IsInteractive func() bool
}

// Extension adds one product-owned top-level command to the Patroni CLI tree.
type Extension func(ExtensionContext) *cobra.Command

// ExtensionContext exposes only stable composition helpers to an extension.
type ExtensionContext struct {
	application *adapter
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
	if extension.application == nil {
		return RootInvocation{}, usageError("CLI extension is not initialized")
	}
	return extension.application.rootInvocation(command)
}

// UsageError returns an error with the CLI's stable usage exit category.
func (ExtensionContext) UsageError(message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "invalid CLI extension invocation"
	}
	return usageError(message)
}

// ExitError returns an error with a stable control category and exit code.
func (ExtensionContext) ExitError(category control.Category, message string, cause error) error {
	if category == "" {
		category = control.CategoryInternal
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "CLI extension failed"
	}
	return &exitError{category: category, code: control.ExitCode(category), message: message, cause: cause}
}

type rootOptions struct {
	configFile           string
	dcsURL               string
	dcsAlias             string
	insecure             bool
	context              string
	output               string
	allowUnsupportedRead bool
}

type adapter struct {
	stdin       io.Reader
	input       *bufio.Reader
	stdout      io.Writer
	stderr      io.Writer
	root        rootOptions
	factory     runtimeFactory
	clock       func() time.Time
	newID       func() string
	interactive func() bool
	application Application
}

func defaultApplication() Application {
	return Application{
		Name: "patronictl", Short: "Native Go command-line control for Patroni clusters",
		Version: version.String(), RequestIDPrefix: "patronictl-go-cli",
	}
}

func normalizeApplication(application Application) Application {
	defaults := defaultApplication()
	application.Name = strings.TrimSpace(application.Name)
	if application.Name == "" {
		application.Name = defaults.Name
	}
	application.Short = strings.TrimSpace(application.Short)
	if application.Short == "" {
		application.Short = defaults.Short
	}
	application.Version = strings.TrimSpace(application.Version)
	if application.Version == "" {
		application.Version = defaults.Version
	}
	application.RequestIDPrefix = strings.TrimSpace(application.RequestIDPrefix)
	if application.RequestIDPrefix == "" {
		if application.Name == defaults.Name {
			application.RequestIDPrefix = defaults.RequestIDPrefix
		} else {
			application.RequestIDPrefix = application.Name + "-cli"
		}
	}
	if application.Info != nil {
		copyInfo := *application.Info
		copyInfo.Name = strings.TrimSpace(copyInfo.Name)
		if copyInfo.Name == "" {
			copyInfo.Name = application.Name
		}
		copyInfo.Version = strings.TrimSpace(copyInfo.Version)
		if copyInfo.Version == "" {
			copyInfo.Version = application.Version
		}
		copyInfo.Commit = strings.TrimSpace(copyInfo.Commit)
		if copyInfo.Commit == "" {
			copyInfo.Commit = "unknown"
		}
		copyInfo.BuildTime = strings.TrimSpace(copyInfo.BuildTime)
		if copyInfo.BuildTime == "" {
			copyInfo.BuildTime = "unknown"
		}
		copyInfo.GoVersion = strings.TrimSpace(copyInfo.GoVersion)
		if copyInfo.GoVersion == "" {
			copyInfo.GoVersion = stdlibruntime.Version()
		}
		copyInfo.SupportedPatroni = strings.TrimSpace(copyInfo.SupportedPatroni)
		if copyInfo.SupportedPatroni == "" {
			copyInfo.SupportedPatroni = version.Current().SupportedPatroni
		}
		application.Info = &copyInfo
	}
	return application
}

// NewRootCommand constructs the standalone adapter without process-global
// output. It uses os.Stdin only for command SQL, password, and confirmation
// input; tests use newRootCommand to replace every boundary.
func NewRootCommand(stdout, stderr io.Writer) *cobra.Command {
	return newRootCommandWithAllBoundaries(
		os.Stdin, stdout, stderr, defaultRuntimeFactory, time.Now, newCLIRequestID,
		func() bool { return isTerminalFile(os.Stdin) },
	)
}

// NewRootCommandWithOptions constructs the command tree for an embedding
// application. The zero value preserves standalone patronictl behavior.
func NewRootCommandWithOptions(options CommandOptions) *cobra.Command {
	stdin := options.Stdin
	if stdin == nil {
		stdin = os.Stdin
	}
	stdout := options.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := options.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	interactive := options.IsInteractive
	if interactive == nil {
		interactive = func() bool {
			file, ok := stdin.(*os.File)
			return ok && isTerminalFile(file)
		}
	}
	application := normalizeApplication(options.Application)
	return newRootCommandWithComposition(
		stdin, stdout, stderr, runtimeFactoryWithOptions(options.Environment), time.Now,
		func() string { return newCLIRequestIDWithPrefix(application.RequestIDPrefix) },
		interactive, application, options.Environment, options.Extensions,
	)
}

func newRootCommand(stdin io.Reader, stdout, stderr io.Writer, factory runtimeFactory) *cobra.Command {
	return newRootCommandWithBoundaries(stdin, stdout, stderr, factory, time.Now, newCLIRequestID)
}

func newRootCommandWithBoundaries(
	stdin io.Reader,
	stdout, stderr io.Writer,
	factory runtimeFactory,
	clock func() time.Time,
	newID func() string,
) *cobra.Command {
	return newRootCommandWithAllBoundaries(stdin, stdout, stderr, factory, clock, newID, func() bool { return true })
}

func newRootCommandWithAllBoundaries(
	stdin io.Reader,
	stdout, stderr io.Writer,
	factory runtimeFactory,
	clock func() time.Time,
	newID func() string,
	interactive func() bool,
) *cobra.Command {
	return newRootCommandWithComposition(
		stdin, stdout, stderr, factory, clock, newID, interactive,
		defaultApplication(), patroniruntime.EnvironmentOptions{}, nil,
	)
}

func newRootCommandWithComposition(
	stdin io.Reader,
	stdout, stderr io.Writer,
	factory runtimeFactory,
	clock func() time.Time,
	newID func() string,
	interactive func() bool,
	profile Application,
	environment patroniruntime.EnvironmentOptions,
	extensions []Extension,
) *cobra.Command {
	profile = normalizeApplication(profile)
	application := &adapter{
		stdin: stdin, input: bufio.NewReader(stdin), stdout: stdout, stderr: stderr,
		factory: factory, clock: clock, newID: newID, interactive: interactive, application: profile,
	}
	command := &cobra.Command{
		Use:              application.application.Name,
		Short:            application.application.Short,
		Version:          application.application.Version,
		SilenceErrors:    true,
		SilenceUsage:     true,
		TraverseChildren: true,
		Args:             cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
		PersistentPreRunE: application.validateOutputContract,
	}
	command.SetIn(stdin)
	command.SetOut(stdout)
	command.SetErr(stderr)

	defaultConfig := strings.TrimSpace(environment.Load.Path)
	if defaultConfig == "" {
		defaultConfig, _ = defaultConfigPath()
	}
	// Click group options are intentionally local to the root. With
	// TraverseChildren this preserves `patronictl -c FILE query ... -c SQL` and the
	// equivalent -d pair without illegal Cobra inherited-shorthand collisions.
	command.Flags().StringVarP(&application.root.configFile, "config-file", "c", defaultConfig, "Configuration file")
	command.Flags().StringVarP(&application.root.dcsURL, "dcs-url", "d", "", "The DCS connect url")
	command.Flags().StringVar(&application.root.dcsAlias, "dcs", "", "The DCS connect url")
	command.Flags().BoolVarP(&application.root.insecure, "insecure", "k", false, "Allow connections to SSL sites without certs")

	command.PersistentFlags().StringVar(&application.root.context, "context", "", "Select a named Patroni context")
	command.PersistentFlags().StringVarP(&application.root.output, "output", "o", "", application.application.Name+" output envelope: human, json, or yaml")
	command.PersistentFlags().BoolVar(&application.root.allowUnsupportedRead, "allow-unsupported-read", false,
		"Allow best-effort reads from an unsupported Patroni major version")

	application.addReadCommands(command)
	application.addWriteCommands(command)
	extensionContext := ExtensionContext{application: application}
	for _, extend := range extensions {
		if extend == nil {
			continue
		}
		if child := extend(extensionContext); child != nil {
			command.AddCommand(child)
		}
	}
	command.InitDefaultCompletionCmd()
	application.wrapCommandErrors(command)
	return command
}

func defaultConfigPath() (string, error) {
	return resolveDefaultConfigPath()
}

func (application *adapter) validateOutputContract(command *cobra.Command, _ []string) error {
	var err error
	switch application.root.output {
	case "", "human", "json", "yaml":
	default:
		err = usageError(fmt.Sprintf("invalid --output %q: choose human, json, or yaml", application.root.output))
	}
	if err == nil && application.root.output != "" {
		if format := command.Flags().Lookup("format"); format != nil && format.Changed {
			err = usageError("--format and --output are mutually exclusive")
		}
	}
	return application.renderCommandError(command, nil, err)
}

func (application *adapter) wrapCommandErrors(command *cobra.Command) {
	if command.Args != nil {
		validate := command.Args
		command.Args = func(current *cobra.Command, args []string) error {
			err := validate(current, args)
			if err != nil {
				var typed *exitError
				if !errors.As(err, &typed) {
					err = usageError(err.Error())
				}
			}
			return application.renderCommandError(current, args, err)
		}
	}
	if command.RunE != nil {
		run := command.RunE
		command.RunE = func(current *cobra.Command, args []string) error {
			return application.renderCommandError(current, args, run(current, args))
		}
	}
	command.SetFlagErrorFunc(func(current *cobra.Command, err error) error {
		return application.renderCommandError(current, nil, usageError(err.Error()))
	})
	for _, child := range command.Commands() {
		application.wrapCommandErrors(child)
	}
}

func newCLIRequestID() string {
	return newCLIRequestIDWithPrefix(defaultApplication().RequestIDPrefix)
}

func newCLIRequestIDWithPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultApplication().RequestIDPrefix
	}
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return prefix + "-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return prefix + "-" + hex.EncodeToString(value[:])
}

func (application *adapter) now() time.Time {
	if application != nil && application.clock != nil {
		return application.clock().UTC()
	}
	return time.Now().UTC()
}

func (application *adapter) wallNow() time.Time {
	if application != nil && application.clock != nil {
		return application.clock()
	}
	return time.Now()
}

func (application *adapter) isInteractive() bool {
	return application != nil && application.interactive != nil && application.interactive()
}

func (application *adapter) requestID() string {
	if application != nil && application.newID != nil {
		if value := application.newID(); value != "" {
			return value
		}
	}
	return newCLIRequestID()
}

func (application *adapter) rootInvocation(command *cobra.Command) (RootInvocation, error) {
	if command == nil {
		return RootInvocation{}, usageError("CLI extension command is nil")
	}
	root := command.Root()
	dcsURL := application.root.dcsURL
	dcsURLSet := root.Flags().Changed("dcs-url")
	if root.Flags().Changed("dcs") {
		if dcsURLSet && application.root.dcsAlias != dcsURL {
			return RootInvocation{}, usageError("--dcs-url and --dcs specify different values")
		}
		dcsURL, dcsURLSet = application.root.dcsAlias, true
	}
	return RootInvocation{
		ConfigFile: application.root.configFile, ConfigFileSet: root.Flags().Changed("config-file"),
		DCSURL: dcsURL, DCSURLSet: dcsURLSet,
		Insecure: application.root.insecure, InsecureSet: root.Flags().Changed("insecure"),
		Context: application.root.context, Output: application.root.output,
	}, nil
}

type exitError struct {
	category control.Category
	code     int
	message  string
	cause    error
	rendered bool
}

func (err *exitError) Error() string {
	if err == nil {
		return ""
	}
	return err.message
}

func (err *exitError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func usageError(message string) error {
	return &exitError{category: control.CategoryUsage, code: control.ExitCode(control.CategoryUsage), message: message}
}

func exitForControl(err *control.Error, rendered bool) error {
	if err == nil {
		return nil
	}
	return &exitError{category: err.Category, code: control.ExitCode(err.Category), message: err.Message, cause: err, rendered: rendered}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var typed *exitError
	if errors.As(err, &typed) && typed.code != 0 {
		return typed.code
	}
	return 1
}

// ExitCode exposes stable process mapping to the public CLI facade.
func ExitCode(err error) int {
	return exitCode(err)
}

func errorWasRendered(err error) bool {
	var typed *exitError
	return errors.As(err, &typed) && typed.rendered
}

// Execute owns process signals and exact patronictl-compatible exit mapping.
func Execute() int {
	return ExecuteWithOptions(CommandOptions{})
}

// ExecuteWithOptions owns process signals for an embedding application.
func ExecuteWithOptions(options CommandOptions) int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return ExecuteContext(ctx, options)
}

// ExecuteContext runs an embedding command tree with caller-owned cancellation.
func ExecuteContext(ctx context.Context, options CommandOptions) int {
	if ctx == nil {
		ctx = context.Background()
	}
	command := NewRootCommandWithOptions(options)
	if err := command.ExecuteContext(ctx); err != nil {
		if !errorWasRendered(err) {
			// The command has already failed, and there is no remaining
			// channel on which a stderr write failure could be reported.
			_, _ = fmt.Fprintln(command.ErrOrStderr(), err)
		}
		return exitCode(err)
	}
	return 0
}
