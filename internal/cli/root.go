// Package cli is the standalone BOAR Cobra adapter. Business algorithms stay
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
	"syscall"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/internal/version"
	"github.com/spf13/cobra"
)

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
	serve       ServeRunner
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

// NewRootCommandWithServe composes the standalone CLI with the process-level
// Server bootstrap without making either adapter import the other.
func NewRootCommandWithServe(stdout, stderr io.Writer, runner ServeRunner) *cobra.Command {
	return newRootCommandWithAllBoundariesAndServe(
		os.Stdin, stdout, stderr, defaultRuntimeFactory, time.Now, newCLIRequestID,
		func() bool { return isTerminalFile(os.Stdin) }, runner,
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
	return newRootCommandWithAllBoundariesAndServe(stdin, stdout, stderr, factory, clock, newID, interactive, nil)
}

func newRootCommandWithAllBoundariesAndServe(
	stdin io.Reader,
	stdout, stderr io.Writer,
	factory runtimeFactory,
	clock func() time.Time,
	newID func() string,
	interactive func() bool,
	serve ServeRunner,
) *cobra.Command {
	application := &adapter{
		stdin: stdin, input: bufio.NewReader(stdin), stdout: stdout, stderr: stderr,
		factory: factory, clock: clock, newID: newID, interactive: interactive, serve: serve,
	}
	command := &cobra.Command{
		Use:              "boar",
		Short:            "Patroni 4.x control SDK, CLI, and stateless control plane",
		Version:          version.String(),
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

	defaultConfig, _ := defaultConfigPath()
	// Click group options are intentionally local to the root. With
	// TraverseChildren this preserves `boar -c FILE query ... -c SQL` and the
	// equivalent -d pair without illegal Cobra inherited-shorthand collisions.
	command.Flags().StringVarP(&application.root.configFile, "config-file", "c", defaultConfig, "Configuration file")
	command.Flags().StringVarP(&application.root.dcsURL, "dcs-url", "d", "", "The DCS connect url")
	command.Flags().StringVar(&application.root.dcsAlias, "dcs", "", "The DCS connect url")
	command.Flags().BoolVarP(&application.root.insecure, "insecure", "k", false, "Allow connections to SSL sites without certs")

	command.PersistentFlags().StringVar(&application.root.context, "context", "", "Select a named BOAR context")
	command.PersistentFlags().StringVarP(&application.root.output, "output", "o", "", "BOAR output envelope: human, json, or yaml")
	command.PersistentFlags().BoolVar(&application.root.allowUnsupportedRead, "allow-unsupported-read", false,
		"Allow best-effort reads from an unsupported Patroni major version")

	application.addReadCommands(command)
	application.addWriteCommands(command)
	application.addServeCommand(command)
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
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "boar-cli-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return "boar-cli-" + hex.EncodeToString(value[:])
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

func errorWasRendered(err error) bool {
	var typed *exitError
	return errors.As(err, &typed) && typed.rendered
}

// Execute owns process signals and exact additive exit mapping.
func Execute() int {
	return ExecuteWithServe(nil)
}

// ExecuteWithServe owns process signals and composes the optional Server
// runner at the binary boundary.
func ExecuteWithServe(runner ServeRunner) int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	command := NewRootCommandWithServe(os.Stdout, os.Stderr, runner)
	if err := command.ExecuteContext(ctx); err != nil {
		if !errorWasRendered(err) {
			fmt.Fprintln(os.Stderr, err)
		}
		return exitCode(err)
	}
	return 0
}
