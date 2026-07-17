package cli

import (
	"context"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/control"
	"github.com/spf13/cobra"
)

// ServeInvocation is the primitive process-boundary contract between the CLI
// parser and Server bootstrap. It intentionally contains no HTTP adapter type.
type ServeInvocation struct {
	ConfigPath string
	Context    string
	DCSURL     *string
	Insecure   *bool
	Unsafe     bool

	Listen           string
	AdminUsername    string
	PasswordHashFile string
	SessionKeyFile   string
	SessionTTL       time.Duration

	TLSCertFile       string
	TLSKeyFile        string
	TrustedProxyCIDRs []string
	AllowInsecureHTTP bool
	RequestTimeout    time.Duration
	ShutdownTimeout   time.Duration
}

// ServeRunner starts the HTTP adapter and blocks until its context is canceled
// or the Server exits.
type ServeRunner func(context.Context, ServeInvocation) error

type serveOptions struct {
	unsafe            bool
	listen            string
	adminUsername     string
	passwordHashFile  string
	sessionKeyFile    string
	sessionTTL        time.Duration
	tlsCertFile       string
	tlsKeyFile        string
	trustedProxyCIDRs []string
	allowInsecureHTTP bool
	requestTimeout    time.Duration
	shutdownTimeout   time.Duration
}

func (application *adapter) addServeCommand(root *cobra.Command) {
	options := &serveOptions{}
	command := &cobra.Command{
		Use:   "serve",
		Short: "Run the stateless BOAR HTTP control plane and embedded Web UI",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return application.runServe(command, options)
		},
	}
	flags := command.Flags()
	flags.BoolVar(&options.unsafe, "unsafe", false, "Disable login and transport security (default listen 0.0.0.0:8421)")
	flags.StringVar(&options.listen, "listen", "", "Server listen IP and port (default 127.0.0.1:8080; with --unsafe 0.0.0.0:8421)")
	flags.StringVar(&options.adminUsername, "admin-user", "", "Single administrator username (or BOAR_ADMIN_USERNAME)")
	flags.StringVar(&options.passwordHashFile, "password-hash-file", "", "Protected Argon2id PHC password hash file")
	flags.StringVar(&options.sessionKeyFile, "session-key-file", "", "Protected HMAC session key file")
	flags.DurationVar(&options.sessionTTL, "session-ttl", 0, "Signed session lifetime (default 30m)")
	flags.StringVar(&options.tlsCertFile, "tls-cert", "", "TLS certificate file")
	flags.StringVar(&options.tlsKeyFile, "tls-key", "", "Protected TLS private key file")
	flags.StringSliceVar(&options.trustedProxyCIDRs, "trusted-proxy-cidr", nil, "Trusted HTTPS reverse proxy CIDR (repeatable)")
	flags.BoolVar(&options.allowInsecureHTTP, "allow-insecure-http", false, "Explicitly allow cleartext HTTP on loopback only")
	flags.DurationVar(&options.requestTimeout, "request-timeout", 0, "Maximum duration of one API request (default 1m30s)")
	flags.DurationVar(&options.shutdownTimeout, "shutdown-timeout", 0, "Graceful shutdown deadline (default 10s)")
	root.AddCommand(command)
}

func (application *adapter) runServe(command *cobra.Command, options *serveOptions) error {
	if application.root.output != "" {
		return usageError("--output is not supported by the long-running serve command")
	}
	if options.unsafe {
		for _, name := range []string{
			"admin-user", "password-hash-file", "session-key-file", "session-ttl",
			"tls-cert", "tls-key", "trusted-proxy-cidr", "allow-insecure-http",
		} {
			if command.Flags().Changed(name) {
				return usageError("--unsafe cannot be combined with --" + name)
			}
		}
	}
	root := command.Root()
	dcsURL := application.root.dcsURL
	dcsURLSet := root.Flags().Changed("dcs-url")
	if root.Flags().Changed("dcs") {
		if dcsURLSet && application.root.dcsAlias != dcsURL {
			return usageError("--dcs-url and --dcs specify different values")
		}
		dcsURL, dcsURLSet = application.root.dcsAlias, true
	}
	invocation := ServeInvocation{
		Context: application.root.context, Unsafe: options.unsafe,
		Listen: options.listen, AdminUsername: options.adminUsername,
		PasswordHashFile: options.passwordHashFile, SessionKeyFile: options.sessionKeyFile,
		SessionTTL: options.sessionTTL, TLSCertFile: options.tlsCertFile, TLSKeyFile: options.tlsKeyFile,
		TrustedProxyCIDRs: append([]string(nil), options.trustedProxyCIDRs...),
		AllowInsecureHTTP: options.allowInsecureHTTP, RequestTimeout: options.requestTimeout,
		ShutdownTimeout: options.shutdownTimeout,
	}
	if root.Flags().Changed("config-file") {
		invocation.ConfigPath = application.root.configFile
	}
	if dcsURLSet {
		invocation.DCSURL = stringValue(dcsURL)
	}
	if root.Flags().Changed("insecure") {
		invocation.Insecure = boolValue(application.root.insecure)
	}
	if application.serve == nil {
		return usageError("serve is unavailable in this BOAR embedding")
	}
	if err := application.serve(command.Context(), invocation); err != nil {
		message := strings.TrimSpace(err.Error())
		if message == "" {
			message = "BOAR Server failed"
		}
		return &exitError{
			category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig),
			message: message, cause: err,
		}
	}
	return nil
}

func stringValue(value string) *string {
	copyValue := value
	return &copyValue
}

func boolValue(value bool) *bool {
	copyValue := value
	return &copyValue
}
