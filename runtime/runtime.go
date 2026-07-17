// Package runtime wires Patroni configuration to concrete SDK clients.
// Adapters use it only at bootstrap; command and HTTP handlers consume the
// resulting public control.Service and never rebuild infrastructure mid-call.
package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs/etcd3"
	"github.com/pgsty/go-patroni/internal/version"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
)

type EnvironmentOptions struct {
	Load      config.LoadRequest
	Overrides config.Overrides
	Document  *config.Document
	// UserAgent identifies an embedding application on Patroni REST requests.
	// Empty uses go-patroni/<version>.
	UserAgent string
	// ProductVersion is returned by the high-level local version operation.
	// Empty uses the complete go-patroni build version string.
	ProductVersion string
	// SupportedPatroniRange lets an embedding product narrow the SDK's audited
	// range without mutating package-global state.
	SupportedPatroniRange *model.VersionRange
}

type Environment struct {
	document       *config.Document
	overrides      config.Overrides
	userAgent      string
	productVersion string
	supportedRange *model.VersionRange
}

type RuntimeOptions struct {
	Context            string
	Operation          config.Operation
	ExplicitScope      string
	ExplicitGroup      *int
	UseConfiguredGroup bool
	AllowMissingScope  bool
}

type Runtime struct {
	Service  *control.Service
	Resolved config.Resolved
	Target   model.Target
	Warnings []string

	closeOnce sync.Once
	closeErr  error
	close     func() error
}

func NewEnvironment(ctx context.Context, options EnvironmentOptions) (*Environment, error) {
	if ctx == nil {
		return nil, errors.New("patroni environment requires a context")
	}
	document := options.Document
	if document == nil {
		var err error
		document, err = config.Load(ctx, options.Load)
		if err != nil {
			return nil, err
		}
	}
	userAgent := strings.TrimSpace(options.UserAgent)
	if userAgent == "" {
		userAgent = "go-patroni/" + version.Current().Version
	}
	productVersion := strings.TrimSpace(options.ProductVersion)
	if productVersion == "" {
		productVersion = version.String()
	}
	var supportedRange *model.VersionRange
	if options.SupportedPatroniRange != nil {
		if err := options.SupportedPatroniRange.Validate(); err != nil {
			return nil, fmt.Errorf("patroni environment version range: %w", err)
		}
		copyRange := *options.SupportedPatroniRange
		supportedRange = &copyRange
	}
	return &Environment{
		document: document, overrides: options.Overrides,
		userAgent: userAgent, productVersion: productVersion,
		supportedRange: supportedRange,
	}, nil
}

func (environment *Environment) ContextNames() []string {
	if environment == nil || environment.document == nil {
		return nil
	}
	return environment.document.ContextNames()
}

func (environment *Environment) DefaultContext() string {
	if environment == nil || environment.document == nil {
		return model.DefaultContext
	}
	return environment.document.DefaultContext()
}

func (environment *Environment) Resolve(contextName string) (config.Resolved, error) {
	if environment == nil || environment.document == nil {
		return config.Resolved{}, errors.New("patroni environment is not initialized")
	}
	return environment.document.Resolve(config.ResolveRequest{Context: contextName, Overrides: environment.overrides})
}

// OpenConfiguration builds a local-only control runtime for effective
// configuration inspection. It deliberately does not resolve endpoints or
// open etcd, Patroni REST, or PostgreSQL connections.
func (environment *Environment) OpenConfiguration(ctx context.Context, contextName string) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("patroni configuration runtime requires a context")
	}
	if environment == nil || environment.document == nil {
		return nil, errors.New("patroni environment is not initialized")
	}
	resolved, err := environment.Resolve(contextName)
	if err != nil {
		return nil, err
	}
	if err := resolved.Validate(config.OperationInspect, ""); err != nil {
		return nil, err
	}
	target := (model.Target{
		Context: resolved.Context, Namespace: resolved.Namespace, Scope: resolved.Scope, Group: cloneInt(resolved.Group),
	}).Normalize()
	service, err := control.NewConfigurationService(control.ConfigurationServiceOptions{})
	if err != nil {
		return nil, err
	}
	return &Runtime{
		Service: service, Resolved: resolved, Target: target, Warnings: warningMessages(resolved.Warnings),
	}, nil
}

func (environment *Environment) Open(ctx context.Context, options RuntimeOptions) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("patroni runtime requires a context")
	}
	if environment == nil || environment.document == nil {
		return nil, errors.New("patroni environment is not initialized")
	}
	overrides := environment.overrides
	if options.ExplicitGroup != nil {
		overrides.Group = cloneInt(options.ExplicitGroup)
	}
	resolved, err := environment.document.Resolve(config.ResolveRequest{Context: options.Context, Overrides: overrides})
	if err != nil {
		return nil, err
	}
	validationOperation := options.Operation
	if options.AllowMissingScope && strings.TrimSpace(options.ExplicitScope) == "" && strings.TrimSpace(resolved.Scope) == "" {
		validationOperation = config.OperationDiscover
	}
	if err := resolved.Validate(validationOperation, options.ExplicitScope); err != nil {
		return nil, err
	}

	targetGroup := options.ExplicitGroup
	if targetGroup == nil && options.UseConfiguredGroup {
		targetGroup = resolved.Group
	}
	scope := strings.TrimSpace(options.ExplicitScope)
	if scope == "" {
		scope = resolved.Scope
	}
	target := (model.Target{
		Context: resolved.Context, Namespace: resolved.Namespace, Scope: scope, Group: cloneInt(targetGroup),
	}).Normalize()

	endpoints, err := resolveEtcdEndpoints(ctx, resolved.Etcd3, resolved.Network.DNSLookupTimeout)
	if err != nil {
		return nil, err
	}
	var etcdTLS *tls.Config
	if resolved.Etcd3.Protocol == "https" || resolved.Etcd3.TLS.CAFile != "" || resolved.Etcd3.TLS.CertFile != "" {
		transport, tlsError := patroni.NewHTTPTransport(ctx, patroni.TLSOptions{
			CAFile: resolved.Etcd3.TLS.CAFile, CertFile: resolved.Etcd3.TLS.CertFile, KeyFile: resolved.Etcd3.TLS.KeyFile,
		}.WithKeyPassword(resolved.Etcd3.TLS.KeyPassword.Reveal()))
		if tlsError != nil {
			return nil, tlsError
		}
		etcdTLS = transport.TLSClientConfig.Clone()
		transport.CloseIdleConnections()
	}
	store, err := etcd3.New(ctx, (etcd3.Options{
		Endpoints: endpoints, TLS: etcdTLS, Username: resolved.Etcd3.Username,
		DialTimeout: resolved.Network.DCSDialTimeout, RequestTimeout: resolved.Network.DCSRequestTimeout,
	}).WithPassword(resolved.Etcd3.Password.Reveal()))
	if err != nil {
		return nil, err
	}
	cleanupStore := true
	defer func() {
		if cleanupStore {
			_ = store.Close()
		}
	}()

	restTransport, err := patroni.NewHTTPTransport(ctx, patroni.TLSOptions{
		CAFile: resolved.REST.CAFile, CertFile: resolved.REST.CertFile, KeyFile: resolved.REST.KeyFile,
		InsecureSkipVerify: resolved.REST.Insecure,
	}.WithKeyPassword(resolved.REST.KeyPassword.Reveal()))
	if err != nil {
		return nil, err
	}
	cleanupTransport := true
	defer func() {
		if cleanupTransport {
			restTransport.CloseIdleConnections()
		}
	}()
	var authorizer patroni.Authorizer
	if resolved.REST.Username != "" || resolved.REST.Password.IsSet() {
		authorizer = patroni.NewBasicAuth(resolved.REST.Username, resolved.REST.Password.Reveal())
	}
	restClient, err := patroni.NewClient(patroni.ClientOptions{
		Transport: restTransport, Authorizer: authorizer, UserAgent: environment.userAgent,
		Timeout: resolved.Network.PatroniTimeout,
	})
	if err != nil {
		return nil, err
	}
	queryClient, err := postgres.NewClient(postgres.ClientOptions{
		Timeout: resolved.Network.PostgresTimeout, CloseTimeout: resolved.Network.PostgresCloseTimeout,
	})
	if err != nil {
		return nil, err
	}
	service, err := control.NewService(control.ServiceOptions{
		Snapshots: store, Discovery: store, Patroni: restClient, Postgres: queryClient,
		Config: store, Failover: store, Remover: store, ProductVersion: environment.productVersion,
		SupportedPatroniRange: environment.supportedRange,
	})
	if err != nil {
		return nil, err
	}
	cleanupStore, cleanupTransport = false, false
	return &Runtime{
		Service: service, Resolved: resolved, Target: target, Warnings: warningMessages(resolved.Warnings),
		close: func() error {
			restTransport.CloseIdleConnections()
			return store.Close()
		},
	}, nil
}

func warningMessages(warnings []config.Warning) []string {
	messages := make([]string, 0, len(warnings))
	for _, warning := range warnings {
		messages = append(messages, warning.Message)
	}
	return messages
}

func (runtime *Runtime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		if runtime.close != nil {
			runtime.closeErr = runtime.close()
		}
	})
	return runtime.closeErr
}

func resolveEtcdEndpoints(ctx context.Context, projected config.Etcd3Config, lookupTimeout time.Duration) ([]string, error) {
	switch projected.Locator {
	case config.LocatorHost, config.LocatorHosts:
		return append([]string(nil), projected.Endpoints...), nil
	case config.LocatorURL:
		return []string{projected.URL}, nil
	case config.LocatorProxy:
		return []string{projected.Proxy}, nil
	case config.LocatorSRV:
		lookupContext, cancel := context.WithTimeout(ctx, lookupTimeout)
		defer cancel()
		return lookupEtcdSRV(lookupContext, projected.SRV)
	default:
		return nil, errors.New("etcd3 locator is not configured")
	}
}

func lookupEtcdSRV(ctx context.Context, locator string) ([]string, error) {
	service, protocol, name := "etcd-client", "tcp", strings.TrimSpace(locator)
	if strings.HasPrefix(name, "_") {
		parts := strings.SplitN(name, ".", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[1], "_") {
			return nil, errors.New("etcd3 srv locator is invalid")
		}
		service, protocol, name = strings.TrimPrefix(parts[0], "_"), strings.TrimPrefix(parts[1], "_"), parts[2]
	}
	_, records, err := net.DefaultResolver.LookupSRV(ctx, service, protocol, name)
	if err != nil {
		return nil, fmt.Errorf("resolve etcd3 srv locator: %w", err)
	}
	endpoints := make([]string, 0, len(records))
	for _, record := range records {
		host := strings.TrimSuffix(record.Target, ".")
		endpoints = append(endpoints, net.JoinHostPort(host, fmt.Sprintf("%d", record.Port)))
	}
	if len(endpoints) == 0 {
		return nil, errors.New("etcd3 srv locator returned no endpoints")
	}
	return endpoints, nil
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
