package cli

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestServeCommandPassesOnlyExplicitRootOverrides(t *testing.T) {
	var got ServeInvocation
	runs := 0
	runner := func(ctx context.Context, invocation ServeInvocation) error {
		if ctx == nil || ctx.Err() != nil {
			t.Fatalf("runner context=%v", ctx)
		}
		runs++
		got = invocation
		return nil
	}
	var stdout, stderr bytes.Buffer
	root := NewRootCommandWithServe(&stdout, &stderr, runner)
	root.SetArgs([]string{
		"--config-file", "/tmp/boar-fixture.yaml",
		"--dcs-url", "etcd3://127.0.0.1:2379",
		"--insecure",
		"--context", "staging",
		"serve",
		"--listen", "127.0.0.1:18080",
		"--admin-user", "operator",
		"--password-hash-file", "/tmp/password.hash",
		"--session-key-file", "/tmp/session.key",
		"--session-ttl", "20m",
		"--tls-cert", "/tmp/server.crt",
		"--tls-key", "/tmp/server.key",
		"--trusted-proxy-cidr", "10.0.0.0/8",
		"--trusted-proxy-cidr", "192.0.2.0/24",
		"--allow-insecure-http",
		"--request-timeout", "45s",
		"--shutdown-timeout", "8s",
	})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("runs=%d stdout=%q stderr=%q", runs, stdout.String(), stderr.String())
	}
	want := ServeInvocation{
		ConfigPath: "/tmp/boar-fixture.yaml", Context: "staging",
		DCSURL: stringPointer("etcd3://127.0.0.1:2379"), Insecure: boolPointer(true),
		Listen: "127.0.0.1:18080", AdminUsername: "operator",
		PasswordHashFile: "/tmp/password.hash", SessionKeyFile: "/tmp/session.key",
		SessionTTL: 20 * time.Minute, TLSCertFile: "/tmp/server.crt", TLSKeyFile: "/tmp/server.key",
		TrustedProxyCIDRs: []string{"10.0.0.0/8", "192.0.2.0/24"}, AllowInsecureHTTP: true,
		RequestTimeout: 45 * time.Second, ShutdownTimeout: 8 * time.Second,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("invocation=\n%#v\nwant=\n%#v", got, want)
	}
}

func TestServeCommandLeavesImplicitOverridesUnset(t *testing.T) {
	var got ServeInvocation
	root := NewRootCommandWithServe(&bytes.Buffer{}, &bytes.Buffer{}, func(_ context.Context, invocation ServeInvocation) error {
		got = invocation
		return nil
	})
	root.SetArgs([]string{"serve"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got.ConfigPath != "" || got.DCSURL != nil || got.Insecure != nil || got.Context != "" {
		t.Fatalf("implicit root options leaked as explicit overrides: %#v", got)
	}
	if got.Listen != "" || got.SessionTTL != 0 || got.RequestTimeout != 0 || got.ShutdownTimeout != 0 {
		t.Fatalf("implicit serve values should be resolved from config/built-ins by Server: %#v", got)
	}
}

func TestServeCommandUnsafeIsOneFlagZeroCredentialBootstrap(t *testing.T) {
	var got ServeInvocation
	root := NewRootCommandWithServe(&bytes.Buffer{}, &bytes.Buffer{}, func(_ context.Context, invocation ServeInvocation) error {
		got = invocation
		return nil
	})
	root.SetArgs([]string{"serve", "--unsafe"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !got.Unsafe {
		t.Fatalf("unsafe invocation was not preserved: %#v", got)
	}
	if got.AdminUsername != "" || got.PasswordHashFile != "" || got.SessionKeyFile != "" ||
		got.TLSCertFile != "" || got.TLSKeyFile != "" || len(got.TrustedProxyCIDRs) != 0 || got.AllowInsecureHTTP {
		t.Fatalf("unsafe bootstrap leaked implicit security configuration: %#v", got)
	}
}

func TestServeCommandHelpDocumentsUnsafeNetworkDefault(t *testing.T) {
	root := NewRootCommandWithServe(&bytes.Buffer{}, &bytes.Buffer{}, func(context.Context, ServeInvocation) error {
		t.Fatal("help invoked the Server runner")
		return nil
	})
	command, _, err := root.Find([]string{"serve"})
	if err != nil {
		t.Fatal(err)
	}
	unsafeFlag := command.Flags().Lookup("unsafe")
	listenFlag := command.Flags().Lookup("listen")
	if unsafeFlag == nil || !strings.Contains(unsafeFlag.Usage, "0.0.0.0:8421") ||
		listenFlag == nil || !strings.Contains(listenFlag.Usage, "with --unsafe 0.0.0.0:8421") {
		t.Fatalf("unsafe/listen help does not expose the one-command network default: unsafe=%v listen=%v", unsafeFlag, listenFlag)
	}
}

func TestServeCommandRejectsAdapterContractsBeforeRunner(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "machine output", args: []string{"--output", "json", "serve"}, want: "--output is not supported"},
		{name: "conflicting DCS aliases", args: []string{"--dcs-url", "etcd3://127.0.0.1:2379", "--dcs", "etcd3://127.0.0.2:2379", "serve"}, want: "specify different values"},
		{name: "positional argument", args: []string{"serve", "extra"}, want: "unknown command"},
		{name: "unsafe with password file", args: []string{"serve", "--unsafe", "--password-hash-file", "/tmp/password.hash"}, want: "--unsafe cannot be combined"},
		{name: "unsafe with TLS", args: []string{"serve", "--unsafe", "--tls-cert", "/tmp/server.crt", "--tls-key", "/tmp/server.key"}, want: "--unsafe cannot be combined"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runs := 0
			root := NewRootCommandWithServe(&bytes.Buffer{}, &bytes.Buffer{}, func(context.Context, ServeInvocation) error {
				runs++
				return nil
			})
			root.SetArgs(test.args)
			err := root.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) || runs != 0 {
				t.Fatalf("error=%v runs=%d want substring=%q", err, runs, test.want)
			}
		})
	}
}

func TestServeCommandMapsRunnerErrorWithoutOpeningCLIRuntime(t *testing.T) {
	want := errors.New("test-only server bootstrap rejection")
	root := NewRootCommandWithServe(&bytes.Buffer{}, &bytes.Buffer{}, func(context.Context, ServeInvocation) error { return want })
	root.SetArgs([]string{"serve"})
	err := root.ExecuteContext(context.Background())
	if !errors.Is(err, want) || exitCode(err) != 2 {
		t.Fatalf("error=%v code=%d", err, exitCode(err))
	}
}

func stringPointer(value string) *string { return &value }

func boolPointer(value bool) *bool { return &value }
