package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pgsty/go-patroni/cli"
	"github.com/pgsty/go-patroni/config"
	patroniruntime "github.com/pgsty/go-patroni/runtime"
	"github.com/spf13/cobra"
)

func TestZeroOptionsPreserveStandaloneIdentity(t *testing.T) {
	var stdout, stderr bytes.Buffer
	root := cli.NewRootCommand(cli.Options{
		Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
	})
	if root.Name() != "patronictl" || !strings.Contains(root.Short, "Patroni") || root.Version == "" {
		t.Fatalf("unexpected default identity: name=%q short=%q version=%q", root.Name(), root.Short, root.Version)
	}
	root.SetArgs([]string{"version"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stdout.String(), "patronictl version ") || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestApplicationExtensionReceivesExplicitRootInvocation(t *testing.T) {
	var got cli.RootInvocation
	var stdout, stderr bytes.Buffer
	root := cli.NewRootCommand(cli.Options{
		Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
		Application: cli.Application{Name: "boar", Short: "BOAR control plane", Version: "1.2.3", RequestIDPrefix: "boar-cli"},
		Extensions: []cli.Extension{func(extension cli.ExtensionContext) *cobra.Command {
			return &cobra.Command{
				Use: "serve", Args: cobra.NoArgs,
				RunE: func(command *cobra.Command, _ []string) error {
					var err error
					got, err = extension.Invocation(command)
					return err
				},
			}
		}},
	})
	if root.Name() != "boar" || root.Short != "BOAR control plane" || root.Version != "1.2.3" {
		t.Fatalf("application identity was not applied: %#v", root)
	}
	root.SetArgs([]string{
		"--config-file", "/infra/conf/patronictl.yml",
		"--dcs", "etcd3://127.0.0.1:2379", "--insecure", "--context", "pg-meta", "serve",
	})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := cli.RootInvocation{
		ConfigFile: "/infra/conf/patronictl.yml", ConfigFileSet: true,
		DCSURL: "etcd3://127.0.0.1:2379", DCSURLSet: true,
		Insecure: true, InsecureSet: true, Context: "pg-meta",
	}
	if got != want || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("invocation=%#v want=%#v stdout=%q stderr=%q", got, want, stdout.String(), stderr.String())
	}
}

func TestMachineVersionKeepsSDKContractAndAddsHostIdentity(t *testing.T) {
	var stdout, stderr bytes.Buffer
	root := cli.NewRootCommand(cli.Options{
		Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
		Application: cli.Application{
			Name: "boar", Version: "boar-build",
			Info: &cli.ApplicationInfo{
				Name: "boar", Version: "1.2.3", Commit: "abc123", BuildTime: "now",
				GoVersion: "go-test", SupportedPatroni: ">=4.0.0,<5.0.0",
			},
		},
	})
	root.SetArgs([]string{"--output", "json", "version"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		APIVersion string `json:"apiVersion"`
		Metadata   struct {
			RequestID string `json:"requestId"`
		} `json:"metadata"`
		Data struct {
			MachineSchema string `json:"machineSchema"`
			Application   struct {
				Name             string `json:"name"`
				Version          string `json:"version"`
				SupportedPatroni string `json:"supportedPatroni"`
			} `json:"application"`
		} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.APIVersion != cli.MachineAPIVersion || envelope.Data.MachineSchema != cli.MachineAPIVersion {
		t.Fatalf("machine contract drifted: %#v", envelope)
	}
	if envelope.Data.Application.Name != "boar" || envelope.Data.Application.Version != "1.2.3" ||
		envelope.Data.Application.SupportedPatroni != ">=4.0.0,<5.0.0" ||
		!strings.HasPrefix(envelope.Metadata.RequestID, "boar-cli-") {
		t.Fatalf("host identity missing: %#v", envelope)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestExplicitConfigFlagOverridesInjectedDocument(t *testing.T) {
	injected, err := config.Parse([]byte("scope: injected\n"), "injected")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "patronictl.yml")
	if err := os.WriteFile(path, []byte("scope: explicit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	root := cli.NewRootCommand(cli.Options{
		Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
		Environment: patroniruntime.EnvironmentOptions{Document: injected},
	})
	root.SetArgs([]string{"--config-file", path, "--output", "json", "inspect-config"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data struct {
			Target struct {
				Scope string `json:"scope"`
			} `json:"target"`
		} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.Target.Scope != "explicit" || stderr.Len() != 0 {
		t.Fatalf("scope=%q stderr=%q", envelope.Data.Target.Scope, stderr.String())
	}
}
