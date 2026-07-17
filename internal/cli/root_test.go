package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

type commandManifest struct {
	ExpectedCommandCount int                 `json:"expectedCommandCount"`
	RootParameters       []manifestParameter `json:"rootParameters"`
	Commands             []struct {
		Command    string              `json:"command"`
		Parameters []manifestParameter `json:"parameters"`
	} `json:"commands"`
}

type manifestParameter struct {
	Kind    string   `json:"kind"`
	Name    string   `json:"name"`
	Flags   []string `json:"flags"`
	Default any      `json:"default"`
}

func loadCommandManifest(t *testing.T) commandManifest {
	t.Helper()
	path := filepath.Join("..", "..", "compatibility", "patronictl.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest commandManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestCobraTreeMatchesPinnedPatronictlInventory(t *testing.T) {
	manifest := loadCommandManifest(t)
	root := NewRootCommand(&bytes.Buffer{}, &bytes.Buffer{})
	root.InitDefaultCompletionCmd()

	expectedNames := make([]string, 0, len(manifest.Commands))
	for _, contract := range manifest.Commands {
		expectedNames = append(expectedNames, contract.Command)
		command, _, err := root.Find([]string{contract.Command})
		if err != nil || command == root || command.Name() != contract.Command {
			t.Fatalf("missing top-level command %q: command=%v err=%v", contract.Command, command, err)
		}
		assertManifestFlags(t, command, contract.Parameters)
	}
	sort.Strings(expectedNames)
	expectedTopLevelNames := append(append([]string(nil), expectedNames...), "discover", "inspect-config")
	sort.Strings(expectedTopLevelNames)
	actualNames := make([]string, 0, len(root.Commands()))
	for _, command := range root.Commands() {
		if command.Name() == "completion" || command.Name() == "help" {
			continue
		}
		actualNames = append(actualNames, command.Name())
	}
	sort.Strings(actualNames)
	if manifest.ExpectedCommandCount != 19 || !reflect.DeepEqual(actualNames, expectedTopLevelNames) {
		t.Fatalf("top-level command inventory = %v, want 19 compatibility commands plus exact go-patroni additions %v", actualNames, expectedTopLevelNames)
	}
	for _, name := range []string{"list", "topology"} {
		addition, _, err := root.Find([]string{name})
		if err != nil || addition.Flags().Lookup("all") == nil {
			t.Errorf("go-patroni additive command contract %s --all missing: command=%v err=%v", name, addition, err)
		}
	}
	if addition, _, err := root.Find([]string{"discover"}); err != nil || addition == root || addition.Name() != "discover" {
		t.Errorf("go-patroni additive discover command missing: command=%v err=%v", addition, err)
	}
	if addition, _, err := root.Find([]string{"inspect-config"}); err != nil || addition == root || addition.Name() != "inspect-config" {
		t.Errorf("go-patroni additive inspect-config command missing: command=%v err=%v", addition, err)
	}
	if completion, _, err := root.Find([]string{"completion"}); err != nil || completion.Name() != "completion" {
		t.Fatalf("Cobra completion command missing: command=%v err=%v", completion, err)
	}
	assertManifestFlags(t, root, manifest.RootParameters)
}

func assertManifestFlags(t *testing.T, command *cobra.Command, parameters []manifestParameter) {
	t.Helper()
	for _, parameter := range parameters {
		if parameter.Kind != "option" {
			continue
		}
		for _, spelling := range parameter.Flags {
			name := strings.TrimLeft(spelling, "-")
			if len(spelling) == 2 && strings.HasPrefix(spelling, "-") {
				flag := command.LocalNonPersistentFlags().ShorthandLookup(name)
				if command == command.Root() {
					flag = command.LocalNonPersistentFlags().ShorthandLookup(name)
				}
				if flag == nil {
					t.Errorf("%s lacks shorthand %s for %s", command.CommandPath(), spelling, parameter.Name)
				}
				continue
			}
			if flag := command.LocalNonPersistentFlags().Lookup(name); flag == nil {
				t.Errorf("%s lacks option %s for %s", command.CommandPath(), spelling, parameter.Name)
			}
		}
		literal, ok := manifestScalarDefault(parameter.Default)
		if !ok {
			continue
		}
		primary := manifestPrimaryLongFlag(parameter.Flags)
		if primary == "" {
			continue
		}
		if flag := command.LocalNonPersistentFlags().Lookup(primary); flag != nil && flag.DefValue != literal {
			t.Errorf("%s --%s default = %q, want %q", command.CommandPath(), primary, flag.DefValue, literal)
		}
	}
}

func manifestScalarDefault(value any) (string, bool) {
	switch typed := value.(type) {
	case nil, map[string]any, []any:
		return "", false
	case string:
		return typed, true
	case bool:
		if typed {
			return "true", true
		}
		return "false", true
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(jsonNumber(typed), "0"), "."), true
	default:
		return "", false
	}
}

func jsonNumber(value float64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func manifestPrimaryLongFlag(flags []string) string {
	for _, flag := range flags {
		if strings.HasPrefix(flag, "--") {
			return strings.TrimPrefix(flag, "--")
		}
	}
	return ""
}

func TestRootCompatibilityFlagsDoNotCollideWithQueryFlags(t *testing.T) {
	root := NewRootCommand(&bytes.Buffer{}, &bytes.Buffer{})
	root.SetArgs([]string{"--config-file", "fixture.yml", "query", "alpha", "-c", "select 1", "-d", "postgres", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("root/query -c and -d compatibility flags collide: %v", err)
	}
}

func TestBashZshAndFishCompletionGenerateWithoutRuntime(t *testing.T) {
	tests := map[string]string{
		"bash": "__start_patronictl", "zsh": "#compdef patronictl", "fish": "complete -c patronictl",
	}
	for shell, token := range tests {
		t.Run(shell, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			factory := func(context.Context, runtimeInvocation) (*commandRuntime, error) {
				t.Fatal("completion opened a Patroni runtime")
				return nil, nil
			}
			root := newRootCommand(strings.NewReader(""), &stdout, &stderr, factory)
			root.SetArgs([]string{"completion", shell})
			if err := root.ExecuteContext(context.Background()); err != nil || stderr.String() != "" {
				t.Fatalf("%s completion failed: err=%v stderr=%q", shell, err, stderr.String())
			}
			if !strings.Contains(stdout.String(), token) {
				t.Fatalf("%s completion omitted %q: %s", shell, token, stdout.String())
			}
		})
	}
}
