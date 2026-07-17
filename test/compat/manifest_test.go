package compat_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	patroni "github.com/pgsty/go-patroni"
)

const pinnedCommit = "d701f7b9c3d7e8cb400092d30170ff507697bce9"

type tests struct {
	Inventory    []string `json:"inventory"`
	Unit         []string `json:"unit"`
	Golden       []string `json:"golden"`
	Differential []string `json:"differential"`
	Integration  []string `json:"integration"`
	Contract     []string `json:"contract"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
}

func readJSON[T any](t *testing.T, name string) T {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s as deterministic JSON/YAML: %v", name, err)
	}
	return value
}

func TestPatroniSourcePin(t *testing.T) {
	manifest := readJSON[struct {
		Kind           string `json:"kind"`
		Commit         string `json:"commit"`
		Version        string `json:"version"`
		SourceKind     string `json:"sourceKind"`
		SupportedRange string `json:"supportedRange"`
		ContractFiles  []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"contractFiles"`
		Worktree struct {
			Clean bool `json:"contractFilesClean"`
		} `json:"worktree"`
	}](t, "compatibility/patroni-source.yaml")
	if manifest.Kind != "PatroniSourcePin" || manifest.Commit != pinnedCommit || manifest.Version != "4.1.4" {
		t.Fatalf("unexpected Patroni pin: kind=%q commit=%q version=%q", manifest.Kind, manifest.Commit, manifest.Version)
	}
	if manifest.SupportedRange != ">=3.0.0,<5.0.0" {
		t.Fatalf("unexpected supported range %q", manifest.SupportedRange)
	}
	if manifest.SourceKind != "pinned-official-source" {
		t.Fatalf("source pin depends on input provenance: %q", manifest.SourceKind)
	}
	if !manifest.Worktree.Clean {
		t.Fatal("generated pin must come from clean Patroni contract files")
	}
	if len(manifest.ContractFiles) != 6 {
		t.Fatalf("expected 6 pinned contract files, got %d", len(manifest.ContractFiles))
	}
	for _, file := range manifest.ContractFiles {
		if file.Path == "" || len(file.SHA256) != 64 {
			t.Errorf("invalid contract file pin: %#v", file)
		}
	}
}

func TestPatronictlInventory(t *testing.T) {
	expected := []string{
		"dsn", "query", "remove", "reload", "restart", "reinit", "failover", "switchover", "list",
		"topology", "flush", "pause", "resume", "edit-config", "show-config", "version", "history",
		"demote-cluster", "promote-cluster",
	}
	manifest := readJSON[struct {
		Kind                 string `json:"kind"`
		ExpectedCommandCount int    `json:"expectedCommandCount"`
		RootParameters       []struct {
			Name      string   `json:"name"`
			Flags     []string `json:"flags"`
			SourceRef string   `json:"sourceRef"`
		} `json:"rootParameters"`
		Commands []struct {
			Command    string   `json:"command"`
			Help       string   `json:"help"`
			SourceRef  string   `json:"sourceRef"`
			DataPaths  []string `json:"dataPaths"`
			Formats    []string `json:"formats"`
			Status     string   `json:"status"`
			Risk       string   `json:"risk"`
			Parameters []struct {
				Kind      string   `json:"kind"`
				Name      string   `json:"name"`
				Flags     []string `json:"flags"`
				SourceRef string   `json:"sourceRef"`
			} `json:"parameters"`
			Tests tests `json:"tests"`
		} `json:"commands"`
	}](t, "compatibility/patronictl.yaml")
	if manifest.Kind != "PatronictlCompatibility" {
		t.Fatalf("unexpected kind %q", manifest.Kind)
	}
	if manifest.ExpectedCommandCount != len(expected) || len(manifest.Commands) != len(expected) {
		t.Fatalf("expected %d commands, manifest declares %d and contains %d", len(expected), manifest.ExpectedCommandCount, len(manifest.Commands))
	}
	if len(manifest.RootParameters) != 3 {
		t.Fatalf("expected 3 root parameters, got %d", len(manifest.RootParameters))
	}
	for _, parameter := range manifest.RootParameters {
		if parameter.Name == "" || len(parameter.Flags) == 0 || parameter.SourceRef == "" {
			t.Errorf("incomplete root parameter: %#v", parameter)
		}
	}
	actual := make([]string, 0, len(manifest.Commands))
	for _, command := range manifest.Commands {
		actual = append(actual, command.Command)
		if command.Help == "" || command.SourceRef == "" || command.Risk == "" {
			t.Errorf("%s lacks help/source/risk", command.Command)
		}
		if len(command.DataPaths) == 0 || len(command.Formats) == 0 {
			t.Errorf("%s lacks data path or format contract", command.Command)
		}
		if command.Status != "pending" && command.Status != "complete" {
			t.Errorf("%s has forbidden status %q", command.Command, command.Status)
		}
		if len(command.Tests.Inventory) == 0 {
			t.Errorf("%s lacks source-inventory test link", command.Command)
		}
		seenParameters := map[string]bool{}
		for _, parameter := range command.Parameters {
			key := parameter.Kind + ":" + parameter.Name
			if parameter.Name == "" || parameter.SourceRef == "" {
				t.Errorf("%s has incomplete parameter %#v", command.Command, parameter)
			}
			if seenParameters[key] {
				t.Errorf("%s repeats parameter %s", command.Command, key)
			}
			seenParameters[key] = true
		}
	}
	if !slices.Equal(actual, expected) {
		t.Fatalf("command inventory drift:\nwant %v\n got %v", expected, actual)
	}
}

func TestRESTInventory(t *testing.T) {
	manifest := readJSON[struct {
		Kind                  string   `json:"kind"`
		HealthAliases         []string `json:"healthAliases"`
		ExpectedEndpointCount int      `json:"expectedEndpointCount"`
		Endpoints             []struct {
			ID          string `json:"id"`
			Method      string `json:"method"`
			Path        string `json:"path"`
			Handler     string `json:"handler"`
			Risk        string `json:"risk"`
			Request     string `json:"request"`
			Response    string `json:"response"`
			RawResponse bool   `json:"rawResponse"`
			SourceRef   string `json:"sourceRef"`
			Status      string `json:"status"`
			Since       string `json:"since"`
			Tests       tests  `json:"tests"`
		} `json:"endpoints"`
	}](t, "compatibility/rest-api.yaml")
	if manifest.Kind != "PatroniRESTCompatibility" {
		t.Fatalf("unexpected kind %q", manifest.Kind)
	}
	if len(manifest.HealthAliases) != 18 {
		t.Fatalf("expected 18 health aliases, got %d", len(manifest.HealthAliases))
	}
	if manifest.ExpectedEndpointCount != 75 || len(manifest.Endpoints) != 75 {
		t.Fatalf("expected 75 REST method/path rows, manifest declares %d and contains %d", manifest.ExpectedEndpointCount, len(manifest.Endpoints))
	}
	ids := map[string]bool{}
	pairs := map[string]bool{}
	catalog := patroni.EndpointCatalog()
	if len(catalog) != len(manifest.Endpoints) {
		t.Fatalf("SDK catalog contains %d rows, want %d", len(catalog), len(manifest.Endpoints))
	}
	required := map[string]bool{
		"GET /readiness": false, "GET /metrics": false, "POST /failsafe": false, "POST /sigterm": false,
		"POST /citus": false, "POST /mpp": false, "DELETE /restart": false, "DELETE /switchover": false,
	}
	for index, endpoint := range manifest.Endpoints {
		pair := endpoint.Method + " " + endpoint.Path
		if ids[endpoint.ID] {
			t.Errorf("duplicate REST id %q", endpoint.ID)
		}
		if pairs[pair] {
			t.Errorf("duplicate REST method/path %q", pair)
		}
		ids[endpoint.ID], pairs[pair] = true, true
		if _, ok := required[pair]; ok {
			required[pair] = true
		}
		if endpoint.ID == "" || endpoint.Handler == "" || endpoint.Risk == "" || endpoint.Response == "" || endpoint.SourceRef == "" {
			t.Errorf("incomplete REST entry for %s", pair)
		}
		if !endpoint.RawResponse {
			t.Errorf("%s lacks mandatory raw response escape hatch", pair)
		}
		if endpoint.Status != "pending" && endpoint.Status != "complete" {
			t.Errorf("%s has forbidden status %q", pair, endpoint.Status)
		}
		if len(endpoint.Tests.Inventory) == 0 {
			t.Errorf("%s lacks inventory test link", pair)
		}
		sdk := catalog[index]
		if sdk.ID != endpoint.ID || sdk.Method != endpoint.Method || sdk.Path != endpoint.Path || sdk.Since != endpoint.Since ||
			string(sdk.Risk) != endpoint.Risk || sdk.Request != endpoint.Request || sdk.Response != endpoint.Response {
			t.Errorf("REST SDK catalog row %d differs from source manifest:\nmanifest=%#v\nSDK=%#v", index, endpoint, sdk)
		}
	}
	for pair, found := range required {
		if !found {
			t.Errorf("required REST endpoint missing: %s", pair)
		}
	}
}

func TestDCSInventory(t *testing.T) {
	expected := []string{"initialize", "config", "members/{name}", "leader", "failover", "history", "status", "optime/leader", "sync", "failsafe"}
	manifest := readJSON[struct {
		Kind                   string   `json:"kind"`
		Backend                string   `json:"backend"`
		OtherBackendsSupported []string `json:"otherBackendsSupported"`
		ExpectedKeyCount       int      `json:"expectedKeyCount"`
		Keys                   []struct {
			Path            string   `json:"path"`
			ReadUse         string   `json:"readUse"`
			SDKDirectWrites []string `json:"sdkDirectWrites"`
			CAS             string   `json:"cas"`
			SourceRef       string   `json:"sourceRef"`
			Etcd3SourceRefs []string `json:"etcd3SourceRefs"`
			Status          string   `json:"status"`
			Tests           tests    `json:"tests"`
		} `json:"keys"`
	}](t, "compatibility/dcs.yaml")
	if manifest.Kind != "PatroniDCSCompatibility" || manifest.Backend != "etcd3" {
		t.Fatalf("unexpected DCS inventory kind/backend: %q/%q", manifest.Kind, manifest.Backend)
	}
	if len(manifest.OtherBackendsSupported) != 0 {
		t.Fatalf("MVP must expose no other DCS backend: %v", manifest.OtherBackendsSupported)
	}
	if manifest.ExpectedKeyCount != len(expected) || len(manifest.Keys) != len(expected) {
		t.Fatalf("expected %d DCS keys, manifest declares %d and contains %d", len(expected), manifest.ExpectedKeyCount, len(manifest.Keys))
	}
	actual := make([]string, 0, len(manifest.Keys))
	for _, key := range manifest.Keys {
		actual = append(actual, key.Path)
		if key.ReadUse == "" || len(key.SDKDirectWrites) == 0 || key.CAS == "" || key.SourceRef == "" || len(key.Etcd3SourceRefs) == 0 {
			t.Errorf("incomplete DCS key entry: %#v", key)
		}
		if key.Status != "pending" && key.Status != "complete" {
			t.Errorf("%s has forbidden status %q", key.Path, key.Status)
		}
		if len(key.Tests.Inventory) == 0 {
			t.Errorf("%s lacks inventory test link", key.Path)
		}
	}
	if !slices.Equal(actual, expected) {
		t.Fatalf("DCS key inventory drift:\nwant %v\n got %v", expected, actual)
	}
}

func TestDeviationPolicyStartsEmpty(t *testing.T) {
	manifest := readJSON[struct {
		Kind       string            `json:"kind"`
		Deviations []map[string]any  `json:"deviations"`
		Policy     map[string]string `json:"policy"`
	}](t, "compatibility/deviations.yaml")
	if manifest.Kind != "CompatibilityDeviations" || len(manifest.Deviations) != 0 {
		t.Fatalf("unexpected initial deviation manifest: kind=%q deviations=%d", manifest.Kind, len(manifest.Deviations))
	}
	for _, key := range []string{"default", "humanRendering", "wireOrMachineContract", "release"} {
		if strings.TrimSpace(manifest.Policy[key]) == "" {
			t.Errorf("deviation policy missing %s", key)
		}
	}
}

func Example_inventoryCount() {
	fmt.Println(19, 75, 10)
	// Output: 19 75 10
}
