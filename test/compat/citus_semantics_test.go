package compat_test

import (
	"reflect"
	"testing"
)

type citusCommandContract struct {
	GroupOption  string            `json:"groupOption"`
	OmittedGroup string            `json:"omittedGroup"`
	Targets      map[string]string `json:"targets"`
}

// TestPatronictlCitusSemanticsMatrix freezes the command-specific behavior
// extracted from Patroni's option_citus_group, option_default_citus_group,
// get_all_members, flush, and change_cluster_role code paths. Every one of the
// 19 commands must declare its omitted-group behavior.
func TestPatronictlCitusSemanticsMatrix(t *testing.T) {
	manifest := readJSON[struct {
		Commands []struct {
			Command string               `json:"command"`
			Citus   citusCommandContract `json:"citus"`
		} `json:"commands"`
	}](t, "compatibility/patronictl.yaml")
	want := map[string]citusCommandContract{
		"dsn":             {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"query":           {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"remove":          {GroupOption: "explicit", OmittedGroup: "rejected"},
		"reload":          {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"restart":         {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"reinit":          {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"failover":        {GroupOption: "explicit", OmittedGroup: "prompt-or-reject-with-force"},
		"switchover":      {GroupOption: "explicit", OmittedGroup: "prompt-or-reject-with-force"},
		"list":            {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"topology":        {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"flush":           {GroupOption: "explicit", OmittedGroup: "target-dependent", Targets: map[string]string{"restart": "all-groups", "switchover": "coordinator-group-0"}},
		"pause":           {GroupOption: "configured-default", OmittedGroup: "configured-group"},
		"resume":          {GroupOption: "configured-default", OmittedGroup: "configured-group"},
		"edit-config":     {GroupOption: "configured-default", OmittedGroup: "configured-group"},
		"show-config":     {GroupOption: "configured-default", OmittedGroup: "configured-group"},
		"version":         {GroupOption: "explicit", OmittedGroup: "all-groups"},
		"history":         {GroupOption: "configured-default", OmittedGroup: "configured-group"},
		"demote-cluster":  {GroupOption: "none", OmittedGroup: "coordinator-group-0"},
		"promote-cluster": {GroupOption: "none", OmittedGroup: "coordinator-group-0"},
	}
	if len(manifest.Commands) != len(want) {
		t.Fatalf("Citus matrix has %d commands, want %d", len(manifest.Commands), len(want))
	}
	seen := make(map[string]bool, len(manifest.Commands))
	for _, command := range manifest.Commands {
		expected, ok := want[command.Command]
		if !ok {
			t.Errorf("unexpected Citus command %q", command.Command)
			continue
		}
		seen[command.Command] = true
		if !reflect.DeepEqual(command.Citus, expected) {
			t.Errorf("%s Citus contract mismatch:\nwant %#v\n got %#v", command.Command, expected, command.Citus)
		}
	}
	for command := range want {
		if !seen[command] {
			t.Errorf("Citus contract is missing %s", command)
		}
	}
}
