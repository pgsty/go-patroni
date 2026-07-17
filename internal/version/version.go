// Package version owns build metadata injected by release ldflags.
package version

import (
	"fmt"
	"runtime"
)

var (
	Version   = "0.0.0-dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

const (
	SupportedPatroni = ">=4.0.0,<5.0.0"
	MachineSchema    = "boar.pgsty.com/v1alpha1"
)

// Info is the stable source for CLI and Server version adapters.
type Info struct {
	Version          string `json:"version" yaml:"version"`
	Commit           string `json:"commit" yaml:"commit"`
	BuildTime        string `json:"buildTime" yaml:"buildTime"`
	GoVersion        string `json:"goVersion" yaml:"goVersion"`
	SupportedPatroni string `json:"supportedPatroni" yaml:"supportedPatroni"`
	MachineSchema    string `json:"machineSchema" yaml:"machineSchema"`
}

// Current returns immutable build metadata.
func Current() Info {
	return Info{
		Version: Version, Commit: Commit, BuildTime: BuildTime, GoVersion: runtime.Version(),
		SupportedPatroni: SupportedPatroni, MachineSchema: MachineSchema,
	}
}

// String is suitable for Cobra's built-in --version flag.
func String() string {
	info := Current()
	return fmt.Sprintf("%s (commit=%s, built=%s, go=%s, patroni=%s, schema=%s)",
		info.Version, info.Commit, info.BuildTime, info.GoVersion, info.SupportedPatroni, info.MachineSchema)
}
