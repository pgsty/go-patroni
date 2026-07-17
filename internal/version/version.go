// Package version owns build metadata injected by release ldflags.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

var (
	Version   = "0.0.0-dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

const (
	SupportedPatroni = ">=3.0.0,<5.0.0"
	MachineSchema    = "patroni.pgsty.com/v1alpha1"
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
	version := Version
	if version == "0.0.0-dev" {
		if build, ok := debug.ReadBuildInfo(); ok {
			version = moduleVersion(build.Main.Path, build.Main.Version, version)
		}
	}
	return Info{
		Version: version, Commit: Commit, BuildTime: BuildTime, GoVersion: runtime.Version(),
		SupportedPatroni: SupportedPatroni, MachineSchema: MachineSchema,
	}
}

func moduleVersion(path, reported, fallback string) string {
	if path != "github.com/pgsty/go-patroni" {
		return fallback
	}
	reported = strings.TrimSpace(reported)
	if reported == "" || reported == "(devel)" {
		return fallback
	}
	return strings.TrimPrefix(reported, "v")
}

// String is suitable for Cobra's built-in --version flag.
func String() string {
	info := Current()
	return fmt.Sprintf("%s (commit=%s, built=%s, go=%s, patroni=%s, schema=%s)",
		info.Version, info.Commit, info.BuildTime, info.GoVersion, info.SupportedPatroni, info.MachineSchema)
}
