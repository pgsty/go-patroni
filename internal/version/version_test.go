package version

import (
	"runtime/debug"
	"testing"
)

func TestModuleVersionValidatesModuleIdentity(t *testing.T) {
	tests := []struct {
		name, path, reported, fallback, want string
	}{
		{name: "released CLI", path: "github.com/pgsty/go-patroni", reported: "v0.1.2", fallback: "0.0.0-dev", want: "0.1.2"},
		{name: "local checkout", path: "github.com/pgsty/go-patroni", reported: "(devel)", fallback: "0.0.0-dev", want: "0.0.0-dev"},
		{name: "embedded in Pig", path: "github.com/pgsty/pig", reported: "v1.2.3", fallback: "0.0.0-dev", want: "0.0.0-dev"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := moduleVersion(test.path, test.reported, test.fallback); got != test.want {
				t.Fatalf("moduleVersion() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBuildModuleVersionSupportsEmbedders(t *testing.T) {
	tests := []struct {
		name     string
		build    *debug.BuildInfo
		fallback string
		want     string
	}{
		{
			name:     "standalone main module",
			build:    &debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "v0.1.3"}},
			fallback: "0.0.0-dev", want: "0.1.3",
		},
		{
			name: "embedded released dependency",
			build: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/pgsty/pig", Version: "v1.2.3"},
				Deps: []*debug.Module{{Path: modulePath, Version: "v0.1.3-0.20260717120448-a085a522bec7"}},
			},
			fallback: "0.0.0-dev", want: "0.1.3-0.20260717120448-a085a522bec7",
		},
		{
			name: "embedded versioned replacement",
			build: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/pgsty/boar", Version: "v0.2.0"},
				Deps: []*debug.Module{{
					Path: modulePath, Version: "v0.1.3",
					Replace: &debug.Module{Path: "github.com/example/go-patroni", Version: "v0.1.4"},
				}},
			},
			fallback: "0.0.0-dev", want: "0.1.4",
		},
		{
			name: "embedded local replacement",
			build: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/pgsty/boar", Version: "v0.2.0"},
				Deps: []*debug.Module{{
					Path: modulePath, Version: "v0.1.3",
					Replace: &debug.Module{Path: "../go-patroni", Version: "(devel)"},
				}},
			},
			fallback: "0.0.0-dev", want: "0.0.0-dev",
		},
		{name: "module absent", build: &debug.BuildInfo{Main: debug.Module{Path: "github.com/pgsty/pig"}}, fallback: "dev", want: "dev"},
		{name: "nil build info", fallback: "dev", want: "dev"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := buildModuleVersion(test.build, test.fallback); got != test.want {
				t.Fatalf("buildModuleVersion() = %q, want %q", got, test.want)
			}
		})
	}
}
