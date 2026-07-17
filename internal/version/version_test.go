package version

import "testing"

func TestModuleVersionOnlyUsesGoPatroniMainBuildInfo(t *testing.T) {
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
