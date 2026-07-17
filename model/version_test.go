package model_test

import (
	"errors"
	"testing"

	"github.com/pgsty/go-patroni/model"
)

func TestPatroniVersionRange(t *testing.T) {
	tests := []struct {
		version   string
		supported bool
	}{
		{"2.1.0", false},
		{"3.0.0", true},
		{"3.3.8", true},
		{"4.0.0", true},
		{"4.0.7", true},
		{"4.1.0", true},
		{"4.99.99", true},
		{"5.0.0", false},
		{"5.0.0.dev1", false},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			version, err := model.ParseVersion(test.version)
			if err != nil {
				t.Fatal(err)
			}
			if got := model.SupportedPatroniRange.Contains(version); got != test.supported {
				t.Fatalf("Contains(%s)=%v want %v", test.version, got, test.supported)
			}
			err = model.CheckPatroniVersion(test.version)
			if test.supported && err != nil {
				t.Fatalf("supported version rejected: %v", err)
			}
			if !test.supported && !errors.Is(err, model.ErrUnsupportedPatroniVersion) {
				t.Fatalf("unsupported version error mismatch: %v", err)
			}
		})
	}
}

func TestParseVersion(t *testing.T) {
	version, err := model.ParseVersion("v4.1.0.dev2 (abcdef)")
	if err != nil {
		t.Fatal(err)
	}
	if version.Major != 4 || version.Minor != 1 || version.Patch != 0 || version.Suffix != ".dev2" {
		t.Fatalf("unexpected parse: %#v", version)
	}
	if version.String() != "4.1.0.dev2" {
		t.Fatalf("unexpected string %q", version.String())
	}
	for _, invalid := range []string{"", "4", "4.1", "four.1.0", "4.-1.0", "4.1.0/evil"} {
		if _, err := model.ParseVersion(invalid); err == nil {
			t.Errorf("invalid version accepted: %q", invalid)
		}
	}
}
