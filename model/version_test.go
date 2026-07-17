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
			if got := model.AuditedPatroniRange().Contains(version); got != test.supported {
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

func TestVersionPrereleasePrecedence(t *testing.T) {
	ordered := []string{
		"4.1.0-1",
		"4.1.0-alpha",
		"4.1.0-alpha.1",
		"4.1.0-alpha.beta",
		"4.1.0-beta",
		"4.1.0-beta.2",
		"4.1.0-beta.11",
		"4.1.0-rc.1",
		"4.1.0",
	}
	for index := 0; index < len(ordered)-1; index++ {
		left, leftErr := model.ParseVersion(ordered[index])
		right, rightErr := model.ParseVersion(ordered[index+1])
		if leftErr != nil || rightErr != nil {
			t.Fatalf("parse prerelease pair %q/%q: %v/%v", ordered[index], ordered[index+1], leftErr, rightErr)
		}
		if left.Compare(right) >= 0 || right.Compare(left) <= 0 {
			t.Fatalf("prerelease order %q < %q was not preserved", left, right)
		}
	}
	release, _ := model.ParseVersion("4.1.0")
	dev, _ := model.ParseVersion("4.1.0.dev2")
	if dev.Compare(release) >= 0 {
		t.Fatalf("Patroni development version %q was not ordered before release %q", dev, release)
	}
	huge, _ := model.ParseVersion("4.1.0-184467440737095516160")
	larger, _ := model.ParseVersion("4.1.0-184467440737095516161")
	if huge.Compare(larger) >= 0 {
		t.Fatal("arbitrary-length numeric prerelease identifiers were not ordered numerically")
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

func TestVersionRangeCanOnlyNarrowAuditedSDKRange(t *testing.T) {
	patroni4, err := model.NewVersionRange("4.0.0", "5.0.0")
	if err != nil {
		t.Fatal(err)
	}
	version3, _ := model.ParseVersion("3.3.8")
	version4, _ := model.ParseVersion("4.1.3")
	if patroni4.String() != ">=4.0.0,<5.0.0" || patroni4.Contains(version3) || !patroni4.Contains(version4) {
		t.Fatalf("narrow range contract = %s", patroni4)
	}
	for _, bounds := range [][2]string{{"2.0.0", "5.0.0"}, {"3.0.0", "6.0.0"}, {"4.0.0", "4.0.0"}} {
		if _, err := model.NewVersionRange(bounds[0], bounds[1]); err == nil {
			t.Fatalf("invalid narrowed range %v was accepted", bounds)
		}
	}
}

func TestDeprecatedRangeSnapshotCannotMutateSDKPolicy(t *testing.T) {
	original := model.SupportedPatroniRange
	t.Cleanup(func() { model.SupportedPatroniRange = original })
	model.SupportedPatroniRange = model.VersionRange{
		Min: model.Version{Major: 1},
		Max: model.Version{Major: 99},
	}
	if err := model.CheckPatroniVersion("2.1.0"); !errors.Is(err, model.ErrUnsupportedPatroniVersion) {
		t.Fatalf("mutable compatibility snapshot changed SDK policy: %v", err)
	}
	if _, err := model.NewVersionRange("2.1.0", "4.0.0"); err == nil {
		t.Fatal("mutable compatibility snapshot widened NewVersionRange")
	}
}
