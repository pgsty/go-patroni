package model

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var (
	versionPattern = regexp.MustCompile(`^v?([0-9]+)\.([0-9]+)\.([0-9]+)([.-][0-9A-Za-z.-]+)?(?: \([^\r\n()]*\))?$`)

	// ErrUnsupportedPatroniVersion identifies versions outside [3.0.0,5.0.0).
	ErrUnsupportedPatroniVersion = errors.New("unsupported Patroni version")
)

// Version is the numeric Patroni SemVer core plus an optional upstream suffix.
type Version struct {
	Major  int
	Minor  int
	Patch  int
	Suffix string
}

// ParseVersion accepts Patroni version strings and rejects ambiguous forms.
func ParseVersion(value string) (Version, error) {
	match := versionPattern.FindStringSubmatch(value)
	if match == nil {
		return Version{}, fmt.Errorf("invalid Patroni version %q", value)
	}
	parts := [3]int{}
	for index := range parts {
		parsed, err := strconv.Atoi(match[index+1])
		if err != nil {
			return Version{}, fmt.Errorf("invalid Patroni version %q: %w", value, err)
		}
		parts[index] = parsed
	}
	return Version{Major: parts[0], Minor: parts[1], Patch: parts[2], Suffix: match[4]}, nil
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d%s", v.Major, v.Minor, v.Patch, v.Suffix)
}

// Compare compares the numeric version core. Patroni pre-release suffixes do
// not permit crossing the supported major-version boundary.
func (v Version) Compare(other Version) int {
	left := [...]int{v.Major, v.Minor, v.Patch}
	right := [...]int{other.Major, other.Minor, other.Patch}
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

// VersionRange is inclusive at Min and exclusive at Max.
type VersionRange struct {
	Min Version
	Max Version
}

func (r VersionRange) Contains(version Version) bool {
	return version.Compare(r.Min) >= 0 && version.Compare(r.Max) < 0
}

var SupportedPatroniRange = VersionRange{
	Min: Version{Major: 3, Minor: 0, Patch: 0},
	Max: Version{Major: 5, Minor: 0, Patch: 0},
}

// CheckPatroniVersion parses and enforces the audited SDK compatibility range.
func CheckPatroniVersion(value string) error {
	version, err := ParseVersion(value)
	if err != nil {
		return err
	}
	if !SupportedPatroniRange.Contains(version) {
		return fmt.Errorf("%w: %s is outside >=3.0.0,<5.0.0", ErrUnsupportedPatroniVersion, version)
	}
	return nil
}
