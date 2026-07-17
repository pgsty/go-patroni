package model

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// Compare applies SemVer precedence to the numeric core and optional
// pre-release suffix. A release is newer than a pre-release with the same core.
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
	return comparePrerelease(v.Suffix, other.Suffix)
}

func comparePrerelease(left, right string) int {
	left = strings.TrimLeft(left, ".-")
	right = strings.TrimLeft(right, ".-")
	if left == "" && right == "" {
		return 0
	}
	if left == "" {
		return 1
	}
	if right == "" {
		return -1
	}
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	for index := 0; index < len(leftParts) && index < len(rightParts); index++ {
		leftNumber, leftNumeric := numericIdentifier(leftParts[index])
		rightNumber, rightNumeric := numericIdentifier(rightParts[index])
		switch {
		case leftNumeric && rightNumeric:
			if len(leftNumber) < len(rightNumber) ||
				(len(leftNumber) == len(rightNumber) && leftNumber < rightNumber) {
				return -1
			}
			if len(leftNumber) > len(rightNumber) ||
				(len(leftNumber) == len(rightNumber) && leftNumber > rightNumber) {
				return 1
			}
		case leftNumeric:
			return -1
		case rightNumeric:
			return 1
		case leftParts[index] < rightParts[index]:
			return -1
		case leftParts[index] > rightParts[index]:
			return 1
		}
	}
	if len(leftParts) < len(rightParts) {
		return -1
	}
	if len(leftParts) > len(rightParts) {
		return 1
	}
	return 0
}

func numericIdentifier(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return "", false
		}
	}
	normalized := strings.TrimLeft(value, "0")
	if normalized == "" {
		normalized = "0"
	}
	return normalized, true
}

// VersionRange is inclusive at Min and exclusive at Max.
type VersionRange struct {
	Min Version
	Max Version
}

func (r VersionRange) Contains(version Version) bool {
	// Feature and major-version upper bounds fail closed for prereleases of the
	// boundary itself: <5.0.0 does not opt into 5.0.0-rc1.
	return version.Compare(r.Min) >= 0 && compareNumericCore(version, r.Max) < 0
}

func compareNumericCore(left, right Version) int {
	leftParts := [...]int{left.Major, left.Minor, left.Patch}
	rightParts := [...]int{right.Major, right.Minor, right.Patch}
	for index := range leftParts {
		if leftParts[index] < rightParts[index] {
			return -1
		}
		if leftParts[index] > rightParts[index] {
			return 1
		}
	}
	return 0
}

// NewVersionRange parses an inclusive lower bound and exclusive upper bound.
func NewVersionRange(minimum, maximum string) (VersionRange, error) {
	minimumVersion, err := ParseVersion(minimum)
	if err != nil {
		return VersionRange{}, fmt.Errorf("minimum Patroni version: %w", err)
	}
	maximumVersion, err := ParseVersion(maximum)
	if err != nil {
		return VersionRange{}, fmt.Errorf("maximum Patroni version: %w", err)
	}
	rangeValue := VersionRange{Min: minimumVersion, Max: maximumVersion}
	if err := rangeValue.Validate(); err != nil {
		return VersionRange{}, err
	}
	return rangeValue, nil
}

// Validate requires a non-empty increasing range contained within the SDK's
// audited Patroni range.
func (r VersionRange) Validate() error {
	if r.Min.Compare(r.Max) >= 0 {
		return errors.New("patroni version range minimum must be less than maximum")
	}
	audited := AuditedPatroniRange()
	if r.Min.Compare(audited.Min) < 0 || r.Max.Compare(audited.Max) > 0 {
		return fmt.Errorf("patroni version range %s is outside SDK range %s", r, audited)
	}
	return nil
}

func (r VersionRange) String() string {
	return fmt.Sprintf(">=%s,<%s", r.Min, r.Max)
}

var auditedPatroniRange = VersionRange{
	Min: Version{Major: 3, Minor: 0, Patch: 0},
	Max: Version{Major: 5, Minor: 0, Patch: 0},
}

// SupportedPatroniRange is retained as a source-compatible snapshot. SDK
// behavior uses AuditedPatroniRange and cannot be changed by mutating this
// variable.
// Deprecated: use AuditedPatroniRange.
var SupportedPatroniRange = auditedPatroniRange

// AuditedPatroniRange returns an immutable-by-copy SDK compatibility boundary.
func AuditedPatroniRange() VersionRange { return auditedPatroniRange }

// CheckPatroniVersion parses and enforces the audited SDK compatibility range.
func CheckPatroniVersion(value string) error {
	version, err := ParseVersion(value)
	if err != nil {
		return err
	}
	audited := AuditedPatroniRange()
	if !audited.Contains(version) {
		return fmt.Errorf("%w: %s is outside %s", ErrUnsupportedPatroniVersion, version, audited)
	}
	return nil
}
