// Package model contains normalized BOAR domain types. These types are kept
// separate from Patroni wire payloads and adapter-specific machine contracts.
package model

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	DefaultContext   = "default"
	DefaultNamespace = "/service"

	clusterIDPrefix = "boar:cluster/"
	memberIDPrefix  = "boar:member/"
)

// Target is the complete stable resource identity shared by BOAR adapters.
// Scope alone is deliberately not a globally unique identifier.
type Target struct {
	Context   string `json:"context" yaml:"context"`
	Namespace string `json:"namespace" yaml:"namespace"`
	Scope     string `json:"scope" yaml:"scope"`
	Group     *int   `json:"group,omitempty" yaml:"group,omitempty"`
	Member    string `json:"member,omitempty" yaml:"member,omitempty"`
}

// Normalize returns a copy with stable defaults and path normalization.
func (t Target) Normalize() Target {
	t.Context = strings.TrimSpace(t.Context)
	if t.Context == "" {
		t.Context = DefaultContext
	}
	t.Namespace = NormalizeNamespace(t.Namespace)
	t.Scope = strings.TrimSpace(t.Scope)
	t.Member = strings.TrimSpace(t.Member)
	if t.Group != nil {
		group := *t.Group
		t.Group = &group
	}
	return t
}

// NormalizeNamespace collapses slashes and returns a single leading slash.
func NormalizeNamespace(namespace string) string {
	rawParts := strings.FieldsFunc(strings.TrimSpace(namespace), func(r rune) bool { return r == '/' })
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return DefaultNamespace
	}
	return "/" + strings.Join(parts, "/")
}

// Validate checks the identity fields needed for discovery or cluster work.
func (t Target) Validate(requireScope bool) error {
	t = t.Normalize()
	if invalidIdentityPart(t.Context) {
		return errors.New("target context contains a forbidden control character")
	}
	if invalidIdentityPart(t.Namespace) {
		return errors.New("target namespace contains a forbidden control character")
	}
	if requireScope && t.Scope == "" {
		return errors.New("target scope is required")
	}
	if invalidIdentityPart(t.Scope) {
		return errors.New("target scope contains a forbidden control character")
	}
	if t.Member != "" && t.Scope == "" {
		return errors.New("target member requires a scope")
	}
	if invalidIdentityPart(t.Member) {
		return errors.New("target member contains a forbidden control character")
	}
	if t.Group != nil && *t.Group < 0 {
		return errors.New("target Citus group must be non-negative")
	}
	return nil
}

func invalidIdentityPart(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool { return r == 0 || r == '\n' || r == '\r' }) >= 0
}

// ClusterID returns a reversible escaped canonical cluster identity.
func (t Target) ClusterID() string {
	t = t.Normalize()
	group := "-"
	if t.Group != nil {
		group = strconv.Itoa(*t.Group)
	}
	return clusterIDPrefix + strings.Join([]string{
		url.PathEscape(t.Context), url.PathEscape(t.Namespace), url.PathEscape(t.Scope), group,
	}, "/")
}

// MemberID returns a reversible escaped canonical member identity, or an empty
// string when no member is selected.
func (t Target) MemberID() string {
	t = t.Normalize()
	if t.Member == "" {
		return ""
	}
	group := "-"
	if t.Group != nil {
		group = strconv.Itoa(*t.Group)
	}
	return memberIDPrefix + strings.Join([]string{
		url.PathEscape(t.Context), url.PathEscape(t.Namespace), url.PathEscape(t.Scope), group, url.PathEscape(t.Member),
	}, "/")
}

// ParseTargetID reverses ClusterID or MemberID and validates the result.
func ParseTargetID(value string) (Target, error) {
	var member bool
	var encoded string
	switch {
	case strings.HasPrefix(value, clusterIDPrefix):
		encoded = strings.TrimPrefix(value, clusterIDPrefix)
	case strings.HasPrefix(value, memberIDPrefix):
		member = true
		encoded = strings.TrimPrefix(value, memberIDPrefix)
	default:
		return Target{}, errors.New("target ID has an unknown prefix")
	}
	parts := strings.Split(encoded, "/")
	expected := 4
	if member {
		expected = 5
	}
	if len(parts) != expected {
		return Target{}, fmt.Errorf("target ID has %d segments, expected %d", len(parts), expected)
	}
	decoded := make([]string, len(parts))
	for index, part := range parts {
		var err error
		decoded[index], err = url.PathUnescape(part)
		if err != nil {
			return Target{}, fmt.Errorf("decode target ID segment %d: %w", index, err)
		}
	}
	target := Target{Context: decoded[0], Namespace: decoded[1], Scope: decoded[2]}
	if decoded[3] != "-" {
		group, err := strconv.Atoi(decoded[3])
		if err != nil {
			return Target{}, fmt.Errorf("decode target group: %w", err)
		}
		target.Group = &group
	}
	if member {
		target.Member = decoded[4]
	}
	target = target.Normalize()
	if target.Context == "" || target.Namespace == "" || target.Scope == "" || member && target.Member == "" {
		return Target{}, errors.New("target ID contains an empty required segment")
	}
	if err := target.Validate(true); err != nil {
		return Target{}, fmt.Errorf("invalid target ID: %w", err)
	}
	return target, nil
}
