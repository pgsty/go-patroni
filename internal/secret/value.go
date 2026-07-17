// Package secret provides values and sanitizers that are safe by default for
// fmt, JSON, YAML, and slog. Plaintext access is deliberately explicit.
package secret

import (
	"encoding/json"
	"log/slog"
	"regexp"
	"sort"
	"strings"
)

const Redacted = "[REDACTED]"

// Value stores a credential without exposing it through common formatting or
// serialization paths. Value is immutable after construction.
type Value struct {
	plaintext string
}

func New(plaintext string) Value { return Value{plaintext: plaintext} }

func (v Value) IsSet() bool { return v.plaintext != "" }

// Reveal returns the plaintext for an immediate transport boundary. Callers
// must never place the result in errors, logs, plans, evidence, or argv.
func (v Value) Reveal() string { return v.plaintext }

func (v Value) String() string {
	if !v.IsSet() {
		return ""
	}
	return Redacted
}

func (v Value) GoString() string { return v.String() }

func (v Value) MarshalJSON() ([]byte, error) { return json.Marshal(v.String()) }

func (v Value) MarshalText() ([]byte, error) { return []byte(v.String()), nil }

func (v Value) MarshalYAML() (any, error) { return v.String(), nil }

func (v Value) LogValue() slog.Value { return slog.StringValue(v.String()) }

var (
	authorizationPattern  = regexp.MustCompile(`(?i)((?:proxy-)?authorization\s*:\s*(?:bearer|basic)\s+)[^\s,;]+`)
	cookieHeaderPattern   = regexp.MustCompile(`(?i)((?:set-cookie|cookie)\s*:\s*)[^\r\n]+`)
	uriPasswordPattern    = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^/\s:@]+:)([^@\s/]+)(@)`)
	dsnPasswordPattern    = regexp.MustCompile(`(?i)(\bpassword\s*=\s*)(?:'[^']*'|"[^"]*"|[^\s;]+)`)
	keyValueSecretPattern = regexp.MustCompile(`(?i)(\b(?:password|passwd|pwd|token|access[_-]?token|refresh[_-]?token|secret|api[_-]?key|access[_-]?key|client[_-]?secret|session[_-]?key)\s*=\s*)(?:'[^']*'|"[^"]*"|[^\s,;&]+)`)
	yamlSecretPattern     = regexp.MustCompile(`(?im)^(\s*(?:password|passwd|pwd|token|access[_-]?token|refresh[_-]?token|secret|authorization|cookie|api[_-]?key|access[_-]?key|client[_-]?secret|session[_-]?key|private[_-]?key)\s*:\s*).*$`)
	jsonSecretPattern     = regexp.MustCompile(`(?i)("(?:password|passwd|pwd|token|access[_-]?token|refresh[_-]?token|secret|authorization|cookie|api[_-]?key|access[_-]?key|client[_-]?secret|session[_-]?key|private[_-]?key)"\s*:\s*")[^"]*(")`)
	privateKeyPattern     = regexp.MustCompile(`(?s)-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----`)
)

// Redact sanitizes known values and common credential-bearing text shapes.
// It is a final defense for diagnostics, not permission to log payloads.
func Redact(text string, known ...Value) string {
	values := make([]string, 0, len(known))
	for _, value := range known {
		if value.IsSet() {
			values = append(values, value.Reveal())
		}
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	for _, value := range values {
		text = strings.ReplaceAll(text, value, Redacted)
	}
	text = authorizationPattern.ReplaceAllString(text, `${1}`+Redacted)
	text = cookieHeaderPattern.ReplaceAllString(text, `${1}`+Redacted)
	text = uriPasswordPattern.ReplaceAllString(text, `${1}`+Redacted+`${3}`)
	text = dsnPasswordPattern.ReplaceAllString(text, `${1}`+Redacted)
	text = keyValueSecretPattern.ReplaceAllString(text, `${1}`+Redacted)
	text = yamlSecretPattern.ReplaceAllStringFunc(text, func(line string) string {
		separator := strings.IndexByte(line, ':')
		if separator < 0 {
			return line
		}
		value := strings.TrimSpace(line[separator+1:])
		if value == "" || value == "null" || value == "~" {
			return line
		}
		return line[:separator+1] + " " + Redacted
	})
	text = jsonSecretPattern.ReplaceAllString(text, `${1}`+Redacted+`${2}`)
	text = privateKeyPattern.ReplaceAllString(text, Redacted)
	return text
}
