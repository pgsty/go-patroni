package config

import (
	"log/slog"

	internalsecret "github.com/pgsty/go-patroni/internal/secret"
)

const Redacted = internalsecret.Redacted

// Secret is a public config value that cannot reveal plaintext through normal
// formatting, JSON/YAML encoding, or slog.
type Secret struct {
	value internalsecret.Value
}

func newSecret(value string) Secret { return Secret{value: internalsecret.New(value)} }

func (s Secret) IsSet() bool { return s.value.IsSet() }

// Reveal returns plaintext only for an immediate authenticated transport.
func (s Secret) Reveal() string { return s.value.Reveal() }

func (s Secret) String() string { return s.value.String() }

func (s Secret) GoString() string { return s.value.GoString() }

func (s Secret) MarshalJSON() ([]byte, error) { return s.value.MarshalJSON() }

func (s Secret) MarshalText() ([]byte, error) { return s.value.MarshalText() }

func (s Secret) MarshalYAML() (any, error) { return s.value.MarshalYAML() }

func (s Secret) LogValue() slog.Value { return s.value.LogValue() }
