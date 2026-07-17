package config

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultDNSLookupTimeout     = 5 * time.Second
	defaultDCSDialTimeout       = 5 * time.Second
	defaultDCSRequestTimeout    = 10 * time.Second
	defaultPatroniTimeout       = 10 * time.Second
	defaultPostgresTimeout      = 30 * time.Second
	defaultPostgresCloseTimeout = 5 * time.Second
)

// NetworkConfig is the secret-free, operation-wide deadline projection from
// boar.network. Each value is an upper bound that is further constrained by a
// shorter caller deadline.
type NetworkConfig struct {
	DNSLookupTimeout     time.Duration
	DCSDialTimeout       time.Duration
	DCSRequestTimeout    time.Duration
	PatroniTimeout       time.Duration
	PostgresTimeout      time.Duration
	PostgresCloseTimeout time.Duration
}

func (configuration NetworkConfig) String() string {
	return fmt.Sprintf("config.NetworkConfig{dnsLookup:%s,dcsDial:%s,dcsRequest:%s,patroni:%s,postgres:%s,postgresClose:%s}",
		configuration.DNSLookupTimeout, configuration.DCSDialTimeout, configuration.DCSRequestTimeout,
		configuration.PatroniTimeout, configuration.PostgresTimeout, configuration.PostgresCloseTimeout)
}

func (configuration NetworkConfig) GoString() string { return configuration.String() }

// NetworkConfig projects boar.network while tolerating unknown fields. The
// returned defaults are part of the inspect-config contract and are never
// inferred independently by an adapter.
func (document *Document) NetworkConfig() (NetworkConfig, error) {
	if document == nil {
		return NetworkConfig{}, newError(ErrorProjection, "boar.network", "", "document is nil", nil)
	}
	projected := NetworkConfig{
		DNSLookupTimeout: defaultDNSLookupTimeout, DCSDialTimeout: defaultDCSDialTimeout,
		DCSRequestTimeout: defaultDCSRequestTimeout, PatroniTimeout: defaultPatroniTimeout,
		PostgresTimeout: defaultPostgresTimeout, PostgresCloseTimeout: defaultPostgresCloseTimeout,
	}
	if document.network == nil {
		return projected, nil
	}
	network, ok := document.network.(map[string]any)
	if !ok {
		return NetworkConfig{}, document.networkError("boar.network", "must be a mapping")
	}
	fields := []struct {
		key         string
		field       string
		destination *time.Duration
	}{
		{key: "dns_timeout", field: "boar.network.dns_timeout", destination: &projected.DNSLookupTimeout},
		{key: "dcs_dial_timeout", field: "boar.network.dcs_dial_timeout", destination: &projected.DCSDialTimeout},
		{key: "dcs_request_timeout", field: "boar.network.dcs_request_timeout", destination: &projected.DCSRequestTimeout},
		{key: "patroni_timeout", field: "boar.network.patroni_timeout", destination: &projected.PatroniTimeout},
		{key: "postgres_timeout", field: "boar.network.postgres_timeout", destination: &projected.PostgresTimeout},
		{key: "postgres_close_timeout", field: "boar.network.postgres_close_timeout", destination: &projected.PostgresCloseTimeout},
	}
	for _, item := range fields {
		value, exists := network[item.key]
		if !exists || value == nil {
			continue
		}
		text, valid := value.(string)
		if !valid || strings.TrimSpace(text) == "" {
			return NetworkConfig{}, document.networkError(item.field, "must be a positive Go duration")
		}
		duration, err := time.ParseDuration(strings.TrimSpace(text))
		if err != nil || duration <= 0 {
			return NetworkConfig{}, document.networkError(item.field, "must be a positive Go duration")
		}
		*item.destination = duration
	}
	return projected, nil
}

func (document *Document) networkError(field, message string) *Error {
	return newError(ErrorProjection, field, document.sourceName, message, nil)
}
