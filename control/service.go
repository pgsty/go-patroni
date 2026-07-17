package control

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	buildversion "github.com/pgsty/go-patroni/internal/version"
	"github.com/pgsty/go-patroni/model"
)

type ServiceOptions struct {
	Snapshots            dcs.SnapshotReader
	Discovery            dcs.Discoverer
	Patroni              PatroniControlClient
	Postgres             PostgresQueryExecutor
	Config               dcs.ConfigCAS
	Failover             dcs.FailoverCAS
	Remover              dcs.ClusterRemover
	Clock                func() time.Time
	NewOperationID       func() string
	ProductVersion       string
	RandomIndex          func(int) (int, error)
	Wait                 func(context.Context, time.Duration) error
	VerificationAttempts int
	// SupportedPatroniRange lets an embedding product narrow the SDK's audited
	// range. Nil uses model.AuditedPatroniRange; widening is rejected.
	SupportedPatroniRange *model.VersionRange
	// StandbyVerificationAttempts bounds DCS convergence observations after
	// demote-cluster and promote-cluster. Zero uses VerificationAttempts when
	// that legacy option is explicitly set, otherwise the Patroni-compatible
	// default policy.
	StandbyVerificationAttempts int
	// StandbyVerificationInterval controls the caller-cancelable delay between
	// standby-cluster DCS observations. Zero uses one second, matching
	// patronictl's source polling cadence.
	StandbyVerificationInterval time.Duration
}

// Service is the SDK's adapter-neutral high-level API. It stores transport
// capabilities and immutable policy only; a context is supplied to every I/O
// method and is never retained.
type Service struct {
	snapshots                   dcs.SnapshotReader
	discovery                   dcs.Discoverer
	patroni                     PatroniControlClient
	postgres                    PostgresQueryExecutor
	configDCS                   dcs.ConfigCAS
	failoverDCS                 dcs.FailoverCAS
	removerDCS                  dcs.ClusterRemover
	planKey                     [32]byte
	clock                       func() time.Time
	newOperationID              func() string
	productVersion              string
	randomIndex                 func(int) (int, error)
	wait                        func(context.Context, time.Duration) error
	verificationAttempts        int
	supportedPatroniRange       model.VersionRange
	standbyVerificationAttempts int
	standbyVerificationInterval time.Duration
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Snapshots == nil {
		return nil, errors.New("control service requires a DCS snapshot reader")
	}
	var planKey [32]byte
	if _, err := rand.Read(planKey[:]); err != nil {
		return nil, errors.New("control service could not initialize plan binding")
	}
	clock := options.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	newOperationID := options.NewOperationID
	if newOperationID == nil {
		newOperationID = randomOperationID
	}
	productVersion := strings.TrimSpace(options.ProductVersion)
	if productVersion == "" {
		productVersion = buildversion.Current().Version
	}
	randomIndex := options.RandomIndex
	if randomIndex == nil {
		randomIndex = cryptoRandomIndex
	}
	wait := options.Wait
	if wait == nil {
		wait = waitForContext
	}
	verificationAttempts := options.VerificationAttempts
	if verificationAttempts <= 0 {
		verificationAttempts = 3
	}
	supportedPatroniRange := model.AuditedPatroniRange()
	if options.SupportedPatroniRange != nil {
		if err := options.SupportedPatroniRange.Validate(); err != nil {
			return nil, fmt.Errorf("control service Patroni version range: %w", err)
		}
		supportedPatroniRange = *options.SupportedPatroniRange
	}
	standbyVerificationAttempts := options.StandbyVerificationAttempts
	if standbyVerificationAttempts <= 0 {
		if options.VerificationAttempts > 0 {
			standbyVerificationAttempts = verificationAttempts
		} else {
			// Patroni role changes are consumed by the HA loop. Observe once
			// immediately and then for up to 30 seconds at patronictl's one-second
			// cadence, while remaining bounded and caller-cancelable.
			standbyVerificationAttempts = 31
		}
	}
	standbyVerificationInterval := options.StandbyVerificationInterval
	if standbyVerificationInterval <= 0 {
		standbyVerificationInterval = time.Second
	}
	return &Service{
		snapshots: options.Snapshots, discovery: options.Discovery, patroni: options.Patroni, postgres: options.Postgres, configDCS: options.Config, failoverDCS: options.Failover,
		removerDCS: options.Remover,
		planKey:    planKey, clock: clock,
		newOperationID: newOperationID, productVersion: productVersion,
		randomIndex: randomIndex, wait: wait, verificationAttempts: verificationAttempts,
		supportedPatroniRange:       supportedPatroniRange,
		standbyVerificationAttempts: standbyVerificationAttempts, standbyVerificationInterval: standbyVerificationInterval,
	}, nil
}

func cryptoRandomIndex(length int) (int, error) {
	if length <= 0 {
		return 0, errors.New("random selection requires a positive length")
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(length)))
	if err != nil {
		return 0, err
	}
	return int(value.Int64()), nil
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomOperationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "patroni-operation-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return "patroni-operation-" + hex.EncodeToString(value[:])
}

func (service *Service) operationID() string {
	if service != nil && service.newOperationID != nil {
		if value := strings.TrimSpace(service.newOperationID()); value != "" {
			return value
		}
	}
	return randomOperationID()
}

func (service *Service) now() time.Time {
	if service == nil || service.clock == nil {
		return time.Now().UTC()
	}
	return service.clock().UTC()
}

func (service *Service) planToken(domain string, value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, service.planKey[:])
	_, _ = mac.Write([]byte("go-patroni/control/" + domain + "\x00"))
	_, _ = mac.Write(encoded)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func validContext(ctx context.Context) bool { return ctx != nil }

// formatPatroniTimestamp matches Python datetime.isoformat(): UTC is rendered
// with an explicit +00:00 offset instead of RFC3339's shorter Z spelling.
// Patroni persists and compares these values as ISO-8601 text in DCS.
func formatPatroniTimestamp(value time.Time) string {
	return value.Format("2006-01-02T15:04:05.999999999-07:00")
}
