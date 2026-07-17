package control

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

const supportedPatroniRangeText = ">=3.0.0,<5.0.0"

// checkSnapshotPatroniVersion rejects every explicit version outside the SDK's
// supported range. Empty legacy member versions are not invented: REST writes
// may probe them at the transport boundary, while an entirely offline DCS
// cluster remains removable through the separately confirmed delete flow.
func checkSnapshotPatroniVersion(snapshot dcs.Snapshot, allowUnsupportedRead bool) error {
	if allowUnsupportedRead {
		return nil
	}
	for _, member := range snapshot.Cluster.Members {
		version := strings.TrimSpace(member.Data.PatroniVersion)
		if version == "" {
			return fmt.Errorf("member %s has no version: %w", member.Name, model.ErrUnsupportedPatroniVersion)
		}
		if err := checkPatroniVersion(version); err != nil {
			return fmt.Errorf("member %s reports %q: %w", member.Name, version, err)
		}
	}
	return nil
}

// checkSnapshotKnownPatroniVersion lets the version diagnostic reach Patroni
// REST when a legacy/malformed member record omits its version. The REST value
// is still rejected unless the caller explicitly selected best-effort reads.
func checkSnapshotKnownPatroniVersion(snapshot dcs.Snapshot, allowUnsupportedRead bool) error {
	if allowUnsupportedRead {
		return nil
	}
	for _, member := range snapshot.Cluster.Members {
		version := strings.TrimSpace(member.Data.PatroniVersion)
		if version == "" {
			continue
		}
		if err := checkPatroniVersion(version); err != nil {
			return fmt.Errorf("member %s reports %q: %w", member.Name, version, err)
		}
	}
	return nil
}

func checkPatroniVersion(version string) error {
	if err := model.CheckPatroniVersion(strings.TrimSpace(version)); err != nil {
		if errors.Is(err, model.ErrUnsupportedPatroniVersion) {
			return err
		}
		return fmt.Errorf("%w: %v", model.ErrUnsupportedPatroniVersion, err)
	}
	return nil
}

func unsupportedVersionResult[T any](
	service *Service,
	operationID, operation string,
	target model.Target,
	path Path,
	snapshot dcs.Snapshot,
	cause error,
) Result[T] {
	return failedRead[T](service, operationID, operation, target, path, CategoryUnsupported, false,
		operation+" requires Patroni "+supportedPatroniRangeText, cause,
		snapshotEvidence(service, snapshot, "Patroni compatibility range checked before operation"))
}
