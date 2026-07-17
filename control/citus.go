package control

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

func (service *Service) operationSnapshots(
	ctx context.Context,
	operation string,
	target model.Target,
	citus bool,
	allowUnsupportedRead bool,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	return service.operationSnapshotsWithVersionCheck(ctx, operation, target, citus, allowUnsupportedRead, checkSnapshotPatroniVersion)
}

// versionSnapshots differs only in its compatibility policy: the version
// diagnostic must be allowed to probe REST when a legacy DCS member record has
// no Patroni version. Explicit unsupported versions still fail closed unless
// the caller opted into a best-effort read.
func (service *Service) versionSnapshots(
	ctx context.Context,
	operation string,
	target model.Target,
	citus bool,
	allowUnsupportedRead bool,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	return service.operationSnapshotsWithVersionCheck(ctx, operation, target, citus, allowUnsupportedRead, checkSnapshotKnownPatroniVersion)
}

// citusCoordinatorSnapshot implements Patroni's get_dcs(scope, nil)
// coordinator rule for commands that do not expose a group selector. One
// namespace discovery identifies all groups, but only concrete group 0 is
// selected and subjected to the operation's compatibility gate.
func (service *Service) citusCoordinatorSnapshot(
	ctx context.Context,
	operation string,
	target model.Target,
	allowUnsupportedRead bool,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	target = target.Normalize()
	if target.Group != nil {
		return service.operationSnapshots(ctx, operation, target, true, allowUnsupportedRead)
	}
	coordinatorVersionCheck := func(snapshot dcs.Snapshot, allow bool) error {
		if snapshot.Target.Group != nil && *snapshot.Target.Group == 0 {
			return checkSnapshotPatroniVersion(snapshot, allow)
		}
		return nil
	}
	snapshots, evidence, failure := service.citusGroupSnapshotsWithVersionCheck(
		ctx, operation, target, allowUnsupportedRead, coordinatorVersionCheck,
	)
	if failure != nil {
		return nil, evidence, failure
	}
	for _, snapshot := range snapshots {
		if snapshot.Target.Group != nil && *snapshot.Target.Group == 0 {
			return []dcs.Snapshot{snapshot}, evidence, nil
		}
	}
	return nil, evidence, &discoveryFailure{category: CategoryNotFound, message: "Citus scope has no coordinator group 0"}
}

type snapshotVersionCheck func(dcs.Snapshot, bool) error

func (service *Service) operationSnapshotsWithVersionCheck(
	ctx context.Context,
	operation string,
	target model.Target,
	citus bool,
	allowUnsupportedRead bool,
	versionCheck snapshotVersionCheck,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	if citus {
		return service.citusGroupSnapshotsWithVersionCheck(ctx, operation, target, allowUnsupportedRead, versionCheck)
	}
	snapshot, err := service.snapshots.Snapshot(ctx, target)
	if err != nil {
		category, retryable := classifyReadError(err)
		return nil, nil, &discoveryFailure{category: category, retryable: retryable, message: operation + " cluster snapshot failed", cause: err}
	}
	if versionErr := versionCheck(snapshot, allowUnsupportedRead); versionErr != nil {
		evidence := []Evidence{snapshotEvidence(service, snapshot, "Patroni compatibility range checked during "+operation)}
		return nil, evidence, &discoveryFailure{category: CategoryUnsupported, message: operation + " requires Patroni " + supportedPatroniRangeText, cause: versionErr}
	}
	return []dcs.Snapshot{snapshot}, []Evidence{snapshotEvidence(service, snapshot, operation+" cluster snapshot read")}, nil
}

func snapshotGroupIDs(snapshots []dcs.Snapshot) string {
	groups := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Target.Group != nil {
			groups = append(groups, strconv.Itoa(*snapshot.Target.Group))
		}
	}
	return strings.Join(groups, ",")
}

type resolvedPlannedMember struct {
	target   model.Target
	member   dcs.Member
	snapshot dcs.Snapshot
}

func selectMemberAcrossSnapshots(snapshots []dcs.Snapshot, role Role, memberName string) (dcs.Snapshot, dcs.Member, bool) {
	for _, snapshot := range snapshots {
		if member, ok := selectMember(snapshot.Cluster, role, memberName); ok {
			return snapshot, member, true
		}
	}
	return dcs.Snapshot{}, dcs.Member{}, false
}

func resolvePlannedMembersAcrossSnapshots(
	service *Service,
	operation string,
	snapshots []dcs.Snapshot,
	targets []model.Target,
	role Role,
) ([]resolvedPlannedMember, []MemberWriteResult) {
	byCluster := make(map[string]dcs.Snapshot, len(snapshots))
	for _, snapshot := range snapshots {
		byCluster[snapshot.Target.Normalize().ClusterID()] = snapshot
	}
	resolved := make([]resolvedPlannedMember, 0, len(targets))
	conflicts := make([]MemberWriteResult, 0)
	for _, rawTarget := range targets {
		target := rawTarget.Normalize()
		snapshot, clusterExists := byCluster[target.ClusterID()]
		var member dcs.Member
		memberExists := false
		if clusterExists {
			for _, candidate := range snapshot.Cluster.Members {
				if candidate.Name == target.Member {
					member, memberExists = candidate, true
					break
				}
			}
		}
		if memberExists && memberMatchesRole(snapshot.Cluster, member, role) {
			resolved = append(resolved, resolvedPlannedMember{target: target, member: member, snapshot: snapshot})
			continue
		}
		evidence := Evidence{Source: EvidenceDCS, ObservedAt: service.now(), Summary: "confirmed member or Citus group is absent or no longer matches its role", Path: string(PathDCS), SendState: SendNotSent}
		if clusterExists {
			evidence.Revision = strconv.FormatInt(snapshot.Revision, 10)
			evidence.Path = snapshot.Prefix
		}
		errorValue := NewError(CategoryConflict, operation, target, false, operation+" was not sent because cluster membership changed", nil, evidence)
		conflicts = append(conflicts, MemberWriteResult{Target: target, Outcome: Failed, SendState: SendNotSent, Verification: VerifiedFailed, Summary: errorValue.Message, Evidence: []Evidence{evidence}, Error: errorValue})
	}
	return resolved, conflicts
}

// citusGroupSnapshots expands a group-less Citus target into the concrete
// Patroni group roots evidenced by one bounded namespace discovery. It keeps
// this orchestration in control.Service so CLI, Pig, and Server adapters never
// assemble DCS topology themselves.
func (service *Service) citusGroupSnapshots(
	ctx context.Context,
	operation string,
	target model.Target,
	allowUnsupportedRead bool,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	return service.citusGroupSnapshotsWithVersionCheck(ctx, operation, target, allowUnsupportedRead, checkSnapshotPatroniVersion)
}

func (service *Service) citusGroupSnapshotsWithVersionCheck(
	ctx context.Context,
	operation string,
	target model.Target,
	allowUnsupportedRead bool,
	versionCheck snapshotVersionCheck,
) ([]dcs.Snapshot, []Evidence, *discoveryFailure) {
	target = target.Normalize()
	if target.Group != nil {
		snapshot, err := service.snapshots.Snapshot(ctx, target)
		if err != nil {
			category, retryable := classifyReadError(err)
			return nil, nil, &discoveryFailure{category: category, retryable: retryable, message: "cluster snapshot failed", cause: err}
		}
		if versionErr := versionCheck(snapshot, allowUnsupportedRead); versionErr != nil {
			return nil, []Evidence{snapshotEvidence(service, snapshot, "Patroni compatibility range checked during "+operation)},
				&discoveryFailure{category: CategoryUnsupported, message: operation + " requires Patroni " + supportedPatroniRangeText, cause: versionErr}
		}
		return []dcs.Snapshot{snapshot}, []Evidence{snapshotEvidence(service, snapshot, "cluster snapshot read")}, nil
	}
	if service.discovery == nil {
		return nil, nil, &discoveryFailure{category: CategoryConfig, message: operation + " across Citus groups requires a DCS discoverer"}
	}
	items, err := service.discovery.Discover(ctx, dcs.DiscoveryRequest{Context: target.Context, Namespace: target.Namespace})
	if err != nil {
		category, retryable := classifyReadError(err)
		return nil, nil, &discoveryFailure{category: category, retryable: retryable, message: "Citus group discovery failed", cause: err}
	}
	sort.SliceStable(items, func(left, right int) bool { return targetLess(items[left].Target, items[right].Target) })
	evidence := []Evidence{discoveryEvidence(service, discoveryTarget(target.Context, target.Namespace), items)}
	snapshots := make([]dcs.Snapshot, 0)
	for _, item := range items {
		itemTarget := item.Target.Normalize()
		if itemTarget.Context != target.Context || itemTarget.Namespace != target.Namespace || itemTarget.Scope != target.Scope || itemTarget.Group == nil {
			continue
		}
		if err := itemTarget.Validate(true); err != nil || itemTarget.Member != "" {
			if err == nil {
				err = fmt.Errorf("discovered Citus target contains a member selector")
			}
			return nil, evidence, &discoveryFailure{category: CategoryFailed, message: "DCS discoverer returned an invalid Citus group target", cause: err}
		}
		var snapshot dcs.Snapshot
		if item.Snapshot != nil {
			snapshot = *item.Snapshot
			if snapshot.Target.Normalize().ClusterID() != itemTarget.ClusterID() {
				return nil, evidence, &discoveryFailure{category: CategoryFailed, message: "DCS discoverer returned a mismatched Citus group snapshot",
					cause: fmt.Errorf("snapshot target %s does not match discovered target %s", snapshot.Target.Normalize().ClusterID(), itemTarget.ClusterID())}
			}
		} else {
			var readErr error
			snapshot, readErr = service.snapshots.Snapshot(ctx, itemTarget)
			if readErr != nil {
				category, retryable := classifyReadError(readErr)
				return nil, evidence, &discoveryFailure{category: category, retryable: retryable, message: "Citus group snapshot failed", cause: readErr}
			}
			evidence = append(evidence, snapshotEvidence(service, snapshot, "legacy discoverer Citus group snapshot read"))
		}
		if versionErr := versionCheck(snapshot, allowUnsupportedRead); versionErr != nil {
			evidence = append(evidence, snapshotEvidence(service, snapshot, "Patroni compatibility range checked during "+operation))
			return nil, evidence, &discoveryFailure{category: CategoryUnsupported, message: operation + " requires Patroni " + supportedPatroniRangeText, cause: versionErr}
		}
		snapshots = append(snapshots, snapshot)
	}
	if len(snapshots) == 0 {
		return nil, evidence, &discoveryFailure{category: CategoryNotFound, message: "Citus scope has no discovered groups"}
	}
	return snapshots, evidence, nil
}
