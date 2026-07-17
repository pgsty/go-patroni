package control

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

// Discover returns a secret-safe summary of every Patroni cluster evidenced
// beneath one namespace. The built-in etcd3 discoverer supplies per-cluster
// snapshots from the same linearizable prefix read, avoiding N+1 DCS reads.
func (service *Service) Discover(ctx context.Context, request DiscoverRequest) Result[DiscoverData] {
	operationID := service.operationID()
	target := discoveryTarget(request.Context, request.Namespace)
	if !validContext(ctx) {
		return failedRead[DiscoverData](service, operationID, "discover", target, PathDCS, CategoryUsage, false, "discover requires a context", nil)
	}
	if err := target.Validate(false); err != nil {
		return failedRead[DiscoverData](service, operationID, "discover", target, PathDCS, CategoryUsage, false, "discovery target is invalid", err)
	}
	if service.discovery == nil {
		return failedRead[DiscoverData](service, operationID, "discover", target, PathDCS, CategoryConfig, false, "discover requires a DCS discoverer", nil)
	}
	clusters, evidence, failure := service.discoverClusters(ctx, "discover", target, request.AllowUnsupportedRead, model.ManagementUnmanaged)
	if failure != nil {
		return failedReadWithEvidence[DiscoverData](service, operationID, "discover", target, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	summaries := make([]model.ClusterSummary, 0, len(clusters))
	for _, cluster := range clusters {
		summaries = append(summaries, model.ClusterSummary{
			Target: cluster.Target, DiscoveryState: cluster.DiscoveryState, ManagementState: cluster.ManagementState,
			ReachabilityState: cluster.ReachabilityState, HealthState: cluster.HealthState,
			Revision: cluster.Revision, MemberCount: len(cluster.Members), Leader: cluster.Leader,
		})
	}
	return Result[DiscoverData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS,
		Data: DiscoverData{Clusters: summaries}, Evidence: evidence,
	}
}

// ListAll projects every discovered scope from a single namespace read.
func (service *Service) ListAll(ctx context.Context, request ListAllRequest) Result[ListData] {
	operationID := service.operationID()
	target := discoveryTarget(request.Context, request.Namespace)
	if !validContext(ctx) {
		return failedRead[ListData](service, operationID, "list", target, PathDCS, CategoryUsage, false, "list --all requires a context", nil)
	}
	if err := target.Validate(false); err != nil {
		return failedRead[ListData](service, operationID, "list", target, PathDCS, CategoryUsage, false, "list --all target is invalid", err)
	}
	if service.discovery == nil {
		return failedRead[ListData](service, operationID, "list", target, PathDCS, CategoryConfig, false, "list --all requires a DCS discoverer", nil)
	}
	clusters, evidence, failure := service.discoverClusters(ctx, "list", target, request.AllowUnsupportedRead, model.ManagementAllSelected)
	if failure != nil {
		return failedReadWithEvidence[ListData](service, operationID, "list", target, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	return Result[ListData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS,
		Data: ListData{Clusters: clusters}, Evidence: evidence,
	}
}

// TopologyAll projects deterministic per-cluster topology trees from the same
// bounded namespace snapshot used for discovery.
func (service *Service) TopologyAll(ctx context.Context, request TopologyAllRequest) Result[TopologyListData] {
	operationID := service.operationID()
	target := discoveryTarget(request.Context, request.Namespace)
	if !validContext(ctx) {
		return failedRead[TopologyListData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "topology --all requires a context", nil)
	}
	if err := target.Validate(false); err != nil {
		return failedRead[TopologyListData](service, operationID, "topology", target, PathDCS, CategoryUsage, false, "topology --all target is invalid", err)
	}
	if service.discovery == nil {
		return failedRead[TopologyListData](service, operationID, "topology", target, PathDCS, CategoryConfig, false, "topology --all requires a DCS discoverer", nil)
	}
	clusters, evidence, failure := service.discoverClusters(ctx, "topology", target, request.AllowUnsupportedRead, model.ManagementAllSelected)
	if failure != nil {
		return failedReadWithEvidence[TopologyListData](service, operationID, "topology", target, PathDCS, failure.category, failure.retryable, failure.message, failure.cause, evidence)
	}
	topologies := make([]TopologyData, 0, len(clusters))
	for _, cluster := range clusters {
		topologies = append(topologies, TopologyData{Cluster: cluster, Members: topologyMembers(cluster)})
	}
	return Result[TopologyListData]{
		OperationID: operationID, Outcome: Succeeded, Target: target, Path: PathDCS,
		Data: TopologyListData{Topologies: topologies}, Evidence: evidence,
	}
}

type discoveryFailure struct {
	category  Category
	retryable bool
	message   string
	cause     error
}

func (service *Service) discoverClusters(
	ctx context.Context,
	operation string,
	target model.Target,
	allowUnsupportedRead bool,
	management model.ManagementState,
) ([]model.Cluster, []Evidence, *discoveryFailure) {
	items, err := service.discovery.Discover(ctx, dcs.DiscoveryRequest{Context: target.Context, Namespace: target.Namespace})
	if err != nil {
		category, retryable := classifyReadError(err)
		return nil, nil, &discoveryFailure{category: category, retryable: retryable, message: "namespace discovery failed", cause: err}
	}
	sort.SliceStable(items, func(left, right int) bool { return targetLess(items[left].Target, items[right].Target) })
	evidence := []Evidence{discoveryEvidence(service, target, items)}
	clusters := make([]model.Cluster, 0, len(items))
	for _, item := range items {
		itemTarget := item.Target.Normalize()
		if err := itemTarget.Validate(true); err != nil || itemTarget.Context != target.Context || itemTarget.Namespace != target.Namespace || itemTarget.Member != "" {
			if err == nil {
				err = fmt.Errorf("discovered target does not belong to requested context/namespace")
			}
			return nil, evidence, &discoveryFailure{category: CategoryFailed, message: "DCS discoverer returned an invalid target", cause: err}
		}
		var snapshot dcs.Snapshot
		if item.Snapshot != nil {
			snapshot = *item.Snapshot
			snapshotTarget := snapshot.Target.Normalize()
			if snapshotTarget.ClusterID() != itemTarget.ClusterID() {
				return nil, evidence, &discoveryFailure{category: CategoryFailed, message: "DCS discoverer returned a mismatched snapshot", cause: fmt.Errorf("snapshot target %s does not match discovered target %s", snapshotTarget.ClusterID(), itemTarget.ClusterID())}
			}
		} else {
			// Compatibility path for external Discoverer implementations created
			// before same-read snapshots were added to DiscoveredCluster.
			var readErr error
			snapshot, readErr = service.snapshots.Snapshot(ctx, itemTarget)
			if readErr != nil {
				category, retryable := classifyReadError(readErr)
				return nil, evidence, &discoveryFailure{category: category, retryable: retryable, message: "discovered cluster snapshot failed", cause: readErr}
			}
			evidence = append(evidence, snapshotEvidence(service, snapshot, "legacy discoverer cluster snapshot read"))
		}
		if versionErr := checkSnapshotPatroniVersion(snapshot, allowUnsupportedRead); versionErr != nil {
			evidence = append(evidence, snapshotEvidence(service, snapshot, "Patroni compatibility range checked during "+operation))
			return nil, evidence, &discoveryFailure{category: CategoryUnsupported, message: operation + " requires Patroni " + supportedPatroniRangeText, cause: versionErr}
		}
		cluster := projectCluster(snapshot, itemTarget)
		cluster.DiscoveryState = model.DiscoveryDiscovered
		cluster.ManagementState = management
		cluster.ReachabilityState = model.ReachabilityUnknown
		cluster.HealthState = model.HealthUnknown
		clusters = append(clusters, cluster)
	}
	return clusters, evidence, nil
}

func discoveryTarget(contextName, namespace string) model.Target {
	return (model.Target{Context: contextName, Namespace: namespace}).Normalize()
}

func discoveryEvidence(service *Service, target model.Target, clusters []dcs.DiscoveredCluster) Evidence {
	revision := int64(0)
	for _, cluster := range clusters {
		if cluster.Revision > revision {
			revision = cluster.Revision
		}
	}
	evidence := Evidence{
		Source: EvidenceDCS, ObservedAt: service.now(),
		Summary: fmt.Sprintf("bounded namespace discovery returned %d cluster(s)", len(clusters)),
		Path:    dcs.NamespacePrefix(target.Namespace),
	}
	if revision > 0 {
		evidence.Revision = strconv.FormatInt(revision, 10)
	}
	return evidence
}

func targetLess(left, right model.Target) bool {
	left, right = left.Normalize(), right.Normalize()
	if left.Scope != right.Scope {
		return left.Scope < right.Scope
	}
	leftGroup, rightGroup := -1, -1
	if left.Group != nil {
		leftGroup = *left.Group
	}
	if right.Group != nil {
		rightGroup = *right.Group
	}
	return leftGroup < rightGroup
}
