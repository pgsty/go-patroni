package dcs

import (
	"context"

	"github.com/pgsty/go-patroni/model"
)

// SnapshotReader provides fresh, revisioned Patroni cluster state. Control
// code must not retain a Snapshot as a later write precondition.
type SnapshotReader interface {
	Snapshot(context.Context, model.Target) (Snapshot, error)
}

// Discoverer performs one bounded namespace scan and returns only known
// Patroni root/member evidence in deterministic target order.
type Discoverer interface {
	Discover(context.Context, DiscoveryRequest) ([]DiscoveredCluster, error)
}

// Watcher begins strictly after afterRevision and emits a full resnapshot when
// the requested etcd history has been compacted.
type Watcher interface {
	Watch(context.Context, model.Target, int64) WatchStream
}

// ConfigCAS is deliberately capability-scoped; BOAR has no public arbitrary
// DCS put surface.
type ConfigCAS interface {
	CompareAndSwapConfig(context.Context, model.Target, []byte, *int64) (WriteResult, error)
}

// FailoverCAS contains the only direct failover-key mutations needed by the
// Patroni 4.x command-specific fallback algorithms.
type FailoverCAS interface {
	WriteFailover(context.Context, model.Target, []byte, *int64) (WriteResult, error)
	DeleteFailover(context.Context, model.Target, *int64) (WriteResult, error)
}

// ClusterRemover deletes an exact cluster root, never a textual sibling.
type ClusterRemover interface {
	DeleteCluster(context.Context, model.Target) (RemoveResult, error)
}

// Store is BOAR's complete Patroni-oriented DCS contract. Smaller control use
// cases should depend on the capability interface they actually need.
type Store interface {
	SnapshotReader
	Discoverer
	Watcher
	ConfigCAS
	FailoverCAS
	ClusterRemover
	Close() error
}
