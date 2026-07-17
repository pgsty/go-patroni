package etcd3

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (store *Store) Watch(ctx context.Context, target model.Target, afterRevision int64) dcs.WatchStream {
	events := make(chan dcs.WatchEvent, 32)
	errorsChannel := make(chan error, 1)
	stream := dcs.WatchStream{Events: events, Errors: errorsChannel}
	go store.runWatch(ctx, target, afterRevision, events, errorsChannel)
	return stream
}

func (store *Store) runWatch(
	ctx context.Context,
	target model.Target,
	afterRevision int64,
	events chan<- dcs.WatchEvent,
	errorsChannel chan<- error,
) {
	defer close(events)
	defer close(errorsChannel)
	if ctx == nil {
		errorsChannel <- dcs.NewError(dcs.ErrorConfiguration, "watch", "", errors.New("context is nil"))
		return
	}
	if store == nil || store.client == nil {
		sendWatchError(ctx, errorsChannel, dcs.NewError(dcs.ErrorConfiguration, "watch", "", errors.New("store is nil")))
		return
	}
	prefix, err := dcs.ClusterPrefix(target)
	if err != nil {
		sendWatchError(ctx, errorsChannel, dcs.NewError(dcs.ErrorConfiguration, "watch", "", err))
		return
	}
	prefix += "/"
	revision := afterRevision + 1
	if revision < 1 {
		revision = 1
	}
	for {
		watchContext, cancel, contextErr := store.operationContext(ctx, "watch", prefix)
		if contextErr != nil {
			sendWatchError(ctx, errorsChannel, contextErr)
			return
		}
		watch := store.client.Watch(watchContext, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())
		restart := false
		for response := range watch {
			if response.Canceled {
				if response.CompactRevision > 0 {
					snapshot, snapshotErr := store.Snapshot(ctx, target)
					if snapshotErr != nil {
						sendWatchError(ctx, errorsChannel, snapshotErr)
						return
					}
					if !sendWatchEvent(ctx, events, dcs.WatchEvent{
						Type: dcs.WatchResync, Revision: snapshot.Revision, Snapshot: &snapshot, At: time.Now().UTC(),
					}) {
						return
					}
					revision = snapshot.Revision + 1
					restart = true
					break
				}
				if ctx.Err() != nil {
					cancel()
					return
				}
				if errors.Is(watchContext.Err(), context.DeadlineExceeded) {
					restart = true
					break
				}
				watchErr := response.Err()
				if watchErr == nil {
					watchErr = errors.New("etcd watch canceled")
				}
				sendWatchError(ctx, errorsChannel, dcs.NewError(dcs.ErrorTransport, "watch", prefix, watchErr))
				return
			}
			if response.Header.Revision >= revision {
				revision = response.Header.Revision + 1
			}
			for _, event := range response.Events {
				kind := dcs.WatchPut
				if event.Type == clientv3.EventTypeDelete {
					kind = dcs.WatchDelete
				}
				entry := entryFromKV(strings.TrimSuffix(prefix, "/"), event.Kv)
				if kind == dcs.WatchDelete && event.PrevKv != nil {
					previous := entryFromKV(strings.TrimSuffix(prefix, "/"), event.PrevKv)
					entry.Value = previous.Value
					entry.Kind = previous.Kind
				}
				if !sendWatchEvent(ctx, events, dcs.WatchEvent{
					Type: kind, Revision: response.Header.Revision, Entry: &entry, At: time.Now().UTC(),
				}) {
					return
				}
			}
		}
		watchContextErr := watchContext.Err()
		cancel()
		if restart {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(watchContextErr, context.DeadlineExceeded) {
			continue
		}
		sendWatchError(ctx, errorsChannel, dcs.NewError(dcs.ErrorTransport, "watch", prefix, errors.New("watch stream closed")))
		return
	}
}

func sendWatchEvent(ctx context.Context, output chan<- dcs.WatchEvent, event dcs.WatchEvent) bool {
	select {
	case output <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func sendWatchError(ctx context.Context, output chan<- error, err error) {
	select {
	case output <- err:
	case <-ctx.Done():
	}
}
