package etcd3

import (
	"context"
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (store *Store) CompareAndSwapConfig(
	ctx context.Context,
	target model.Target,
	value []byte,
	expectedModRevision *int64,
) (dcs.WriteResult, error) {
	key, err := dcs.KeyPath(target, "config")
	if err != nil {
		return dcs.WriteResult{}, dcs.NewWriteError(dcs.ErrorConfiguration, "config-cas", "", dcs.DeliveryNotSent, err)
	}
	return store.put(ctx, "config-cas", key, value, expectedModRevision)
}

func (store *Store) WriteFailover(
	ctx context.Context,
	target model.Target,
	value []byte,
	expectedModRevision *int64,
) (dcs.WriteResult, error) {
	key, err := dcs.KeyPath(target, "failover")
	if err != nil {
		return dcs.WriteResult{}, dcs.NewWriteError(dcs.ErrorConfiguration, "failover-write", "", dcs.DeliveryNotSent, err)
	}
	return store.put(ctx, "failover-write", key, value, expectedModRevision)
}

func (store *Store) put(
	ctx context.Context,
	operation string,
	key string,
	value []byte,
	expectedModRevision *int64,
) (dcs.WriteResult, error) {
	operationContext, cancel, err := store.operationContext(ctx, operation, key)
	if err != nil {
		return dcs.WriteResult{}, writeNotSentError(operation, key, err)
	}
	defer cancel()
	if expectedModRevision == nil {
		response, err := store.client.Put(operationContext, key, string(value), clientv3.WithPrevKV())
		if err != nil {
			return dcs.WriteResult{}, writeMaybeSentError(operation, key, err)
		}
		result := dcs.WriteResult{Applied: true, Revision: response.Header.Revision}
		if response.PrevKv != nil {
			previous := entryFromKV(clusterPrefixForKey(key), response.PrevKv)
			result.Previous = &previous
		}
		return result, nil
	}
	transaction, err := store.client.Txn(operationContext).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", *expectedModRevision)).
		Then(clientv3.OpPut(key, string(value), clientv3.WithPrevKV())).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return dcs.WriteResult{}, writeMaybeSentError(operation, key, err)
	}
	result := dcs.WriteResult{Applied: transaction.Succeeded, Revision: transaction.Header.Revision}
	if transaction.Succeeded {
		if len(transaction.Responses) > 0 {
			if put := transaction.Responses[0].GetResponsePut(); put != nil && put.PrevKv != nil {
				previous := entryFromKV(clusterPrefixForKey(key), put.PrevKv)
				result.Previous = &previous
			}
		}
		return result, nil
	}
	observed := int64(0)
	if len(transaction.Responses) > 0 {
		if get := transaction.Responses[0].GetResponseRange(); get != nil && len(get.Kvs) > 0 {
			current := entryFromKV(clusterPrefixForKey(key), get.Kvs[0])
			observed = current.ModRevision
			result.Current = &current
		}
	}
	return result, &dcs.ConflictError{Key: key, ExpectedRevision: *expectedModRevision, ObservedRevision: observed}
}

func (store *Store) DeleteFailover(
	ctx context.Context,
	target model.Target,
	expectedModRevision *int64,
) (dcs.WriteResult, error) {
	key, err := dcs.KeyPath(target, "failover")
	if err != nil {
		return dcs.WriteResult{}, dcs.NewWriteError(dcs.ErrorConfiguration, "failover-delete", "", dcs.DeliveryNotSent, err)
	}
	operationContext, cancel, err := store.operationContext(ctx, "failover-delete", key)
	if err != nil {
		return dcs.WriteResult{}, writeNotSentError("failover-delete", key, err)
	}
	defer cancel()
	if expectedModRevision == nil {
		response, err := store.client.Delete(operationContext, key, clientv3.WithPrevKV())
		if err != nil {
			return dcs.WriteResult{}, writeMaybeSentError("failover-delete", key, err)
		}
		result := dcs.WriteResult{Applied: response.Deleted > 0, Revision: response.Header.Revision}
		if len(response.PrevKvs) > 0 {
			previous := entryFromKV(clusterPrefixForKey(key), response.PrevKvs[0])
			result.Previous = &previous
		}
		return result, nil
	}
	transaction, err := store.client.Txn(operationContext).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", *expectedModRevision)).
		Then(clientv3.OpDelete(key, clientv3.WithPrevKV())).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return dcs.WriteResult{}, writeMaybeSentError("failover-delete", key, err)
	}
	result := dcs.WriteResult{Applied: transaction.Succeeded, Revision: transaction.Header.Revision}
	if transaction.Succeeded {
		if len(transaction.Responses) > 0 {
			if deleted := transaction.Responses[0].GetResponseDeleteRange(); deleted != nil && len(deleted.PrevKvs) > 0 {
				previous := entryFromKV(clusterPrefixForKey(key), deleted.PrevKvs[0])
				result.Previous = &previous
			}
		}
		return result, nil
	}
	observed := int64(0)
	if len(transaction.Responses) > 0 {
		if get := transaction.Responses[0].GetResponseRange(); get != nil && len(get.Kvs) > 0 {
			current := entryFromKV(clusterPrefixForKey(key), get.Kvs[0])
			observed = current.ModRevision
			result.Current = &current
		}
	}
	return result, &dcs.ConflictError{Key: key, ExpectedRevision: *expectedModRevision, ObservedRevision: observed}
}

// DeleteCluster removes only the exact trailing-slash cluster prefix. A scope
// named "alpha" can therefore never delete sibling scope "alphabet".
func (store *Store) DeleteCluster(ctx context.Context, target model.Target) (dcs.RemoveResult, error) {
	prefix, err := dcs.ClusterPrefix(target)
	if err != nil {
		return dcs.RemoveResult{}, dcs.NewWriteError(dcs.ErrorConfiguration, "remove-cluster", "", dcs.DeliveryNotSent, err)
	}
	prefix += "/"
	operationContext, cancel, err := store.operationContext(ctx, "remove-cluster", prefix)
	if err != nil {
		return dcs.RemoveResult{}, writeNotSentError("remove-cluster", prefix, err)
	}
	defer cancel()
	response, err := store.client.Delete(operationContext, prefix, clientv3.WithPrefix())
	if err != nil {
		return dcs.RemoveResult{}, writeMaybeSentError("remove-cluster", prefix, err)
	}
	return dcs.RemoveResult{Deleted: response.Deleted, Revision: response.Header.Revision}, nil
}

func clusterPrefixForKey(key string) string {
	for _, suffix := range []string{"/config", "/failover"} {
		if strings.HasSuffix(key, suffix) {
			return strings.TrimSuffix(key, suffix)
		}
	}
	return ""
}
