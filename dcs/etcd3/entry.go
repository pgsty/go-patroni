package etcd3

import (
	"strings"

	"github.com/pgsty/go-patroni/dcs"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func entryFromKV(prefix string, value *mvccpb.KeyValue) dcs.Entry {
	if value == nil {
		return dcs.Entry{}
	}
	key := string(value.Key)
	relative := ""
	if prefix != "" {
		relative = strings.TrimPrefix(strings.TrimPrefix(key, strings.TrimRight(prefix, "/")), "/")
	}
	return dcs.Entry{
		Key: key, RelativePath: relative, Kind: dcs.ClassifyRelativePath(relative),
		CreateRevision: value.CreateRevision, ModRevision: value.ModRevision, Version: value.Version,
		Lease: value.Lease, Value: append([]byte(nil), value.Value...),
	}
}

func entriesFromKVs(prefix string, values []*mvccpb.KeyValue) []dcs.Entry {
	entries := make([]dcs.Entry, 0, len(values))
	for _, value := range values {
		entries = append(entries, entryFromKV(prefix, value))
	}
	dcs.SortEntries(entries)
	return entries
}
