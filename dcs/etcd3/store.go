// Package etcd3 is the only BOAR DCS backend. It maps etcd v3 revisions,
// transactions, and watches into Patroni-oriented dcs contracts.
package etcd3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultDialTimeout      = 5 * time.Second
	defaultRequestTimeout   = 10 * time.Second
	defaultMaxDiscoveryKeys = int64(100_000)
)

type Options struct {
	Endpoints        []string
	TLS              *tls.Config
	Username         string
	DialTimeout      time.Duration
	RequestTimeout   time.Duration
	MaxDiscoveryKeys int64

	password string
}

func (options Options) WithPassword(password string) Options {
	options.password = password
	return options
}

func (options Options) String() string {
	return fmt.Sprintf("etcd3.Options{endpoints:%d,tls:%t,username:%t,password:%t,dialTimeout:%s,requestTimeout:%s,maxDiscoveryKeys:%d}",
		len(options.Endpoints), options.TLS != nil, options.Username != "", options.password != "",
		options.DialTimeout, options.RequestTimeout, options.MaxDiscoveryKeys)
}

func (options Options) GoString() string { return options.String() }

type Store struct {
	client           *clientv3.Client
	requestTimeout   time.Duration
	maxDiscoveryKeys int64
}

var _ dcs.Store = (*Store)(nil)

func New(ctx context.Context, options Options) (*Store, error) {
	if ctx == nil {
		return nil, dcs.NewError(dcs.ErrorConfiguration, "connect", "", errors.New("context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(options.Endpoints) == 0 {
		return nil, dcs.NewError(dcs.ErrorConfiguration, "connect", "endpoints", errors.New("no endpoints"))
	}
	for _, endpoint := range options.Endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return nil, dcs.NewError(dcs.ErrorConfiguration, "connect", "endpoints", err)
		}
	}
	dialTimeout := options.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	configuration := clientv3.Config{
		Endpoints: append([]string(nil), options.Endpoints...), DialTimeout: dialTimeout,
		Username: options.Username, Password: options.password, Context: ctx, Logger: zap.NewNop(),
	}
	if options.TLS != nil {
		configuration.TLS = options.TLS.Clone()
	}
	client, err := clientv3.New(configuration)
	if err != nil {
		return nil, dcs.NewError(contextErrorKind(err), "connect", "", err)
	}
	return NewFromClient(client, options), nil
}

func validateEndpoint(endpoint string) error {
	if strings.Contains(endpoint, "@") {
		return errors.New("endpoint userinfo is not accepted")
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Scheme != "http" && parsed.Scheme != "https" {
			return errors.New("endpoint must be an http(s) URL without userinfo")
		}
	}
	return nil
}

// NewFromClient supports embedding and deterministic transport substitution.
// Ownership of client transfers to Store and Close closes it.
func NewFromClient(client *clientv3.Client, options Options) *Store {
	requestTimeout := options.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultRequestTimeout
	}
	maximum := options.MaxDiscoveryKeys
	if maximum <= 0 {
		maximum = defaultMaxDiscoveryKeys
	}
	return &Store{client: client, requestTimeout: requestTimeout, maxDiscoveryKeys: maximum}
}

func (store *Store) String() string {
	if store == nil {
		return "etcd3.Store<nil>"
	}
	return fmt.Sprintf("etcd3.Store{requestTimeout:%s,maxDiscoveryKeys:%d}", store.requestTimeout, store.maxDiscoveryKeys)
}

func (store *Store) GoString() string { return store.String() }

func (store *Store) Close() error {
	if store == nil || store.client == nil {
		return nil
	}
	return store.client.Close()
}

func (store *Store) operationContext(ctx context.Context, operation, key string) (context.Context, context.CancelFunc, error) {
	if store == nil || store.client == nil {
		return nil, nil, dcs.NewError(dcs.ErrorConfiguration, operation, key, errors.New("store is nil"))
	}
	if ctx == nil {
		return nil, nil, dcs.NewError(dcs.ErrorConfiguration, operation, key, errors.New("context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, dcs.NewError(contextErrorKind(err), operation, key, err)
	}
	child, cancel := context.WithTimeout(ctx, store.requestTimeout)
	return child, cancel, nil
}

func contextErrorKind(err error) dcs.ErrorKind {
	if errors.Is(err, context.Canceled) {
		return dcs.ErrorCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return dcs.ErrorDeadline
	}
	normalized := rpctypes.Error(err)
	if errors.Is(normalized, rpctypes.ErrAuthFailed) || errors.Is(normalized, rpctypes.ErrPermissionDenied) ||
		errors.Is(normalized, rpctypes.ErrInvalidAuthToken) {
		return dcs.ErrorAuthentication
	}
	var etcdError rpctypes.EtcdError
	if errors.As(normalized, &etcdError) {
		switch etcdError.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			return dcs.ErrorAuthentication
		}
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return dcs.ErrorAuthentication
	}
	return dcs.ErrorTransport
}

func readError(operation, key string, err error) error {
	return dcs.NewError(contextErrorKind(err), operation, key, err)
}

func writeNotSentError(operation, key string, err error) error {
	kind := dcs.ErrorConfiguration
	var typed *dcs.Error
	if errors.As(err, &typed) {
		kind = typed.Kind
	}
	return dcs.NewWriteError(kind, operation, key, dcs.DeliveryNotSent, err)
}

func writeMaybeSentError(operation, key string, err error) error {
	return dcs.NewWriteError(contextErrorKind(err), operation, key, dcs.DeliveryMaybeSent, err)
}

func (store *Store) Snapshot(ctx context.Context, target model.Target) (dcs.Snapshot, error) {
	prefix, err := dcs.ClusterPrefix(target)
	if err != nil {
		return dcs.Snapshot{}, dcs.NewError(dcs.ErrorConfiguration, "snapshot", "", err)
	}
	keyPrefix := prefix + "/"
	operationContext, cancel, err := store.operationContext(ctx, "snapshot", keyPrefix)
	if err != nil {
		return dcs.Snapshot{}, err
	}
	defer cancel()
	response, err := store.client.Get(operationContext, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return dcs.Snapshot{}, readError("snapshot", keyPrefix, err)
	}
	entries := entriesFromKVs(prefix, response.Kvs)
	return dcs.BuildSnapshot(target, prefix, response.Header.Revision, entries), nil
}

func (store *Store) Discover(ctx context.Context, request dcs.DiscoveryRequest) ([]dcs.DiscoveredCluster, error) {
	prefix := dcs.NamespacePrefix(request.Namespace)
	operationContext, cancel, err := store.operationContext(ctx, "discover", prefix)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := store.client.Get(operationContext, prefix, clientv3.WithPrefix(), clientv3.WithLimit(store.maxDiscoveryKeys+1))
	if err != nil {
		return nil, readError("discover", prefix, err)
	}
	if response.More || int64(len(response.Kvs)) > store.maxDiscoveryKeys {
		return nil, dcs.NewError(dcs.ErrorLimit, "discover", prefix,
			fmt.Errorf("namespace contains more than %d keys", store.maxDiscoveryKeys))
	}
	entries := entriesFromKVs("", response.Kvs)
	return dcs.DiscoverFromEntries(request, response.Header.Revision, entries), nil
}
