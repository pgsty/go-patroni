package etcd3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOptionsFormattingAndEndpointValidationAreSecretSafe(t *testing.T) {
	const password = "__BOAR_TEST_ONLY_ETCD3_OPTIONS_PASSWORD__"
	options := (Options{
		Endpoints: []string{"https://etcd.example.invalid:2379"},
		Username:  "operator",
	}).WithPassword(password)
	for _, rendered := range []string{options.String(), fmt.Sprintf("%#v", options)} {
		if strings.Contains(rendered, password) || strings.Contains(rendered, "etcd.example.invalid") {
			t.Fatalf("options formatting leaked endpoint or password: %s", rendered)
		}
	}

	tests := []Options{
		{},
		{Endpoints: []string{"http://user:password@etcd.example.invalid:2379"}},
		{Endpoints: []string{"ftp://etcd.example.invalid:2379"}},
	}
	for _, invalid := range tests {
		store, err := New(context.Background(), invalid)
		if store != nil || err == nil {
			t.Fatalf("invalid options accepted: options=%#v store=%#v err=%v", invalid, store, err)
		}
		if strings.Contains(err.Error(), "password") || strings.Contains(fmt.Sprintf("%#v", err), "password") {
			t.Fatal("configuration error leaked endpoint userinfo")
		}
	}
}

func TestPreflightCancellationIsNotSentAndPreciselyClassified(t *testing.T) {
	store, err := New(context.Background(), Options{Endpoints: []string{"http://127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.CompareAndSwapConfig(canceled, model.Target{Scope: "alpha"}, []byte(`{}`), nil)
	var typed *dcs.Error
	if !errors.As(err, &typed) || typed.Kind != dcs.ErrorCanceled || typed.Delivery != dcs.DeliveryNotSent || typed.AmbiguousWrite() {
		t.Fatalf("preflight cancellation classification mismatch: %#v", err)
	}

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = store.Snapshot(expired, model.Target{Scope: "alpha"})
	if !errors.As(err, &typed) || typed.Kind != dcs.ErrorDeadline || typed.Delivery != dcs.DeliveryUnknown {
		t.Fatalf("preflight deadline classification mismatch: %#v", err)
	}
}

func TestAuthenticatedConstructionHonorsCallerDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("close listener: %v", closeErr)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		<-ctx.Done()
		_ = connection.Close()
	}()

	started := time.Now()
	store, err := New(ctx, (Options{
		Endpoints: []string{"http://" + listener.Addr().String()}, Username: "operator", DialTimeout: 5 * time.Second,
	}).WithPassword("test-only-constructor-password"))
	if store != nil {
		_ = store.Close()
		t.Fatal("authenticated etcd construction unexpectedly succeeded against a non-gRPC listener")
	}
	var typed *dcs.Error
	if !errors.As(err, &typed) || typed.Kind != dcs.ErrorDeadline || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("authenticated constructor deadline classification mismatch: %#v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("authenticated constructor ignored caller deadline for %s", elapsed)
	}
}

func TestGRPCAuthenticationFailuresAreTypedAndSecretSafe(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "grpc unauthenticated", err: status.Error(codes.Unauthenticated, "test-only upstream credential detail")},
		{name: "grpc permission denied", err: status.Error(codes.PermissionDenied, "test-only upstream credential detail")},
		{name: "etcd authentication failed", err: rpctypes.ErrAuthFailed},
		{name: "etcd invalid auth token", err: rpctypes.ErrInvalidAuthToken},
		{name: "etcd permission denied", err: rpctypes.ErrPermissionDenied},
	}
	for _, test := range tests {
		const detail = "test-only upstream credential detail"
		err := readError("discover", "/pg/", test.err)
		var typed *dcs.Error
		if !errors.As(err, &typed) || typed.Kind != dcs.ErrorAuthentication {
			t.Fatalf("%s classification mismatch: %#v", test.name, err)
		}
		if strings.Contains(err.Error(), detail) || strings.Contains(fmt.Sprintf("%#v", err), detail) {
			t.Fatalf("%s detail leaked through typed error: %v", test.name, err)
		}
	}
}

func TestRequestTimeoutClampsLongerCallerDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("close listener: %v", closeErr)
		}
	})
	stop := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		<-stop
		_ = connection.Close()
	}()
	defer close(stop)

	store, err := New(context.Background(), Options{
		Endpoints: []string{"http://" + listener.Addr().String()}, DialTimeout: 5 * time.Second, RequestTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	caller, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	_, err = store.Snapshot(caller, model.Target{Scope: "alpha"})
	var typed *dcs.Error
	if !errors.As(err, &typed) || typed.Kind != dcs.ErrorDeadline || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DCS request timeout classification mismatch: %#v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("DCS request timeout did not clamp longer caller deadline: %s", elapsed)
	}
}

func TestNilStoreAndNilContextFailWithoutPanics(t *testing.T) {
	var store *Store
	if _, err := store.Snapshot(context.Background(), model.Target{Scope: "alpha"}); err == nil {
		t.Fatal("nil store snapshot succeeded")
	}
	if _, err := store.CompareAndSwapConfig(context.Background(), model.Target{Scope: "alpha"}, nil, nil); err == nil {
		t.Fatal("nil store write succeeded")
	}
	store, err := New(context.Background(), Options{Endpoints: []string{"http://127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	if _, err := store.Discover(nil, dcs.DiscoveryRequest{}); err == nil { //nolint:staticcheck // nil-context contract test
		t.Fatal("nil context discovery succeeded")
	}

	stream := store.Watch(nil, model.Target{Scope: "alpha"}, 0) //nolint:staticcheck // nil-context contract test
	if err := <-stream.Errors; err == nil {
		t.Fatal("nil context watch did not report an error")
	}
	if _, ok := <-stream.Events; ok {
		t.Fatal("nil context watch event channel remained open")
	}
}
