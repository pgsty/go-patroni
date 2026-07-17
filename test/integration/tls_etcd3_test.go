//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/dcs/etcd3"
	"github.com/pgsty/go-patroni/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcd3TLSMutualAuthVerificationAndCredentials(t *testing.T) {
	if os.Getenv("GO_PATRONI_TEST_ETCD_TLS_ISOLATED") != "1" {
		t.Fatal("refusing etcd TLS integration test without GO_PATRONI_TEST_ETCD_TLS_ISOLATED=1")
	}
	endpoint := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_ENDPOINT")
	namespace := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_NAMESPACE")
	if !strings.HasPrefix(endpoint, "https://127.0.0.1:") {
		t.Fatalf("refusing etcd TLS integration test against non-loopback endpoint %q", endpoint)
	}
	if !isolatedNamespace.MatchString(namespace) {
		t.Fatalf("refusing etcd TLS integration test with unsafe namespace %q", namespace)
	}
	passwordFile := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_PASSWORD_FILE")
	password, err := os.ReadFile(passwordFile)
	if err != nil {
		t.Fatal("read isolated etcd password file")
	}
	passwordText := strings.TrimSpace(string(password))
	if passwordText == "" {
		t.Fatal("isolated etcd password file is empty")
	}
	if information, statErr := os.Stat(passwordFile); statErr != nil || information.Mode().Perm()&0o077 != 0 {
		t.Fatalf("isolated etcd password file must not be group/world accessible")
	}

	caFile := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_CA")
	certFile := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_CLIENT_CERT")
	keyFile := os.Getenv("GO_PATRONI_TEST_ETCD_TLS_CLIENT_KEY")
	verifiedTLS := etcdTLSConfig(t, patroni.TLSOptions{
		CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "127.0.0.1",
	})
	options := (etcd3.Options{
		Endpoints: []string{endpoint}, TLS: verifiedTLS, Username: "root",
		DialTimeout: 3 * time.Second, RequestTimeout: 3 * time.Second,
	}).WithPassword(passwordText)
	if strings.Contains(options.String(), passwordText) || strings.Contains(fmt.Sprintf("%#v", options), passwordText) {
		t.Fatal("etcd options formatting exposed the password")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, TLS: verifiedTLS.Clone(), Username: "root", Password: passwordText,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "/" + namespace + "/alpha"
	if _, err := raw.Put(ctx, prefix+"/config", `{"ttl":30}`); err != nil {
		t.Fatalf("seed isolated authenticated etcd: %v", safeEtcdTLSError(err, passwordText))
	}
	if _, err := raw.Put(ctx, prefix+"/leader", "node-a"); err != nil {
		t.Fatalf("seed isolated authenticated etcd leader: %v", safeEtcdTLSError(err, passwordText))
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = raw.Delete(cleanupContext, "/"+namespace+"/", clientv3.WithPrefix())
		_ = raw.Close()
	})

	store, err := etcd3.New(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot, err := store.Snapshot(ctx, model.Target{Context: "tls", Namespace: namespace, Scope: "alpha"})
	if err != nil || snapshot.Revision <= 0 || snapshot.Cluster.Leader == nil || snapshot.Cluster.Leader.Name != "node-a" {
		t.Fatalf("verified mTLS and authenticated etcd snapshot failed: snapshot=%#v err=%v", snapshot, safeEtcdTLSError(err, passwordText))
	}

	untrustedTLS := etcdTLSConfig(t, patroni.TLSOptions{
		CertFile: certFile, KeyFile: keyFile, ServerName: "127.0.0.1",
	})
	expectEtcdTLSFailure(t, namespace, (etcd3.Options{
		Endpoints: []string{endpoint}, TLS: untrustedTLS, Username: "root", RequestTimeout: 2 * time.Second,
	}).WithPassword(passwordText), passwordText, "untrusted CA", dcs.ErrorTransport, dcs.ErrorDeadline)

	wrongHostnameTLS := etcdTLSConfig(t, patroni.TLSOptions{
		CAFile: caFile, CertFile: certFile, KeyFile: keyFile,
	})
	wrongHostnameEndpoint := strings.Replace(endpoint, "127.0.0.1", "localhost", 1)
	expectEtcdTLSFailure(t, namespace, (etcd3.Options{
		Endpoints: []string{wrongHostnameEndpoint}, TLS: wrongHostnameTLS, Username: "root", RequestTimeout: 2 * time.Second,
	}).WithPassword(passwordText), passwordText, "hostname mismatch", dcs.ErrorTransport, dcs.ErrorDeadline)

	missingClientTLS := etcdTLSConfig(t, patroni.TLSOptions{CAFile: caFile, ServerName: "127.0.0.1"})
	expectEtcdTLSFailure(t, namespace, (etcd3.Options{
		Endpoints: []string{endpoint}, TLS: missingClientTLS, Username: "root", RequestTimeout: 2 * time.Second,
	}).WithPassword(passwordText), passwordText, "missing client certificate", dcs.ErrorTransport, dcs.ErrorDeadline)

	expectEtcdTLSFailure(t, namespace, etcd3.Options{
		Endpoints: []string{endpoint}, TLS: verifiedTLS, RequestTimeout: 2 * time.Second,
	}, passwordText, "missing etcd credentials", dcs.ErrorAuthentication)
}

func etcdTLSConfig(t *testing.T, options patroni.TLSOptions) *tls.Config {
	t.Helper()
	transport, err := patroni.NewHTTPTransport(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	return transport.TLSClientConfig.Clone()
}

func expectEtcdTLSFailure(
	t *testing.T,
	namespace string,
	options etcd3.Options,
	protected string,
	scenario string,
	expectedKinds ...dcs.ErrorKind,
) {
	t.Helper()
	if len(expectedKinds) == 0 {
		t.Fatal("expected at least one typed DCS error kind")
	}
	if options.DialTimeout <= 0 {
		options.DialTimeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store, err := etcd3.New(ctx, options)
	if err == nil {
		defer store.Close()
		_, err = store.Discover(ctx, dcs.DiscoveryRequest{Context: "tls", Namespace: namespace})
	}
	if err == nil {
		t.Fatalf("real etcd accepted %s", scenario)
	}
	var typed *dcs.Error
	if !errors.As(err, &typed) {
		t.Fatalf("real etcd %s error was not typed: %v", scenario, safeEtcdTLSError(err, protected))
	}
	accepted := false
	for _, kind := range expectedKinds {
		if typed.Kind == kind {
			accepted = true
			break
		}
	}
	if !accepted {
		t.Fatalf(
			"real etcd %s error kind %s was not one of %v: %v",
			scenario,
			typed.Kind,
			expectedKinds,
			safeEtcdTLSError(err, protected),
		)
	}
	if strings.Contains(err.Error(), protected) || strings.Contains(fmt.Sprintf("%#v", err), protected) {
		t.Fatalf("real etcd %s error exposed credentials", scenario)
	}
}

func safeEtcdTLSError(err error, protected string) error {
	if err == nil {
		return nil
	}
	return errors.New(strings.ReplaceAll(err.Error(), protected, "[REDACTED]"))
}
