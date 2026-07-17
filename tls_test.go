package patroni_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/youmark/pkcs8"
)

type testCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	pem         []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "go-patroni test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{certificate: certificate, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func issueTestCertificate(t *testing.T, ca testCA, serial int64, name string, usage x509.ExtKeyUsage) ([]byte, []byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	if usage == x509.ExtKeyUsageServerAuth {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		template.DNSNames = []string{"localhost"}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), key
}

func encryptedKeyPEM(t *testing.T, key *ecdsa.PrivateKey, password string) []byte {
	t.Helper()
	der, err := pkcs8.MarshalPrivateKey(key, []byte(password), nil)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: der})
}

func writeTLSFile(t *testing.T, directory, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTLSMutualAuthenticationEncryptedKeyAndRotationCache(t *testing.T) {
	const password = "__BOAR_TEST_ONLY_TLS_KEY_PASSWORD__"
	ca := newTestCA(t)
	serverCertPEM, serverKeyPEM, _ := issueTestCertificate(t, ca, 2, "server", x509.ExtKeyUsageServerAuth)
	serverPair, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientRoots := x509.NewCertPool()
	clientRoots.AppendCertsFromPEM(ca.pem)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
			t.Error("client certificate was not authenticated")
		}
		_, _ = io.WriteString(writer, `{"state":"running","patroni":{"version":"4.1.0","scope":"demo","name":"node-1"}}`)
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverPair},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	defer server.Close()

	directory := t.TempDir()
	caPath := writeTLSFile(t, directory, "ca.pem", ca.pem)
	clientCert, _, clientKey := issueTestCertificate(t, ca, 3, "client-one", x509.ExtKeyUsageClientAuth)
	certPath := writeTLSFile(t, directory, "client.pem", clientCert)
	keyPath := writeTLSFile(t, directory, "client-key.pem", encryptedKeyPEM(t, clientKey, password))
	options := (patroni.TLSOptions{CAFile: caPath, CertFile: certPath, KeyFile: keyPath}).WithKeyPassword(password)
	for _, output := range []string{fmt.Sprint(options), fmt.Sprintf("%#v", options)} {
		if strings.Contains(output, password) {
			t.Fatal("TLS options formatter leaked key password")
		}
	}

	cache := patroni.NewTransportCache()
	defer cache.CloseIdleConnections()
	first, err := cache.Transport(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	again, err := cache.Transport(context.Background(), options)
	if err != nil || first != again {
		t.Fatalf("unchanged credential fingerprint did not reuse transport: same=%t err=%v", first == again, err)
	}
	client, _ := patroni.NewClient(patroni.ClientOptions{Transport: first})
	response, err := client.GetPatroni(context.Background(), server.URL)
	if err != nil || response.Data.State != "running" {
		t.Fatalf("mTLS request failed: response=%#v err=%v", response, err)
	}

	rotatedCert, _, rotatedKey := issueTestCertificate(t, ca, 4, "client-two", x509.ExtKeyUsageClientAuth)
	if err := os.WriteFile(certPath, rotatedCert, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, encryptedKeyPEM(t, rotatedKey, password), 0o600); err != nil {
		t.Fatal(err)
	}
	rotated, err := cache.Transport(context.Background(), options)
	if err != nil || rotated == first {
		t.Fatalf("rotated credential content did not create a new transport: same=%t err=%v", rotated == first, err)
	}
	rotatedClient, _ := patroni.NewClient(patroni.ClientOptions{Transport: rotated})
	if _, err := rotatedClient.GetPatroni(context.Background(), server.URL); err != nil {
		t.Fatalf("rotated mTLS credential failed: %v", err)
	}
}

func TestTLSVerificationDefaultsAndExplicitInsecureMode(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, `{"patroni":{"version":"4.1.0","scope":"demo","name":"node-1"}}`)
	}))
	defer server.Close()

	verifiedTransport, err := patroni.NewHTTPTransport(context.Background(), patroni.TLSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	verifiedClient, _ := patroni.NewClient(patroni.ClientOptions{Transport: verifiedTransport})
	if _, err := verifiedClient.GetPatroni(context.Background(), server.URL); err == nil {
		t.Fatal("self-signed server unexpectedly passed default TLS verification")
	}

	insecureOptions := patroni.TLSOptions{InsecureSkipVerify: true}
	if !strings.Contains(insecureOptions.String(), "insecure:true") {
		t.Fatal("explicit insecure mode is not observable")
	}
	insecureTransport, err := patroni.NewHTTPTransport(context.Background(), insecureOptions)
	if err != nil {
		t.Fatal(err)
	}
	insecureClient, _ := patroni.NewClient(patroni.ClientOptions{Transport: insecureTransport})
	if response, err := insecureClient.GetPatroni(context.Background(), server.URL); err != nil || response.StatusCode != 200 {
		t.Fatalf("explicit insecure test connection failed: response=%#v err=%v", response, err)
	}
}

func TestTLSConfigurationErrorsAreTypedSecretSafeAndCancellable(t *testing.T) {
	const password = "__BOAR_TEST_ONLY_WRONG_TLS_PASSWORD__"
	directory := t.TempDir()
	keyPath := writeTLSFile(t, directory, "key.pem", []byte("not a private key"))
	certPath := writeTLSFile(t, directory, "cert.pem", []byte("not a certificate"))
	_, err := patroni.NewHTTPTransport(context.Background(), (patroni.TLSOptions{CertFile: certPath, KeyFile: keyPath}).WithKeyPassword(password))
	var tlsErr *patroni.TLSConfigError
	if !errors.As(err, &tlsErr) || strings.Contains(err.Error(), password) || strings.Contains(fmt.Sprintf("%#v", err), password) {
		t.Fatalf("TLS key error was untyped or unsafe")
	}

	_, err = patroni.NewHTTPTransport(context.Background(), patroni.TLSOptions{CertFile: certPath})
	if !errors.As(err, &tlsErr) || tlsErr.Field != "cert/key" {
		t.Fatalf("incomplete certificate pair error mismatch: %#v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = patroni.NewHTTPTransport(cancelled, patroni.TLSOptions{CAFile: certPath})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled credential read returned %v", err)
	}
}
