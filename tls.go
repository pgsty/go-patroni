package patroni

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/youmark/pkcs8"
)

type TLSOptions struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
	// IncludeSystemCAs augments an explicit CAFile with the host root pool.
	// By default an explicit CAFile is the exclusive trust bundle, matching
	// Patroni ctl.cacert/restapi.cafile behavior.
	IncludeSystemCAs bool

	keyPassword []byte
}

// WithKeyPassword returns a copy with a protected key passphrase. Formatting
// TLSOptions never exposes the passphrase; only transport construction reads
// it, and temporary copies are cleared after parsing.
func (options TLSOptions) WithKeyPassword(password string) TLSOptions {
	options.keyPassword = append([]byte(nil), password...)
	return options
}

func (options TLSOptions) String() string {
	return fmt.Sprintf("patroni.TLSOptions{ca:%t,cert:%t,key:%t,keyPassword:%t,serverName:%q,insecure:%t,includeSystemCAs:%t}",
		options.CAFile != "", options.CertFile != "", options.KeyFile != "", len(options.keyPassword) > 0,
		options.ServerName, options.InsecureSkipVerify, options.IncludeSystemCAs)
}

func (options TLSOptions) GoString() string { return options.String() }

type TLSConfigError struct {
	Field string
	cause error
}

func (err *TLSConfigError) Error() string {
	if err == nil {
		return ""
	}
	return "patroni TLS configuration field " + err.Field + " is invalid"
}

func (err *TLSConfigError) GoString() string { return err.Error() }

func (err *TLSConfigError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

type tlsMaterial struct {
	options TLSOptions
	ca      []byte
	cert    []byte
	key     []byte
	keyPass []byte
}

func loadTLSMaterial(ctx context.Context, options TLSOptions) (tlsMaterial, error) {
	material := tlsMaterial{options: options, keyPass: append([]byte(nil), options.keyPassword...)}
	if ctx == nil {
		return material, &TLSConfigError{Field: "context", cause: errors.New("context is nil")}
	}
	if (options.CertFile == "") != (options.KeyFile == "") {
		return material, &TLSConfigError{Field: "cert/key", cause: errors.New("certificate and key must be configured together")}
	}
	var err error
	if options.CAFile != "" {
		material.ca, err = readCredentialFile(ctx, options.CAFile)
		if err != nil {
			return material, &TLSConfigError{Field: "cacert", cause: err}
		}
	}
	if options.CertFile != "" {
		material.cert, err = readCredentialFile(ctx, options.CertFile)
		if err != nil {
			return material, &TLSConfigError{Field: "certfile", cause: err}
		}
		material.key, err = readCredentialFile(ctx, options.KeyFile)
		if err != nil {
			return material, &TLSConfigError{Field: "keyfile", cause: err}
		}
	}
	return material, nil
}

func readCredentialFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		clearBytes(data)
		return nil, err
	}
	return data, nil
}

func (material *tlsMaterial) clear() {
	clearBytes(material.key)
	clearBytes(material.keyPass)
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func buildHTTPTransport(material tlsMaterial) (*http.Transport, error) {
	configuration := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: material.options.ServerName,
		// This value is deliberately explicit and remains observable through
		// TLSOptions.String/config warnings; verification is on by default.
		InsecureSkipVerify: material.options.InsecureSkipVerify,
	}
	if len(material.ca) > 0 {
		roots := x509.NewCertPool()
		if material.options.IncludeSystemCAs {
			var err error
			roots, err = x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
		}
		if !roots.AppendCertsFromPEM(material.ca) {
			return nil, &TLSConfigError{Field: "cacert", cause: errors.New("no CA certificate found")}
		}
		configuration.RootCAs = roots
	}
	if len(material.cert) > 0 {
		keyPEM, err := decryptPrivateKey(material.key, material.keyPass)
		if err != nil {
			return nil, &TLSConfigError{Field: "keyfile_password", cause: err}
		}
		decryptedCopy := !sameBytes(keyPEM, material.key)
		if decryptedCopy {
			defer clearBytes(keyPEM)
		}
		certificate, err := tls.X509KeyPair(material.cert, keyPEM)
		if err != nil {
			return nil, &TLSConfigError{Field: "cert/key", cause: err}
		}
		configuration.Certificates = []tls.Certificate{certificate}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = configuration
	return transport, nil
}

func sameBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func decryptPrivateKey(keyPEM, password []byte) ([]byte, error) {
	if len(password) == 0 {
		return keyPEM, nil
	}
	rest := keyPEM
	for len(rest) > 0 {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = remaining
		if !isPrivateKeyBlock(block.Type) {
			continue
		}
		if x509.IsEncryptedPEMBlock(block) { //nolint:staticcheck
			decrypted, err := x509.DecryptPEMBlock(block, password) //nolint:staticcheck
			if err != nil {
				return nil, err
			}
			return pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: decrypted}), nil
		}
		if block.Type == "ENCRYPTED PRIVATE KEY" {
			privateKey, err := pkcs8.ParsePKCS8PrivateKey(block.Bytes, password)
			if err != nil {
				return nil, err
			}
			decrypted, err := x509.MarshalPKCS8PrivateKey(privateKey)
			if err != nil {
				return nil, err
			}
			return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: decrypted}), nil
		}
		return keyPEM, nil
	}
	return nil, errors.New("private key PEM block not found")
}

func isPrivateKeyBlock(blockType string) bool {
	return blockType == "PRIVATE KEY" || blockType == "ENCRYPTED PRIVATE KEY" ||
		blockType == "RSA PRIVATE KEY" || blockType == "EC PRIVATE KEY"
}

// NewHTTPTransport performs cancellation-aware credential file reads and
// builds a verification-on TLS transport. It retains parsed key material only
// inside crypto/tls, not plaintext file or passphrase buffers.
func NewHTTPTransport(ctx context.Context, options TLSOptions) (*http.Transport, error) {
	material, err := loadTLSMaterial(ctx, options)
	defer material.clear()
	if err != nil {
		return nil, err
	}
	return buildHTTPTransport(material)
}

const defaultTransportCacheEntries = 8

type TransportCacheOptions struct {
	// MaxEntries bounds retained certificate fingerprints. Zero uses the
	// default; negative values are invalid.
	MaxEntries int
}

// TransportCache is an instance-scoped, rotation-aware LRU transport cache.
// The fingerprint includes file contents and TLS settings, so replacing a
// certificate/key creates a new pool without process-global mutable state.
type TransportCache struct {
	mutex      sync.Mutex
	maxEntries int
	transports map[[sha256.Size]byte]*http.Transport
	order      [][sha256.Size]byte
}

func NewTransportCache() *TransportCache {
	cache, _ := NewTransportCacheWithOptions(TransportCacheOptions{})
	return cache
}

func NewTransportCacheWithOptions(options TransportCacheOptions) (*TransportCache, error) {
	if options.MaxEntries < 0 {
		return nil, &TLSConfigError{Field: "cache.max_entries", cause: errors.New("maximum entries must not be negative")}
	}
	maximum := options.MaxEntries
	if maximum == 0 {
		maximum = defaultTransportCacheEntries
	}
	return &TransportCache{
		maxEntries: maximum,
		transports: make(map[[sha256.Size]byte]*http.Transport, maximum),
		order:      make([][sha256.Size]byte, 0, maximum),
	}, nil
}

func (cache *TransportCache) Transport(ctx context.Context, options TLSOptions) (*http.Transport, error) {
	if cache == nil {
		return nil, &TLSConfigError{Field: "cache", cause: errors.New("cache is nil")}
	}
	material, err := loadTLSMaterial(ctx, options)
	defer material.clear()
	if err != nil {
		return nil, err
	}
	fingerprint := material.fingerprint()
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	cache.initialize()
	if transport := cache.transports[fingerprint]; transport != nil {
		cache.touch(fingerprint)
		return transport, nil
	}
	transport, err := buildHTTPTransport(material)
	if err != nil {
		return nil, err
	}
	cache.transports[fingerprint] = transport
	cache.order = append(cache.order, fingerprint)
	cache.evict()
	return transport, nil
}

func (cache *TransportCache) initialize() {
	if cache.maxEntries <= 0 {
		cache.maxEntries = defaultTransportCacheEntries
	}
	if cache.transports == nil {
		cache.transports = make(map[[sha256.Size]byte]*http.Transport, cache.maxEntries)
	}
}

func (cache *TransportCache) touch(fingerprint [sha256.Size]byte) {
	for index, candidate := range cache.order {
		if candidate != fingerprint {
			continue
		}
		copy(cache.order[index:], cache.order[index+1:])
		cache.order[len(cache.order)-1] = fingerprint
		return
	}
	cache.order = append(cache.order, fingerprint)
}

func (cache *TransportCache) evict() {
	for len(cache.order) > cache.maxEntries {
		fingerprint := cache.order[0]
		cache.order = cache.order[1:]
		transport := cache.transports[fingerprint]
		delete(cache.transports, fingerprint)
		if transport != nil {
			transport.CloseIdleConnections()
		}
	}
}

func (material tlsMaterial) fingerprint() [sha256.Size]byte {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%q\x00%q\x00%q\x00%q\x00%t\x00%t\x00", material.options.CAFile,
		material.options.CertFile, material.options.KeyFile, material.options.ServerName,
		material.options.InsecureSkipVerify, material.options.IncludeSystemCAs)
	for _, value := range [][]byte{material.ca, material.cert, material.key, material.keyPass} {
		_, _ = fmt.Fprintf(hash, "%d\x00", len(value))
		_, _ = hash.Write(value)
	}
	var output [sha256.Size]byte
	copy(output[:], hash.Sum(nil))
	return output
}

func (cache *TransportCache) CloseIdleConnections() {
	if cache == nil {
		return
	}
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	for _, transport := range cache.transports {
		transport.CloseIdleConnections()
	}
}

// Purge closes idle connections and forgets every cached fingerprint. Active
// connections remain valid according to net/http transport semantics.
func (cache *TransportCache) Purge() {
	if cache == nil {
		return
	}
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	for _, transport := range cache.transports {
		transport.CloseIdleConnections()
	}
	cache.transports = make(map[[sha256.Size]byte]*http.Transport, cache.maxEntries)
	cache.order = cache.order[:0]
}
