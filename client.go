package patroni

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultTimeout      = 10 * time.Second
	defaultMaxBodyBytes = 8 << 20
)

type Authorizer interface {
	Authorize(context.Context, *http.Request) error
}

type BasicAuth struct {
	username string
	password string
}

func NewBasicAuth(username, password string) BasicAuth {
	return BasicAuth{username: username, password: password}
}

func (auth BasicAuth) Authorize(_ context.Context, request *http.Request) error {
	request.SetBasicAuth(auth.username, auth.password)
	return nil
}

func (auth BasicAuth) String() string {
	if auth.username == "" && auth.password == "" {
		return "patroni.BasicAuth{}"
	}
	return "patroni.BasicAuth{credentials:[REDACTED]}"
}

func (auth BasicAuth) GoString() string { return auth.String() }

type ClientOptions struct {
	HTTPClient       *http.Client
	Transport        http.RoundTripper
	Authorizer       Authorizer
	Logger           *slog.Logger
	Timeout          time.Duration
	MaxResponseBytes int64
	UserAgent        string
}

type Client struct {
	httpClient       *http.Client
	authorizer       Authorizer
	timeout          time.Duration
	maxResponseBytes int64
	userAgent        string
	logger           *slog.Logger
}

func NewClient(options ClientOptions) (*Client, error) {
	if options.HTTPClient != nil && options.Transport != nil {
		return nil, fmt.Errorf("patroni client: HTTPClient and Transport are mutually exclusive")
	}
	base := http.DefaultClient
	if options.HTTPClient != nil {
		base = options.HTTPClient
	}
	client := *base
	if options.Transport != nil {
		client.Transport = options.Transport
	}
	originalRedirect := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > 0 && !readMethod(via[0].Method) {
			return http.ErrUseLastResponse
		}
		if originalRedirect != nil {
			return originalRedirect(request, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	maximum := options.MaxResponseBytes
	if maximum <= 0 {
		maximum = defaultMaxBodyBytes
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Client{
		httpClient: &client, authorizer: options.Authorizer, timeout: timeout,
		maxResponseBytes: maximum, userAgent: options.UserAgent, logger: logger,
	}, nil
}

func (client *Client) String() string {
	if client == nil {
		return "patroni.Client<nil>"
	}
	return fmt.Sprintf("patroni.Client{timeout:%s,maxResponseBytes:%d,authorizer:%t}",
		client.timeout, client.maxResponseBytes, client.authorizer != nil)
}

func (client *Client) GoString() string { return client.String() }

func readMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

type wireResponse struct {
	status int
	header http.Header
	raw    []byte
}

func (client *Client) execute(
	ctx context.Context,
	method string,
	baseURL string,
	endpoint string,
	query url.Values,
	body []byte,
) (wire wireResponse, returnedError error) {
	started := time.Now()
	if client != nil && client.logger != nil {
		defer func() { client.logHTTPExchange(ctx, method, endpoint, started, wire, returnedError) }()
	}
	if client == nil {
		return wireResponse{}, newError(ErrorRequest, method, endpoint, DeliveryNotSent, 0, errors.New("client is nil"))
	}
	if ctx == nil {
		return wireResponse{}, newError(ErrorRequest, method, endpoint, DeliveryNotSent, 0, errors.New("context is nil"))
	}
	requestURL, err := joinEndpoint(baseURL, endpoint, query)
	if err != nil {
		return wireResponse{}, newError(ErrorRequest, method, endpoint, DeliveryNotSent, 0, err)
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(requestContext, method, requestURL, reader)
	if err != nil {
		return wireResponse{}, newError(ErrorRequest, method, endpoint, DeliveryNotSent, 0, err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if client.userAgent != "" {
		request.Header.Set("User-Agent", client.userAgent)
	}
	if client.authorizer != nil {
		if err := client.authorizer.Authorize(requestContext, request); err != nil {
			return wireResponse{}, newError(ErrorAuthentication, method, endpoint, DeliveryNotSent, 0, err)
		}
	}

	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) }}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	response, err := client.httpClient.Do(request)
	if err != nil {
		delivery := DeliveryNotSent
		if wrote.Load() {
			delivery = DeliveryMaybeSent
		}
		return wireResponse{}, newError(ErrorTransport, method, endpoint, delivery, 0, err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil && returnedError == nil {
			returnedError = newError(ErrorResponseBody, method, endpoint, DeliveryResponseReceived, response.StatusCode, closeErr)
		}
	}()

	raw, readErr := io.ReadAll(io.LimitReader(response.Body, client.maxResponseBytes+1))
	wire = wireResponse{status: response.StatusCode, header: response.Header.Clone(), raw: raw}
	if int64(len(raw)) > client.maxResponseBytes {
		wire.raw = raw[:client.maxResponseBytes]
		return wire, newError(ErrorResponseBody, method, endpoint, DeliveryResponseReceived, response.StatusCode,
			fmt.Errorf("response exceeds %d-byte limit", client.maxResponseBytes))
	}
	if readErr != nil {
		return wire, newError(ErrorResponseBody, method, endpoint, DeliveryResponseReceived, response.StatusCode, readErr)
	}
	return wire, nil
}

func (client *Client) logHTTPExchange(
	ctx context.Context,
	method string,
	endpoint string,
	started time.Time,
	wire wireResponse,
	err error,
) {
	if client == nil || client.logger == nil {
		return
	}
	errorKind := ErrorKind("")
	delivery := DeliveryResponseReceived
	var typed *Error
	if errors.As(err, &typed) {
		errorKind = typed.Kind
		delivery = typed.Delivery
	}
	attributes := []any{
		"method", method,
		"endpoint", endpoint,
		"status_code", wire.status,
		"delivery_state", delivery,
		"duration_ms", time.Since(started).Milliseconds(),
		"error_kind", errorKind,
	}
	if ctx == nil {
		client.logger.Debug("patroni http exchange", attributes...)
		return
	}
	client.logger.DebugContext(ctx, "patroni http exchange", attributes...)
}

func joinEndpoint(baseURL, endpoint string, query url.Values) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL")
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return "", fmt.Errorf("base URL scheme must be http or https")
	}
	if base.Host == "" {
		return "", fmt.Errorf("base URL host is required")
	}
	if base.User != nil {
		return "", fmt.Errorf("base URL userinfo is not accepted")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return "", fmt.Errorf("base URL query and fragment are not accepted")
	}
	joinedPath := strings.TrimRight(base.Path, "/") + "/" + strings.TrimLeft(endpoint, "/")
	if endpoint == "/" && !strings.HasSuffix(joinedPath, "/") {
		joinedPath += "/"
	}
	base.Path = joinedPath
	base.RawPath = ""
	if len(query) > 0 {
		base.RawQuery = query.Encode()
	}
	return base.String(), nil
}

func marshalRequest(method, endpoint string, value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, newError(ErrorRequest, method, endpoint, DeliveryNotSent, 0, err)
	}
	return body, nil
}

func jsonResponse[T any](wire wireResponse, method, endpoint string, decode bool) (Response[T], error) {
	response := Response[T]{StatusCode: wire.status, Header: wire.header, Raw: append([]byte(nil), wire.raw...)}
	if !decode {
		return response, nil
	}
	if len(wire.raw) == 0 {
		return response, newError(ErrorDecode, method, endpoint, DeliveryResponseReceived, wire.status, io.ErrUnexpectedEOF)
	}
	if err := decodeJSON(wire.raw, &response.Data); err != nil {
		return response, newError(ErrorDecode, method, endpoint, DeliveryResponseReceived, wire.status, err)
	}
	return response, nil
}

func emptyResponse(wire wireResponse) Response[Empty] {
	return Response[Empty]{StatusCode: wire.status, Header: wire.header, Raw: append([]byte(nil), wire.raw...)}
}

func textResponse(wire wireResponse) Response[string] {
	return Response[string]{
		StatusCode: wire.status, Header: wire.header, Data: string(wire.raw), Raw: append([]byte(nil), wire.raw...),
	}
}
