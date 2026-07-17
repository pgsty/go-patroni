package patroni_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
)

type observation struct {
	status int
	header http.Header
	raw    []byte
	err    error
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type closeErrorBody struct {
	io.Reader
	err error
}

func (body closeErrorBody) Close() error { return body.err }

func observe[T any](response patroni.Response[T], err error) observation {
	return observation{status: response.StatusCode, header: response.Header, raw: response.Raw, err: err}
}

func invokeEndpoint(ctx context.Context, client *patroni.Client, baseURL string, endpoint patroni.Endpoint) observation {
	if strings.HasPrefix(endpoint.ID, "get-") && isHealthPath(endpoint.Path) {
		response, err := client.GetHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path), patroni.HealthQuery{})
		return observe(response, err)
	}
	if strings.HasPrefix(endpoint.ID, "head-") {
		response, err := client.HeadHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path), patroni.HealthQuery{})
		return observe(response, err)
	}
	if strings.HasPrefix(endpoint.ID, "options-") {
		response, err := client.OptionsHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path))
		return observe(response, err)
	}
	switch endpoint.ID {
	case "get-liveness":
		response, err := client.GetLiveness(ctx, baseURL)
		return observe(response, err)
	case "get-readiness":
		response, err := client.GetReadiness(ctx, baseURL, patroni.ReadinessQuery{})
		return observe(response, err)
	case "get-patroni":
		response, err := client.GetPatroni(ctx, baseURL)
		return observe(response, err)
	case "get-cluster":
		response, err := client.GetCluster(ctx, baseURL)
		return observe(response, err)
	case "get-history":
		response, err := client.GetHistory(ctx, baseURL)
		return observe(response, err)
	case "get-config":
		response, err := client.GetConfig(ctx, baseURL)
		return observe(response, err)
	case "get-metrics":
		response, err := client.GetMetrics(ctx, baseURL)
		return observe(response, err)
	case "get-failsafe":
		response, err := client.GetFailsafe(ctx, baseURL)
		return observe(response, err)
	case "patch-config":
		response, err := client.PatchConfig(ctx, baseURL, patroni.DynamicConfig{"loop_wait": 5})
		return observe(response, err)
	case "put-config":
		response, err := client.PutConfig(ctx, baseURL, patroni.DynamicConfig{"ttl": 30})
		return observe(response, err)
	case "post-reload":
		response, err := client.PostReload(ctx, baseURL)
		return observe(response, err)
	case "post-failsafe":
		response, err := client.PostFailsafe(ctx, baseURL, patroni.FailsafePeerRequest{Name: "node-1", ConnURL: "postgres://db/app", APIURL: "http://node-1:8008/patroni"})
		return observe(response, err)
	case "post-sigterm":
		response, err := client.PostSigterm(ctx, baseURL)
		return observe(response, err)
	case "post-restart":
		response, err := client.PostRestart(ctx, baseURL, patroni.RestartRequest{})
		return observe(response, err)
	case "delete-restart":
		response, err := client.DeleteRestart(ctx, baseURL)
		return observe(response, err)
	case "delete-switchover":
		response, err := client.DeleteSwitchover(ctx, baseURL)
		return observe(response, err)
	case "post-reinitialize":
		response, err := client.PostReinitialize(ctx, baseURL, patroni.ReinitializeRequest{})
		return observe(response, err)
	case "post-failover":
		response, err := client.PostFailover(ctx, baseURL, patroni.FailoverRequest{Candidate: "node-2"})
		return observe(response, err)
	case "post-switchover":
		response, err := client.PostSwitchover(ctx, baseURL, patroni.FailoverRequest{Leader: "node-1"})
		return observe(response, err)
	case "post-citus":
		response, err := client.PostCitus(ctx, baseURL, patroni.MPPEvent{Type: "before_demote", Group: 1, Leader: "node-1"})
		return observe(response, err)
	case "post-mpp":
		response, err := client.PostMPP(ctx, baseURL, patroni.MPPEvent{Type: "after_promote", Group: 1, Leader: "node-1"})
		return observe(response, err)
	default:
		return observation{err: fmt.Errorf("test dispatcher lacks %s", endpoint.ID)}
	}
}

func isHealthPath(endpointPath string) bool {
	for _, alias := range patroni.HealthAliases() {
		if string(alias) == endpointPath {
			return true
		}
	}
	return false
}

func writeContractResponse(writer http.ResponseWriter, endpoint patroni.Endpoint) {
	writer.Header().Set("X-Patroni-Contract", endpoint.ID)
	switch endpoint.Response {
	case "status-only":
		writer.WriteHeader(http.StatusOK)
	case "status-json":
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"state":"running","role":"primary","patroni":{"version":"4.1.0","scope":"demo","name":"node-1"},"future":true}`)
	case "cluster-json":
		_, _ = io.WriteString(writer, `{"members":[],"scope":"demo"}`)
	case "history-json":
		_, _ = io.WriteString(writer, `[[1,42,"test","2026-01-01T00:00:00Z","node-1"]]`)
	case "config-json":
		_, _ = io.WriteString(writer, `{"ttl":30,"loop_wait":10}`)
	case "prometheus-text":
		_, _ = io.WriteString(writer, "patroni_primary 1\n")
	case "failsafe-json":
		_, _ = io.WriteString(writer, `{"node-1":"http://node-1:8008/patroni"}`)
	case "text":
		_, _ = io.WriteString(writer, "Accepted")
	default:
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func writeErrorContractResponse(writer http.ResponseWriter, endpoint patroni.Endpoint) {
	writer.Header().Set("X-Patroni-Contract", endpoint.ID)
	writer.WriteHeader(http.StatusServiceUnavailable)
	if endpoint.Response == "status-json" {
		_, _ = io.WriteString(writer, `{"state":"stopped","role":"replica","patroni":{"version":"4.1.0","scope":"demo","name":"node-1"}}`)
	} else if endpoint.Response != "status-only" {
		_, _ = io.WriteString(writer, "injected endpoint failure")
	}
}

func TestPatroniIdentityNameIsOptionalBefore32(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/patroni" {
			t.Fatalf("unexpected Patroni identity request %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"state":"running","role":"primary","patroni":{"version":"3.1.2","scope":"demo"}}`)
	}))
	defer server.Close()

	client, err := patroni.NewClient(patroni.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.GetPatroni(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("decode Patroni 3.1 identity: %v", err)
	}
	if response.Data.Patroni.Version != "3.1.2" || response.Data.Patroni.Scope != "demo" || response.Data.Patroni.Name != "" {
		t.Fatalf("Patroni 3.1 identity = %#v", response.Data.Patroni)
	}
}

func TestEveryCatalogEndpointHasCallableWireContract(t *testing.T) {
	client, err := patroni.NewClient(patroni.ClientOptions{UserAgent: "go-patroni-contract-test"})
	if err != nil {
		t.Fatal(err)
	}
	catalog := patroni.EndpointCatalog()
	if len(catalog) != 75 {
		t.Fatalf("catalog contains %d endpoints", len(catalog))
	}
	for _, endpoint := range catalog {
		t.Run(endpoint.ID, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				wantPath := "/prefix" + endpoint.Path
				if endpoint.Path == "/" {
					wantPath = "/prefix/"
				}
				if request.Method != endpoint.Method || request.URL.Path != wantPath {
					t.Errorf("wire request mismatch: got %s %s want %s %s", request.Method, request.URL.Path, endpoint.Method, wantPath)
				}
				if request.UserAgent() != "go-patroni-contract-test" {
					t.Errorf("user agent missing: %q", request.UserAgent())
				}
				body, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Error(readErr)
				}
				if endpoint.Request != "none" {
					if request.Header.Get("Content-Type") != "application/json" || !json.Valid(body) {
						t.Errorf("typed JSON request missing for %s: content-type=%q body=%q", endpoint.ID, request.Header.Get("Content-Type"), body)
					}
				} else if len(body) != 0 {
					t.Errorf("body sent for bodyless endpoint %s", endpoint.ID)
				}
				if calls.Add(1) == 1 {
					writeContractResponse(writer, endpoint)
				} else {
					writeErrorContractResponse(writer, endpoint)
				}
			}))
			defer server.Close()

			result := invokeEndpoint(context.Background(), client, server.URL+"/prefix", endpoint)
			if result.err != nil {
				t.Fatalf("endpoint call failed: %v", result.err)
			}
			if result.status != http.StatusOK || result.header.Get("X-Patroni-Contract") != endpoint.ID {
				t.Fatalf("response metadata mismatch: status=%d header=%v", result.status, result.header)
			}
			if endpoint.Response != "status-only" && len(result.raw) == 0 {
				t.Fatal("raw response escape hatch is empty")
			}

			failure := invokeEndpoint(context.Background(), client, server.URL+"/prefix", endpoint)
			if failure.err != nil {
				t.Fatalf("endpoint-specific error response became a transport/decode error: %v", failure.err)
			}
			if failure.status != http.StatusServiceUnavailable || failure.header.Get("X-Patroni-Contract") != endpoint.ID {
				t.Fatalf("endpoint-specific error metadata mismatch: status=%d header=%v", failure.status, failure.header)
			}
			if endpoint.Response != "status-only" && len(failure.raw) == 0 {
				t.Fatal("endpoint-specific error raw response is empty")
			}
		})
	}
}

func TestTypedResponsesQueriesAndUnknownFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Test", "preserved")
		switch request.URL.Path {
		case "/api/replica":
			if request.URL.Query().Get("lag") != "10MB" || request.URL.Query().Has("replication_state") || request.URL.Query().Get("tag_zone") != "east" {
				t.Errorf("health query mismatch: %v", request.URL.Query())
			}
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(writer, `{"state":"running","role":"replica","server_version":160004,"xlog":{"replayed_location":42},"patroni":{"version":"4.1.0","scope":"demo","name":"node-2"},"unknown_new_field":{"kept":"raw"}}`)
		case "/api/cluster":
			_, _ = io.WriteString(writer, `{"members":[{"name":"node-2","role":"replica","state":"streaming","lag":"unknown"}],"scope":"demo"}`)
		case "/api/history":
			_, _ = io.WriteString(writer, `[[2,99,"reason","2026-01-01T00:00:00Z","node-2",{"future":true}]]`)
		case "/api/config":
			_, _ = io.WriteString(writer, `{"ttl":30,"future":{"enabled":true}}`)
		case "/api/failsafe":
			_, _ = io.WriteString(writer, `{"node-2":"http://node-2:8008/patroni"}`)
		case "/api/metrics":
			_, _ = io.WriteString(writer, "patroni_primary 0\n")
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	base := server.URL + "/api"

	health, err := client.GetHealth(context.Background(), base, patroni.HealthReplica, patroni.HealthQuery{
		Lag: "10MB", ReplicationState: "streaming", Tags: map[string]string{"zone": "east"},
	})
	if err != nil || health.StatusCode != 503 || health.Data.Role != "replica" || health.Data.Patroni.Name != "node-2" || health.Header.Get("X-Test") != "preserved" || !strings.Contains(string(health.Raw), "unknown_new_field") {
		t.Fatalf("typed health mismatch: response=%#v err=%v", health, err)
	}
	cluster, err := client.GetCluster(context.Background(), base)
	if err != nil || len(cluster.Data.Members) != 1 || string(cluster.Data.Members[0].Lag) != `"unknown"` {
		t.Fatalf("typed cluster mismatch: response=%#v err=%v", cluster, err)
	}
	history, err := client.GetHistory(context.Background(), base)
	if err != nil || len(history.Data) != 1 || history.Data[0].Timeline != 2 || history.Data[0].Member != "node-2" || len(history.Data[0].Extra) != 1 {
		t.Fatalf("typed history mismatch: response=%#v err=%v", history, err)
	}
	configuration, err := client.GetConfig(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if number, ok := configuration.Data["ttl"].(json.Number); !ok || number.String() != "30" {
		t.Fatalf("dynamic config number lost: %#v", configuration.Data)
	}
	failsafe, err := client.GetFailsafe(context.Background(), base)
	if err != nil || failsafe.Data["node-2"] == "" {
		t.Fatalf("failsafe mismatch: %#v err=%v", failsafe, err)
	}
	metrics, err := client.GetMetrics(context.Background(), base)
	if err != nil || metrics.Data != string(metrics.Raw) || !strings.Contains(metrics.Data, "patroni_primary") {
		t.Fatalf("metrics mismatch: %#v err=%v", metrics, err)
	}
}

func TestDecodeErrorPreservesStatusHeadersAndRaw(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Decode", "evidence")
		_, _ = io.WriteString(writer, `{"server_version":"not-an-integer","patroni":{}}`)
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	response, err := client.GetPatroni(context.Background(), server.URL)
	var patroniErr *patroni.Error
	if !errors.As(err, &patroniErr) || patroniErr.Kind != patroni.ErrorDecode || patroniErr.Delivery != patroni.DeliveryResponseReceived {
		t.Fatalf("decode error classification mismatch: %#v", err)
	}
	if response.StatusCode != 200 || response.Header.Get("X-Decode") != "evidence" || !strings.Contains(string(response.Raw), "not-an-integer") {
		t.Fatalf("decode evidence was lost: %#v", response)
	}
}

func TestResponseBodyCloseErrorPreservesReceivedEvidence(t *testing.T) {
	closeError := errors.New("test-only response close failure")
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"X-Close-Test": []string{"preserved"}},
			Body:       closeErrorBody{Reader: strings.NewReader("patroni_primary 1\n"), err: closeError},
		}, nil
	})
	client, err := patroni.NewClient(patroni.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.GetMetrics(context.Background(), "https://patroni.example.invalid")
	var typed *patroni.Error
	if !errors.As(err, &typed) || !errors.Is(err, closeError) || typed.Kind != patroni.ErrorResponseBody ||
		typed.Delivery != patroni.DeliveryResponseReceived {
		t.Fatalf("response close error classification mismatch: %#v", err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Close-Test") != "preserved" ||
		response.Data != "patroni_primary 1\n" || string(response.Raw) != response.Data {
		t.Fatalf("response close error lost received evidence: %#v", response)
	}
}

func TestEndpointSpecificErrorStatusDoesNotBecomeDecodeFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(writer, "DCS unavailable")
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	response, err := client.GetConfig(context.Background(), server.URL)
	if err != nil || response.StatusCode != http.StatusBadGateway || string(response.Raw) != "DCS unavailable" || response.Data != nil {
		t.Fatalf("endpoint status contract mismatch: response=%#v err=%v", response, err)
	}
}

func TestWritesDoNotFollowRedirectsButReadsDo(t *testing.T) {
	var redirectedWrites atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/restart":
			http.Redirect(writer, request, "/write-target", http.StatusTemporaryRedirect)
		case "/write-target":
			redirectedWrites.Add(1)
			_, _ = io.WriteString(writer, "unexpected")
		case "/patroni":
			http.Redirect(writer, request, "/read-target", http.StatusTemporaryRedirect)
		case "/read-target":
			_, _ = io.WriteString(writer, `{"state":"running","patroni":{"version":"4.1.0","scope":"demo","name":"node-1"}}`)
		}
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	write, err := client.PostRestart(context.Background(), server.URL, patroni.RestartRequest{})
	if err != nil || write.StatusCode != http.StatusTemporaryRedirect || redirectedWrites.Load() != 0 {
		t.Fatalf("write redirect policy mismatch: response=%#v calls=%d err=%v", write, redirectedWrites.Load(), err)
	}
	read, err := client.GetPatroni(context.Background(), server.URL)
	if err != nil || read.StatusCode != http.StatusOK || read.Data.State != "running" {
		t.Fatalf("read redirect policy mismatch: response=%#v err=%v", read, err)
	}
}

func TestRestartRequestPreservesPatronictlTimeoutAndConditions(t *testing.T) {
	pending := true
	var observed map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/restart" {
			t.Fatalf("unexpected restart request %s %s", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&observed); err != nil {
			t.Fatal(err)
		}
		writer.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(writer, "scheduled")
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	response, err := client.PostRestart(context.Background(), server.URL, patroni.RestartRequest{
		Schedule: "2026-07-13T17:00:00Z", PostgresVersion: "16.4", Timeout: "1000 ms", RestartPending: &pending,
	})
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("restart request failed: response=%#v err=%v", response, err)
	}
	want := map[string]any{
		"schedule": "2026-07-13T17:00:00Z", "postgres_version": "16.4", "timeout": "1000 ms", "restart_pending": true,
	}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("restart request body = %#v, want %#v", observed, want)
	}
}

func TestRestartWireContractsAcceptNumericTimeoutAndDecodeScheduledFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/restart":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if timeout, ok := body["timeout"].(float64); !ok || timeout != 30 {
				t.Fatalf("numeric restart timeout = %#v, want 30", body["timeout"])
			}
			_, _ = io.WriteString(writer, "restarted")
		case "/patroni":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"state":"running","patroni":{"version":"4.1.3","scope":"demo","name":"node-1"},"scheduled_restart":{"schedule":"2026-07-18T00:00:00+00:00","role":"replica","postgres_version":"17.1","timeout":"30s","restart_pending":true}}`)
		default:
			t.Fatalf("unexpected request path %q", request.URL.Path)
		}
	}))
	defer server.Close()

	client, _ := patroni.NewClient(patroni.ClientOptions{})
	if _, err := client.PostRestart(context.Background(), server.URL, patroni.RestartRequest{Timeout: 30}); err != nil {
		t.Fatalf("numeric restart timeout failed: %v", err)
	}
	response, err := client.GetPatroni(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("decode scheduled restart: %v", err)
	}
	scheduled := response.Data.ScheduledRestart
	if scheduled == nil || scheduled.Role != "replica" || scheduled.PostgresVersion != "17.1" || scheduled.Timeout != "30s" || scheduled.RestartPending == nil || !*scheduled.RestartPending {
		t.Fatalf("scheduled restart = %#v", scheduled)
	}
}

func TestCancellationClassifiesNotSentAndMaybeSent(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.PostRestart(cancelled, server.URL, patroni.RestartRequest{})
	var patroniErr *patroni.Error
	if !errors.As(err, &patroniErr) || patroniErr.Delivery != patroni.DeliveryNotSent || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-send cancellation mismatch: %#v", err)
	}

	timed, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	_, err = client.PostRestart(timed, server.URL, patroni.RestartRequest{})
	close(release)
	if !errors.As(err, &patroniErr) || patroniErr.Delivery != patroni.DeliveryMaybeSent || !patroniErr.AmbiguousWrite() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-send cancellation mismatch: %#v", err)
	}
}

type countingFailTransport struct{ calls atomic.Int32 }

func (transport *countingFailTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls.Add(1)
	return nil, errors.New("injected transport failure")
}

func TestWriteTransportNeverRetries(t *testing.T) {
	transport := &countingFailTransport{}
	client, err := patroni.NewClient(patroni.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PostFailover(context.Background(), "http://patroni.invalid", patroni.FailoverRequest{Candidate: "node-2"})
	var patroniErr *patroni.Error
	if !errors.As(err, &patroniErr) || patroniErr.Kind != patroni.ErrorTransport || transport.calls.Load() != 1 {
		t.Fatalf("write retry contract mismatch: calls=%d err=%#v", transport.calls.Load(), err)
	}
}

func TestDefaultDeadlineAndResponseLimit(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			<-release
		}))
		defer server.Close()
		client, _ := patroni.NewClient(patroni.ClientOptions{Timeout: 30 * time.Millisecond})
		caller, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		started := time.Now()
		_, err := client.GetPatroni(caller, server.URL)
		close(release)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("default deadline not enforced promptly: elapsed=%s err=%v", time.Since(started), err)
		}
	})
	t.Run("body limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(writer, strings.Repeat("x", 32))
		}))
		defer server.Close()
		client, _ := patroni.NewClient(patroni.ClientOptions{MaxResponseBytes: 8})
		response, err := client.GetMetrics(context.Background(), server.URL)
		var patroniErr *patroni.Error
		if !errors.As(err, &patroniErr) || patroniErr.Kind != patroni.ErrorResponseBody || patroniErr.Delivery != patroni.DeliveryResponseReceived || len(response.Raw) != 8 {
			t.Fatalf("body limit classification mismatch: response=%#v err=%#v", response, err)
		}
	})
}

type rejectingAuthorizer struct{}

func (rejectingAuthorizer) Authorize(context.Context, *http.Request) error {
	return errors.New("__BOAR_TEST_ONLY_AUTHORIZER_PASSWORD__")
}

func TestCredentialsAndErrorsAreSafeToFormat(t *testing.T) {
	auth := patroni.NewBasicAuth("admin", "__BOAR_TEST_ONLY_BASIC_PASSWORD__")
	for _, output := range []string{fmt.Sprint(auth), fmt.Sprintf("%#v", auth)} {
		if strings.Contains(output, "__BOAR_TEST_ONLY_BASIC_PASSWORD__") || strings.Contains(output, "admin") {
			t.Fatalf("basic auth formatting leaked credentials")
		}
	}
	client, _ := patroni.NewClient(patroni.ClientOptions{Authorizer: rejectingAuthorizer{}})
	_, err := client.GetPatroni(context.Background(), "http://example.invalid")
	if err == nil || strings.Contains(err.Error(), "__BOAR_TEST_ONLY_AUTHORIZER_PASSWORD__") {
		t.Fatalf("authorizer error leaked through public formatting")
	}
	_, err = client.GetPatroni(context.Background(), "http://user:__BOAR_TEST_ONLY_URL_PASSWORD__@example.invalid")
	if err == nil || strings.Contains(err.Error(), "__BOAR_TEST_ONLY_URL_PASSWORD__") {
		t.Fatalf("base URL validation leaked userinfo")
	}
}

func TestBasicAuthAppliedAndRequestMarshalFailureIsNotSent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username, password, ok := request.BasicAuth()
		if !ok || username != "test-admin" || password != "test-password" {
			t.Error("basic authentication header mismatch")
		}
		_, _ = io.WriteString(writer, `{"patroni":{"version":"4.1.0","scope":"demo","name":"node-1"}}`)
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{Authorizer: patroni.NewBasicAuth("test-admin", "test-password")})
	if _, err := client.GetPatroni(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
	_, err := client.PatchConfig(context.Background(), server.URL, patroni.DynamicConfig{"bad": make(chan int)})
	var patroniErr *patroni.Error
	if !errors.As(err, &patroniErr) || patroniErr.Kind != patroni.ErrorRequest || patroniErr.Delivery != patroni.DeliveryNotSent {
		t.Fatalf("marshal error classification mismatch: %#v", err)
	}
}

func TestInvalidHealthAliasAndClientOptionsFailClosed(t *testing.T) {
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	_, err := client.GetHealth(context.Background(), "http://example.invalid", patroni.HealthAlias("/invented"), patroni.HealthQuery{})
	var patroniErr *patroni.Error
	if !errors.As(err, &patroniErr) || patroniErr.Kind != patroni.ErrorRequest || patroniErr.Delivery != patroni.DeliveryNotSent {
		t.Fatalf("invalid alias classification mismatch: %#v", err)
	}
	_, err = patroni.NewClient(patroni.ClientOptions{HTTPClient: &http.Client{}, Transport: http.DefaultTransport})
	if err == nil {
		t.Fatal("ambiguous client transport options were accepted")
	}
}

func TestReadinessQueryEncodingIsDeterministic(t *testing.T) {
	seen := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen <- request.URL.Query()
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, _ := patroni.NewClient(patroni.ClientOptions{})
	if _, err := client.GetReadiness(context.Background(), server.URL, patroni.ReadinessQuery{Lag: "42", Mode: "write"}); err != nil {
		t.Fatal(err)
	}
	query := <-seen
	if query.Encode() != "lag=42&mode=write" {
		t.Fatalf("readiness query mismatch: %s", query.Encode())
	}
}
