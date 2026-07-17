//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pgsty/go-patroni"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/dcs"
	"github.com/pgsty/go-patroni/model"
)

type staticControlSnapshot struct {
	snapshot dcs.Snapshot
}

type sequenceControlSnapshots struct {
	snapshots []dcs.Snapshot
	next      int
}

type recordingFailoverCAS struct {
	writes  int
	deletes int
}

func (store *recordingFailoverCAS) WriteFailover(context.Context, model.Target, []byte, *int64) (dcs.WriteResult, error) {
	store.writes++
	return dcs.WriteResult{}, errors.New("unexpected real-Patroni DCS fallback")
}

func (store *recordingFailoverCAS) DeleteFailover(context.Context, model.Target, *int64) (dcs.WriteResult, error) {
	store.deletes++
	return dcs.WriteResult{}, errors.New("unexpected failover delete")
}

func (reader staticControlSnapshot) Snapshot(ctx context.Context, _ model.Target) (dcs.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return dcs.Snapshot{}, err
	}
	return reader.snapshot, nil
}

func (reader *sequenceControlSnapshots) Snapshot(ctx context.Context, _ model.Target) (dcs.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return dcs.Snapshot{}, err
	}
	if len(reader.snapshots) == 0 {
		return dcs.Snapshot{}, errors.New("empty control snapshot sequence")
	}
	index := reader.next
	if index >= len(reader.snapshots) {
		index = len(reader.snapshots) - 1
	} else {
		reader.next++
	}
	return reader.snapshots[index], nil
}

type patroniObservation struct {
	status int
	header http.Header
	raw    []byte
	err    error
}

func observePatroni[T any](response patroni.Response[T], err error) patroniObservation {
	return patroniObservation{status: response.StatusCode, header: response.Header, raw: response.Raw, err: err}
}

func TestPatroniRESTInventoryAgainstIsolatedRealPatroni(t *testing.T) {
	if os.Getenv("GO_PATRONI_TEST_PATRONI_ISOLATED") != "1" {
		t.Fatal("refusing Patroni integration test without GO_PATRONI_TEST_PATRONI_ISOLATED=1")
	}
	baseURL := os.Getenv("GO_PATRONI_TEST_PATRONI_URL")
	if !strings.HasPrefix(baseURL, "https://127.0.0.1:") {
		t.Fatalf("refusing Patroni integration test against non-loopback URL %q", baseURL)
	}
	version := os.Getenv("GO_PATRONI_TEST_PATRONI_VERSION")
	if version != "3.3.8" && version != "4.0.7" && version != "4.1.3" {
		t.Fatalf("unexpected Patroni oracle version %q", version)
	}
	tlsOptions := patroni.TLSOptions{
		CAFile: os.Getenv("GO_PATRONI_TEST_PATRONI_CA"), CertFile: os.Getenv("GO_PATRONI_TEST_PATRONI_CLIENT_CERT"),
		KeyFile: os.Getenv("GO_PATRONI_TEST_PATRONI_CLIENT_KEY"), ServerName: "127.0.0.1",
	}
	transport, err := patroni.NewHTTPTransport(context.Background(), tlsOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	client, err := patroni.NewClient(patroni.ClientOptions{
		Transport:  transport,
		Authorizer: patroni.NewBasicAuth(os.Getenv("GO_PATRONI_TEST_PATRONI_USERNAME"), os.Getenv("GO_PATRONI_TEST_PATRONI_PASSWORD")),
		Timeout:    20 * time.Second,
		UserAgent:  "go-patroni-real-patroni-integration",
	})
	if err != nil {
		t.Fatal(err)
	}

	assertRealPatroniTLSAndAuthentication(t, baseURL, tlsOptions, transport)
	identity, err := client.GetPatroni(context.Background(), baseURL)
	if err != nil || identity.StatusCode != http.StatusOK || identity.Data.Patroni.Name != "node1" ||
		identity.Data.Patroni.Scope == "" || identity.Data.Patroni.Version != version || identity.Header.Get("X-GoPatroni-Lab") != "isolated" {
		t.Fatalf("real Patroni identity/TLS header mismatch: response=%#v err=%v", identity, err)
	}
	originalConfig, err := client.GetConfig(context.Background(), baseURL)
	if err != nil || originalConfig.StatusCode != http.StatusOK || len(originalConfig.Data) == 0 {
		t.Fatalf("read real Patroni dynamic config: response=%#v err=%v", originalConfig, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		restored, restoreErr := client.PutConfig(ctx, baseURL, originalConfig.Data)
		if restoreErr != nil || restored.StatusCode != http.StatusOK {
			t.Errorf("restore real Patroni dynamic config: status=%d err=%v", restored.StatusCode, restoreErr)
		}
	})
	unpaused, err := client.PatchConfig(context.Background(), baseURL, patroni.DynamicConfig{"pause": nil})
	if err != nil || unpaused.StatusCode != http.StatusOK {
		t.Fatalf("establish real Patroni unpaused baseline: response=%#v err=%v", unpaused, err)
	}

	controlTarget := (model.Target{Context: "isolated", Namespace: "/service", Scope: identity.Data.Patroni.Scope}).Normalize()
	controlSnapshot := dcs.BuildSnapshot(controlTarget, "/service/"+controlTarget.Scope, 100, []dcs.Entry{
		{RelativePath: "leader", ModRevision: 98, Value: []byte("node1")},
		{RelativePath: "members/node1", ModRevision: 99, Value: []byte(fmt.Sprintf(
			`{"api_url":%q,"conn_url":"postgres://127.0.0.1:5432/postgres","state":"running","role":"primary","version":%q}`,
			baseURL+"/patroni", version))},
		{RelativePath: "members/node2", ModRevision: 100, Value: []byte(fmt.Sprintf(
			`{"api_url":"https://127.0.0.1:1/patroni","state":"running","role":"replica","version":%q}`, version))},
	})
	failoverCAS := &recordingFailoverCAS{}
	controlService, err := control.NewService(control.ServiceOptions{
		Snapshots: staticControlSnapshot{snapshot: controlSnapshot}, Patroni: client, Failover: failoverCAS,
		Clock:          func() time.Time { return time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC) },
		NewOperationID: func() string { return "real-patroni-reload-" + version }, ProductVersion: "integration",
	})
	if err != nil {
		t.Fatal(err)
	}
	reloadRequest := control.ReloadRequest{Target: controlTarget, Members: []string{"node1"}, Role: control.RoleAny}
	preparedReload := controlService.PrepareReload(context.Background(), reloadRequest)
	if preparedReload.Outcome != control.Succeeded {
		t.Fatalf("prepare reload against real Patroni: %#v", preparedReload)
	}
	reloaded := controlService.ExecuteReload(context.Background(), reloadRequest, preparedReload.Data)
	if reloaded.Outcome != control.Succeeded || len(reloaded.Data.Members) != 1 ||
		reloaded.Data.Members[0].HTTPStatus != http.StatusAccepted || reloaded.Data.Members[0].SendState != control.SendAccepted {
		t.Fatalf("control reload against real Patroni: %#v", reloaded)
	}
	pauseRequest := control.PauseRequest{Target: controlTarget}
	preparedPause := controlService.PreparePause(context.Background(), pauseRequest)
	if preparedPause.Outcome != control.Succeeded {
		t.Fatalf("prepare pause against real Patroni: %#v", preparedPause)
	}
	paused := controlService.ExecutePause(context.Background(), pauseRequest, preparedPause.Data)
	if paused.Outcome != control.Succeeded || len(paused.Data.Results) != 1 ||
		paused.Data.Results[0].HTTPStatus != http.StatusOK || paused.Data.RESTSendState != control.SendAccepted {
		t.Fatalf("control pause against real Patroni: %#v", paused)
	}
	pausedSnapshot := dcs.BuildSnapshot(controlTarget, "/service/"+controlTarget.Scope, 102, []dcs.Entry{
		{RelativePath: "config", ModRevision: 101, Value: []byte(`{"pause":true}`)},
		{RelativePath: "leader", ModRevision: 98, Value: []byte("node1")},
		{RelativePath: "members/node1", ModRevision: 102, Value: []byte(fmt.Sprintf(
			`{"api_url":%q,"conn_url":"postgres://127.0.0.1:5432/postgres","state":"running","role":"primary","pause":true,"version":%q}`,
			baseURL+"/patroni", version))},
		{RelativePath: "members/node2", ModRevision: 100, Value: []byte(fmt.Sprintf(
			`{"api_url":"https://127.0.0.1:1/patroni","state":"running","role":"replica","pause":true,"version":%q}`, version))},
	})
	resumeService, err := control.NewService(control.ServiceOptions{
		Snapshots: staticControlSnapshot{snapshot: pausedSnapshot}, Patroni: client, Failover: failoverCAS,
		Clock: func() time.Time { return time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC) }, NewOperationID: func() string { return "real-patroni-resume-" + version },
	})
	if err != nil {
		t.Fatal(err)
	}
	preparedResume := resumeService.PrepareResume(context.Background(), pauseRequest)
	if preparedResume.Outcome != control.Succeeded {
		t.Fatalf("prepare resume against real Patroni: %#v", preparedResume)
	}
	resumed := resumeService.ExecuteResume(context.Background(), pauseRequest, preparedResume.Data)
	if resumed.Outcome != control.Succeeded || len(resumed.Data.Results) != 1 ||
		resumed.Data.Results[0].HTTPStatus != http.StatusOK || resumed.Data.RESTSendState != control.SendAccepted {
		t.Fatalf("control resume against real Patroni: %#v", resumed)
	}
	standbyCommands, featureErr := patroni.SupportsFeature(version, patroni.FeatureStandbyClusterCLI)
	if featureErr != nil {
		t.Fatal(featureErr)
	}
	if standbyCommands {
		standbyRoleSnapshot := dcs.BuildSnapshot(controlTarget, "/service/"+controlTarget.Scope, 103, []dcs.Entry{
			{RelativePath: "leader", ModRevision: 98, Value: []byte("node1")},
			{RelativePath: "members/node1", ModRevision: 103, Value: []byte(fmt.Sprintf(
				`{"api_url":%q,"state":"running","role":"standby_leader","version":%q}`, baseURL+"/patroni", version))},
		})
		primaryRoleSnapshot := dcs.BuildSnapshot(controlTarget, "/service/"+controlTarget.Scope, 104, []dcs.Entry{
			{RelativePath: "leader", ModRevision: 98, Value: []byte("node1")},
			{RelativePath: "members/node1", ModRevision: 104, Value: []byte(fmt.Sprintf(
				`{"api_url":%q,"state":"running","role":"primary","version":%q}`, baseURL+"/patroni", version))},
		})
		roleReader := &sequenceControlSnapshots{snapshots: []dcs.Snapshot{standbyRoleSnapshot, standbyRoleSnapshot, primaryRoleSnapshot}}
		roleService, roleErr := control.NewService(control.ServiceOptions{
			Snapshots: roleReader, Patroni: client,
			Clock:          func() time.Time { return time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC) },
			NewOperationID: func() string { return "real-patroni-promote-cluster-" + version },
			Wait:           func(context.Context, time.Duration) error { return nil },
		})
		if roleErr != nil {
			t.Fatal(roleErr)
		}
		promoteRequest := control.PromoteClusterRequest{Target: controlTarget, Force: true}
		preparedPromote := roleService.PreparePromoteCluster(context.Background(), promoteRequest)
		if preparedPromote.Outcome != control.Succeeded {
			t.Fatalf("prepare promote-cluster against real Patroni: %#v", preparedPromote)
		}
		promoted := roleService.ExecutePromoteCluster(context.Background(), promoteRequest, preparedPromote.Data)
		if promoted.Outcome != control.Succeeded || promoted.Data.HTTPStatus != http.StatusOK ||
			promoted.Data.RESTSendState != control.SendAccepted || promoted.Data.Verification != control.VerifiedSucceeded {
			t.Fatalf("control promote-cluster against real Patroni: %#v", promoted)
		}
	}
	restartRequest := control.RestartRequest{Target: controlTarget, Members: []string{"node1"}, Role: control.RoleAny}
	preparedRestart := controlService.PrepareRestart(context.Background(), restartRequest)
	if preparedRestart.Outcome != control.Succeeded {
		t.Fatalf("prepare restart against real Patroni: %#v", preparedRestart)
	}
	restarted := controlService.ExecuteRestart(context.Background(), restartRequest, preparedRestart.Data)
	if restarted.Outcome != control.Succeeded || len(restarted.Data.Members) != 1 ||
		restarted.Data.Members[0].HTTPStatus != http.StatusOK || restarted.Data.Members[0].SendState != control.SendAccepted {
		t.Fatalf("control restart against real Patroni: %#v", restarted)
	}
	failoverRequest := control.FailoverRequest{Target: controlTarget, Candidate: "node2", Force: true}
	preparedFailover := controlService.PrepareFailover(context.Background(), failoverRequest)
	if preparedFailover.Outcome != control.Succeeded {
		t.Fatalf("prepare failover against real Patroni: %#v", preparedFailover)
	}
	failedOver := controlService.ExecuteFailover(context.Background(), failoverRequest, preparedFailover.Data)
	if failedOver.Outcome != control.Failed || failedOver.Path != control.PathREST || failedOver.Data.HTTPStatus != http.StatusPreconditionFailed ||
		failedOver.Data.RESTSendState != control.SendAccepted || failoverCAS.writes != 0 {
		t.Fatalf("definite real Patroni failover rejection/fallback boundary: result=%#v dcsWrites=%d", failedOver, failoverCAS.writes)
	}
	flushSnapshot := dcs.BuildSnapshot(controlTarget, "/service/"+controlTarget.Scope, 101, []dcs.Entry{
		{RelativePath: "leader", ModRevision: 98, Value: []byte("node1")},
		{RelativePath: "members/node1", ModRevision: 101, Value: []byte(fmt.Sprintf(
			`{"api_url":%q,"state":"running","role":"primary","scheduled_restart":{"schedule":"2026-07-14T03:04:05Z"},"version":%q}`,
			baseURL+"/patroni", version))},
	})
	flushService, err := control.NewService(control.ServiceOptions{
		Snapshots: staticControlSnapshot{snapshot: flushSnapshot}, Patroni: client, Failover: failoverCAS,
		Clock: func() time.Time { return time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC) }, NewOperationID: func() string { return "real-patroni-flush-" + version },
	})
	if err != nil {
		t.Fatal(err)
	}
	flushRequest := control.FlushRequest{Target: controlTarget, Event: control.FlushRestart, Members: []string{"node1"}, Force: true}
	preparedFlush := flushService.PrepareFlush(context.Background(), flushRequest)
	if preparedFlush.Outcome != control.Succeeded {
		t.Fatalf("prepare restart flush against real Patroni: %#v", preparedFlush)
	}
	flushed := flushService.ExecuteFlush(context.Background(), flushRequest, preparedFlush.Data)
	if flushed.Outcome != control.Failed || len(flushed.Data.Results) != 1 || flushed.Data.Results[0].HTTPStatus != http.StatusNotFound ||
		flushed.Data.Results[0].SendState != control.SendAccepted || failoverCAS.deletes != 0 {
		t.Fatalf("real Patroni terminal restart-flush 404 boundary: result=%#v dcsDeletes=%d", flushed, failoverCAS.deletes)
	}

	endpoints, err := patroni.EndpointCatalogFor(version)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[string]int, len(endpoints))
	for _, endpoint := range endpoints {
		t.Run(endpoint.ID, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			observation := invokeRealPatroniEndpoint(ctx, client, baseURL, endpoint, originalConfig.Data)
			if observation.err != nil {
				t.Fatalf("real endpoint call failed: %v", observation.err)
			}
			if !allowedRealPatroniStatus(endpoint, observation.status) {
				t.Fatalf("real endpoint status %d is outside contract for %s %s; raw-bytes=%d",
					observation.status, endpoint.Method, endpoint.Path, len(observation.raw))
			}
			if observation.header == nil {
				t.Fatal("real Patroni response header map was not preserved")
			}
			if endpoint.Response != "status-only" && observation.status >= 200 && observation.status < 300 && len(observation.raw) == 0 {
				t.Fatal("successful real Patroni response lost raw wire data")
			}
			observed[endpoint.ID]++
		})
	}
	if len(observed) != len(endpoints) {
		t.Fatalf("real Patroni inventory covered %d endpoints, want %d for Patroni %s", len(observed), len(endpoints), version)
	}
	for _, endpoint := range endpoints {
		if observed[endpoint.ID] != 1 {
			t.Fatalf("real Patroni endpoint %s observed %d times", endpoint.ID, observed[endpoint.ID])
		}
	}
}

func assertRealPatroniTLSAndAuthentication(
	t *testing.T,
	baseURL string,
	tlsOptions patroni.TLSOptions,
	authenticatedTransport http.RoundTripper,
) {
	t.Helper()
	untrustedTransport, err := patroni.NewHTTPTransport(context.Background(), patroni.TLSOptions{
		CertFile: tlsOptions.CertFile, KeyFile: tlsOptions.KeyFile, ServerName: tlsOptions.ServerName,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer untrustedTransport.CloseIdleConnections()
	untrusted, _ := patroni.NewClient(patroni.ClientOptions{Transport: untrustedTransport, Timeout: 5 * time.Second})
	_, err = untrusted.GetPatroni(context.Background(), baseURL)
	var wireError *patroni.Error
	if !errors.As(err, &wireError) || wireError.Kind != patroni.ErrorTransport {
		t.Fatalf("untrusted real Patroni certificate was not rejected: %#v", err)
	}

	wrongHostnameTransport, err := patroni.NewHTTPTransport(context.Background(), patroni.TLSOptions{
		CAFile: tlsOptions.CAFile, CertFile: tlsOptions.CertFile, KeyFile: tlsOptions.KeyFile, ServerName: "wrong.invalid",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wrongHostnameTransport.CloseIdleConnections()
	wrongHostname, _ := patroni.NewClient(patroni.ClientOptions{Transport: wrongHostnameTransport, Timeout: 5 * time.Second})
	_, err = wrongHostname.GetPatroni(context.Background(), baseURL)
	if !errors.As(err, &wireError) || wireError.Kind != patroni.ErrorTransport {
		t.Fatalf("real Patroni certificate hostname mismatch was not rejected: %#v", err)
	}

	noCertificateTransport, err := patroni.NewHTTPTransport(context.Background(), patroni.TLSOptions{
		CAFile: tlsOptions.CAFile, ServerName: tlsOptions.ServerName,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer noCertificateTransport.CloseIdleConnections()
	noCertificate, _ := patroni.NewClient(patroni.ClientOptions{Transport: noCertificateTransport, Timeout: 5 * time.Second})
	_, err = noCertificate.GetPatroni(context.Background(), baseURL)
	if !errors.As(err, &wireError) || wireError.Kind != patroni.ErrorTransport {
		t.Fatalf("real Patroni mTLS accepted a missing client certificate: %#v", err)
	}

	unauthorized, _ := patroni.NewClient(patroni.ClientOptions{Transport: authenticatedTransport, Timeout: 5 * time.Second})
	response, err := unauthorized.PatchConfig(context.Background(), baseURL, patroni.DynamicConfig{"loop_wait": 10})
	if err != nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("real Patroni unsafe endpoint did not enforce Basic auth: response=%#v err=%v", response, err)
	}
}

func invokeRealPatroniEndpoint(
	ctx context.Context,
	client *patroni.Client,
	baseURL string,
	endpoint patroni.Endpoint,
	original patroni.DynamicConfig,
) patroniObservation {
	if isRealHealthPath(endpoint.Path) {
		switch endpoint.Method {
		case http.MethodGet:
			response, err := client.GetHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path), patroni.HealthQuery{})
			return observePatroni(response, err)
		case http.MethodHead:
			response, err := client.HeadHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path), patroni.HealthQuery{})
			return observePatroni(response, err)
		case http.MethodOptions:
			response, err := client.OptionsHealth(ctx, baseURL, patroni.HealthAlias(endpoint.Path))
			return observePatroni(response, err)
		}
	}
	switch endpoint.ID {
	case "get-liveness":
		response, err := client.GetLiveness(ctx, baseURL)
		return observePatroni(response, err)
	case "get-readiness":
		response, err := client.GetReadiness(ctx, baseURL, patroni.ReadinessQuery{})
		return observePatroni(response, err)
	case "get-patroni":
		response, err := client.GetPatroni(ctx, baseURL)
		return observePatroni(response, err)
	case "get-cluster":
		response, err := client.GetCluster(ctx, baseURL)
		return observePatroni(response, err)
	case "get-history":
		response, err := client.GetHistory(ctx, baseURL)
		return observePatroni(response, err)
	case "get-config":
		response, err := client.GetConfig(ctx, baseURL)
		return observePatroni(response, err)
	case "get-metrics":
		response, err := client.GetMetrics(ctx, baseURL)
		return observePatroni(response, err)
	case "get-failsafe":
		response, err := client.GetFailsafe(ctx, baseURL)
		return observePatroni(response, err)
	case "patch-config":
		response, err := client.PatchConfig(ctx, baseURL, patroni.DynamicConfig{"loop_wait": original["loop_wait"]})
		return observePatroni(response, err)
	case "put-config":
		response, err := client.PutConfig(ctx, baseURL, original)
		return observePatroni(response, err)
	case "post-reload":
		response, err := client.PostReload(ctx, baseURL)
		return observePatroni(response, err)
	case "post-failsafe":
		response, err := client.PostFailsafe(ctx, baseURL, patroni.FailsafePeerRequest{
			Name: "node1", ConnURL: "postgres://node1/postgres", APIURL: "https://node1:8008/patroni",
		})
		return observePatroni(response, err)
	case "post-sigterm":
		response, err := client.PostSigterm(ctx, baseURL)
		return observePatroni(response, err)
	case "post-restart":
		response, err := client.PostRestart(ctx, baseURL, patroni.RestartRequest{Role: "not-a-patroni-role"})
		return observePatroni(response, err)
	case "delete-restart":
		response, err := client.DeleteRestart(ctx, baseURL)
		return observePatroni(response, err)
	case "delete-switchover":
		response, err := client.DeleteSwitchover(ctx, baseURL)
		return observePatroni(response, err)
	case "post-reinitialize":
		response, err := client.PostReinitialize(ctx, baseURL, patroni.ReinitializeRequest{})
		return observePatroni(response, err)
	case "post-failover":
		response, err := client.PostFailover(ctx, baseURL, patroni.FailoverRequest{Candidate: "missing-member"})
		return observePatroni(response, err)
	case "post-switchover":
		response, err := client.PostSwitchover(ctx, baseURL, patroni.FailoverRequest{Leader: "node1", Candidate: "missing-member"})
		return observePatroni(response, err)
	case "post-citus":
		response, err := client.PostCitus(ctx, baseURL, patroni.MPPEvent{Type: "before_demote", Group: 1, Leader: "node1"})
		return observePatroni(response, err)
	case "post-mpp":
		response, err := client.PostMPP(ctx, baseURL, patroni.MPPEvent{Type: "after_promote", Group: 1, Leader: "node1"})
		return observePatroni(response, err)
	default:
		return patroniObservation{err: fmt.Errorf("real Patroni dispatcher lacks endpoint %s", endpoint.ID)}
	}
}

func isRealHealthPath(path string) bool {
	for _, alias := range patroni.HealthAliases() {
		if string(alias) == path {
			return true
		}
	}
	return false
}

func allowedRealPatroniStatus(endpoint patroni.Endpoint, status int) bool {
	if isRealHealthPath(endpoint.Path) {
		return status == http.StatusOK || status == http.StatusServiceUnavailable
	}
	switch endpoint.ID {
	case "get-liveness", "get-readiness", "get-patroni", "get-cluster", "get-history", "get-config", "get-metrics",
		"patch-config", "put-config", "post-citus", "post-mpp":
		return status == http.StatusOK
	case "get-failsafe", "post-failsafe":
		return status == http.StatusBadGateway
	case "post-reload", "post-sigterm":
		return status == http.StatusAccepted
	case "post-restart":
		return status == http.StatusBadRequest
	case "delete-restart", "delete-switchover":
		return status == http.StatusNotFound
	case "post-reinitialize":
		return status == http.StatusServiceUnavailable
	case "post-failover", "post-switchover":
		return status == http.StatusPreconditionFailed || status == http.StatusBadRequest
	default:
		return false
	}
}
