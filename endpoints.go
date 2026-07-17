package patroni

import (
	"context"
	"net/http"
	"net/url"
)

type HealthQuery struct {
	Lag string
	// ReplicationState is retained for source compatibility only. Patroni does
	// not implement replication_state as a health-query filter.
	// Deprecated: use Lag and Tags, or inspect Status.ReplicationState.
	ReplicationState string
	Tags             map[string]string
}

func (query HealthQuery) values() url.Values {
	values := url.Values{}
	if query.Lag != "" {
		values.Set("lag", query.Lag)
	}
	for name, value := range query.Tags {
		values.Set("tag_"+name, value)
	}
	return values
}

type ReadinessQuery struct {
	Lag  string
	Mode string
}

func (query ReadinessQuery) values() url.Values {
	values := url.Values{}
	if query.Lag != "" {
		values.Set("lag", query.Lag)
	}
	if query.Mode != "" {
		values.Set("mode", query.Mode)
	}
	return values
}

func invalidAlias(method string, alias HealthAlias) error {
	return newError(ErrorRequest, method, string(alias), DeliveryNotSent, 0, errInvalidHealthAlias)
}

var errInvalidHealthAlias = &wireContractError{"unknown health alias"}

type wireContractError struct{ message string }

func (err *wireContractError) Error() string { return err.message }

func (client *Client) GetHealth(ctx context.Context, baseURL string, alias HealthAlias, query HealthQuery) (Response[Status], error) {
	if !validHealthAlias(alias) {
		return Response[Status]{}, invalidAlias(http.MethodGet, alias)
	}
	wire, err := client.execute(ctx, http.MethodGet, baseURL, string(alias), query.values(), nil)
	if err != nil {
		return Response[Status]{StatusCode: wire.status, Header: wire.header, Raw: wire.raw}, err
	}
	return jsonResponse[Status](wire, http.MethodGet, string(alias), true)
}

func (client *Client) HeadHealth(ctx context.Context, baseURL string, alias HealthAlias, query HealthQuery) (Response[Empty], error) {
	if !validHealthAlias(alias) {
		return Response[Empty]{}, invalidAlias(http.MethodHead, alias)
	}
	wire, err := client.execute(ctx, http.MethodHead, baseURL, string(alias), query.values(), nil)
	return emptyResponse(wire), err
}

func (client *Client) OptionsHealth(ctx context.Context, baseURL string, alias HealthAlias) (Response[Empty], error) {
	if !validHealthAlias(alias) {
		return Response[Empty]{}, invalidAlias(http.MethodOptions, alias)
	}
	wire, err := client.execute(ctx, http.MethodOptions, baseURL, string(alias), nil, nil)
	return emptyResponse(wire), err
}

func (client *Client) GetLiveness(ctx context.Context, baseURL string) (Response[Empty], error) {
	wire, err := client.execute(ctx, http.MethodGet, baseURL, "/liveness", nil, nil)
	return emptyResponse(wire), err
}

func (client *Client) GetReadiness(ctx context.Context, baseURL string, query ReadinessQuery) (Response[Empty], error) {
	wire, err := client.execute(ctx, http.MethodGet, baseURL, "/readiness", query.values(), nil)
	return emptyResponse(wire), err
}

func (client *Client) GetPatroni(ctx context.Context, baseURL string) (Response[Status], error) {
	return getJSON[Status](ctx, client, baseURL, "/patroni", nil, true)
}

func (client *Client) GetCluster(ctx context.Context, baseURL string) (Response[Cluster], error) {
	return getJSON[Cluster](ctx, client, baseURL, "/cluster", nil, true)
}

func (client *Client) GetHistory(ctx context.Context, baseURL string) (Response[History], error) {
	return getJSON[History](ctx, client, baseURL, "/history", nil, true)
}

func (client *Client) GetConfig(ctx context.Context, baseURL string) (Response[DynamicConfig], error) {
	return getJSON[DynamicConfig](ctx, client, baseURL, "/config", nil, true)
}

func (client *Client) GetMetrics(ctx context.Context, baseURL string) (Response[string], error) {
	wire, err := client.execute(ctx, http.MethodGet, baseURL, "/metrics", nil, nil)
	return textResponse(wire), err
}

func (client *Client) GetFailsafe(ctx context.Context, baseURL string) (Response[FailsafeTopology], error) {
	return getJSON[FailsafeTopology](ctx, client, baseURL, "/failsafe", nil, true)
}

func (client *Client) PatchConfig(ctx context.Context, baseURL string, patch DynamicConfig) (Response[DynamicConfig], error) {
	return sendJSONForJSON[DynamicConfig](ctx, client, http.MethodPatch, baseURL, "/config", patch, true)
}

func (client *Client) PutConfig(ctx context.Context, baseURL string, configuration DynamicConfig) (Response[DynamicConfig], error) {
	return sendJSONForJSON[DynamicConfig](ctx, client, http.MethodPut, baseURL, "/config", configuration, true)
}

func (client *Client) PostReload(ctx context.Context, baseURL string) (Response[string], error) {
	return sendForText(ctx, client, http.MethodPost, baseURL, "/reload", nil)
}

func (client *Client) PostFailsafe(ctx context.Context, baseURL string, request FailsafePeerRequest) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/failsafe", request)
}

func (client *Client) PostSigterm(ctx context.Context, baseURL string) (Response[string], error) {
	return sendForText(ctx, client, http.MethodPost, baseURL, "/sigterm", nil)
}

func (client *Client) PostRestart(ctx context.Context, baseURL string, request RestartRequest) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/restart", request)
}

func (client *Client) DeleteRestart(ctx context.Context, baseURL string) (Response[string], error) {
	return sendForText(ctx, client, http.MethodDelete, baseURL, "/restart", nil)
}

func (client *Client) DeleteSwitchover(ctx context.Context, baseURL string) (Response[string], error) {
	return sendForText(ctx, client, http.MethodDelete, baseURL, "/switchover", nil)
}

func (client *Client) PostReinitialize(ctx context.Context, baseURL string, request ReinitializeRequest) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/reinitialize", request)
}

func (client *Client) PostFailover(ctx context.Context, baseURL string, request FailoverRequest) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/failover", request)
}

func (client *Client) PostSwitchover(ctx context.Context, baseURL string, request FailoverRequest) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/switchover", request)
}

func (client *Client) PostCitus(ctx context.Context, baseURL string, event MPPEvent) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/citus", event)
}

func (client *Client) PostMPP(ctx context.Context, baseURL string, event MPPEvent) (Response[string], error) {
	return sendJSONForText(ctx, client, http.MethodPost, baseURL, "/mpp", event)
}

func getJSON[T any](ctx context.Context, client *Client, baseURL, endpoint string, query url.Values, decodeOnSuccess bool) (Response[T], error) {
	wire, err := client.execute(ctx, http.MethodGet, baseURL, endpoint, query, nil)
	if err != nil {
		return Response[T]{StatusCode: wire.status, Header: wire.header, Raw: wire.raw}, err
	}
	decode := decodeOnSuccess && wire.status >= 200 && wire.status < 300
	return jsonResponse[T](wire, http.MethodGet, endpoint, decode)
}

func sendJSONForJSON[T any](ctx context.Context, client *Client, method, baseURL, endpoint string, request any, decodeOnSuccess bool) (Response[T], error) {
	body, err := marshalRequest(method, endpoint, request)
	if err != nil {
		return Response[T]{}, err
	}
	wire, err := client.execute(ctx, method, baseURL, endpoint, nil, body)
	if err != nil {
		return Response[T]{StatusCode: wire.status, Header: wire.header, Raw: wire.raw}, err
	}
	decode := decodeOnSuccess && wire.status >= 200 && wire.status < 300
	return jsonResponse[T](wire, method, endpoint, decode)
}

func sendJSONForText(ctx context.Context, client *Client, method, baseURL, endpoint string, request any) (Response[string], error) {
	body, err := marshalRequest(method, endpoint, request)
	if err != nil {
		return Response[string]{}, err
	}
	return sendForText(ctx, client, method, baseURL, endpoint, body)
}

func sendForText(ctx context.Context, client *Client, method, baseURL, endpoint string, body []byte) (Response[string], error) {
	wire, err := client.execute(ctx, method, baseURL, endpoint, nil, body)
	return textResponse(wire), err
}
