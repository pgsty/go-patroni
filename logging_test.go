package patroni_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pgsty/go-patroni"
)

type loggingRoundTripper struct{ err error }

func (transport loggingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, transport.err
}

func debugJSONLogger(buffer *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestInjectedLoggerRecordsOnlySafePatroniTransportMetadata(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_PATRONI_LOG_SECRET__"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		if !strings.Contains(string(body), marker) || !strings.Contains(request.URL.Path, marker) {
			t.Errorf("test did not place protected values on the wire: path=%q body=%q", request.URL.Path, body)
		}
		writer.Header().Set("X-Protected", marker)
		_, _ = io.WriteString(writer, `{"echo":"`+marker+`"}`)
	}))
	defer server.Close()

	var output bytes.Buffer
	client, err := patroni.NewClient(patroni.ClientOptions{
		Logger: debugJSONLogger(&output), Authorizer: patroni.NewBasicAuth("operator", marker),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.PatchConfig(context.Background(), server.URL+"/base-"+marker, patroni.DynamicConfig{
		"password": marker, "dsn": "postgres://operator:" + marker + "@db.example/app",
	})
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(response.Raw), marker) {
		t.Fatalf("Patroni logging fixture call failed: response=%#v err=%v", response, err)
	}
	if strings.Contains(output.String(), marker) || strings.Contains(output.String(), "password") || strings.Contains(output.String(), "dsn") || strings.Contains(output.String(), "X-Protected") {
		t.Fatalf("Patroni transport log leaked URL/body/header/response/auth data: %s", output.String())
	}
	record := decodeLogRecord(t, output.String())
	if record["msg"] != "patroni http exchange" || record["method"] != http.MethodPatch || record["endpoint"] != "/config" ||
		record["status_code"] != float64(http.StatusOK) || record["delivery_state"] != string(patroni.DeliveryResponseReceived) || record["error_kind"] != "" {
		t.Fatalf("safe Patroni transport fields mismatch: %#v", record)
	}
	if _, ok := record["duration_ms"].(float64); !ok {
		t.Fatalf("Patroni transport duration is missing or not numeric: %#v", record)
	}
}

func TestInjectedLoggerNeverSerializesPatroniValidationOrTransportCauses(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_PATRONI_LOG_CAUSE__"
	for _, test := range []struct {
		name   string
		client patroni.ClientOptions
		base   string
		kind   patroni.ErrorKind
	}{
		{
			name: "base-url-userinfo", base: "https://operator:" + marker + "@example.invalid/base", kind: patroni.ErrorRequest,
		},
		{
			name: "transport-cause", base: "http://example.invalid/base-" + marker,
			client: patroni.ClientOptions{Transport: loggingRoundTripper{err: errors.New("transport " + marker)}},
			kind:   patroni.ErrorTransport,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			test.client.Logger = debugJSONLogger(&output)
			client, err := patroni.NewClient(test.client)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.GetPatroni(context.Background(), test.base)
			var typed *patroni.Error
			if !errors.As(err, &typed) || typed.Kind != test.kind {
				t.Fatalf("Patroni error kind mismatch: %#v", err)
			}
			if strings.Contains(output.String(), marker) || strings.Contains(output.String(), "operator") || strings.Contains(output.String(), "example.invalid") {
				t.Fatalf("Patroni failure log leaked URL/cause data: %s", output.String())
			}
			record := decodeLogRecord(t, output.String())
			if record["endpoint"] != "/patroni" || record["error_kind"] != string(test.kind) {
				t.Fatalf("safe Patroni failure fields mismatch: %#v", record)
			}
		})
	}
}

func decodeLogRecord(t *testing.T, line string) map[string]any {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &record); err != nil {
		t.Fatalf("decode JSON log %q: %v", line, err)
	}
	return record
}
