package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestInjectedLoggerRecordsOnlySafePostgresQueryMetadata(t *testing.T) {
	const marker = "__BOAR_TEST_ONLY_POSTGRES_LOG_SECRET__"
	connector := &fakeConnector{queue: [][]fakeResult{
		{{
			columns: []Column{{Name: "protected_" + marker}},
			rows:    [][][]byte{{[]byte(marker)}},
			tag:     CommandTag{Text: "SELECT " + marker, RowsAffected: 1},
		}},
		{{err: &pgconn.PgError{Code: "42P01", Message: "server " + marker}}},
	}}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := NewClient(ClientOptions{Connector: connector, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	connection := NewConnectionOptions("postgres://operator:" + marker + "@host-" + marker + ".invalid/app").
		WithTLSMode(TLSDisable).WithPassword(marker)
	result, err := client.Query(context.Background(), connection, QueryRequest{SQL: "select '" + marker + "'"})
	if err != nil || len(result.Sets) != 1 || string(result.Sets[0].Rows[0][0].Text) != marker {
		t.Fatalf("PostgreSQL logging success fixture failed: result=%#v err=%v", result, err)
	}
	_, err = client.Query(context.Background(), connection, QueryRequest{SQL: "select failure_" + marker})
	if err == nil {
		t.Fatal("PostgreSQL logging failure fixture unexpectedly succeeded")
	}
	if strings.Contains(output.String(), marker) || strings.Contains(output.String(), "operator") || strings.Contains(output.String(), "password") || strings.Contains(output.String(), "host-") {
		t.Fatalf("PostgreSQL query log leaked SQL/row/column/tag/connection/password/server data: %s", output.String())
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("PostgreSQL query log count=%d want=2: %s", len(lines), output.String())
	}
	success := decodePostgresLog(t, lines[0])
	if success["msg"] != "postgres query" || success["stage"] != "complete" || success["error_kind"] != "" ||
		success["result_count"] != float64(1) || success["observed_rows"] != float64(1) || success["emitted_rows"] != float64(1) || success["truncated"] != false {
		t.Fatalf("safe PostgreSQL success fields mismatch: %#v", success)
	}
	failure := decodePostgresLog(t, lines[1])
	if failure["stage"] != "query" || failure["error_kind"] != string(ErrorDatabase) || failure["sql_state"] != "42P01" {
		t.Fatalf("safe PostgreSQL failure fields mismatch: %#v", failure)
	}
	if _, ok := failure["duration_ms"].(float64); !ok {
		t.Fatalf("PostgreSQL duration is missing or not numeric: %#v", failure)
	}
}

func TestNilPostgresLoggerUsesDiscardHandler(t *testing.T) {
	connector := &fakeConnector{queue: [][]fakeResult{{{tag: CommandTag{Text: "SELECT 0"}}}}}
	client, err := NewClient(ClientOptions{Connector: connector})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: "select 1"}); err != nil {
		t.Fatal(err)
	}
}

func decodePostgresLog(t *testing.T, line string) map[string]any {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("decode PostgreSQL JSON log %q: %v", line, err)
	}
	return record
}
