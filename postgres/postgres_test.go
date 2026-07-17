package postgres

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeConnector struct {
	mutex       sync.Mutex
	connections []*fakeConnection
	queue       [][]fakeResult
	// executionQueue is indexed by connection and then by Execute call. It is
	// used by checked-query tests that must prove role validation happens on
	// the same connection before user SQL is sent.
	executionQueue [][][]fakeResult
	err            error
	closeError     error
	configs        []*pgx.ConnConfig
}

func (connector *fakeConnector) Connect(ctx context.Context, config *pgx.ConnConfig) (Connection, error) {
	connector.mutex.Lock()
	defer connector.mutex.Unlock()
	connector.configs = append(connector.configs, config.Copy())
	if connector.err != nil {
		return nil, connector.err
	}
	var executions [][]fakeResult
	if len(connector.executionQueue) > 0 {
		executions = connector.executionQueue[0]
		connector.executionQueue = connector.executionQueue[1:]
	} else if len(connector.queue) > 0 {
		executions = [][]fakeResult{connector.queue[0]}
		connector.queue = connector.queue[1:]
	}
	connection := &fakeConnection{executions: executions, closeError: connector.closeError}
	connector.connections = append(connector.connections, connection)
	return connection, nil
}

type fakeConnection struct {
	mutex          sync.Mutex
	executions     [][]fakeResult
	executed       []string
	closed         bool
	closeError     error
	executeContext context.Context
}

func (connection *fakeConnection) Execute(ctx context.Context, sql string) MultiResultReader {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.executed = append(connection.executed, sql)
	connection.executeContext = ctx
	var queue []fakeResult
	if len(connection.executions) > 0 {
		queue = connection.executions[0]
		connection.executions = connection.executions[1:]
	}
	return &fakeMultiResult{results: queue}
}

func (connection *fakeConnection) Close(context.Context) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.closed = true
	return connection.closeError
}

type fakeResult struct {
	columns []Column
	rows    [][][]byte
	tag     CommandTag
	err     error
}

type fakeMultiResult struct {
	results []fakeResult
	index   int
	current *fakeRows
	err     error
}

func (reader *fakeMultiResult) NextResult() bool {
	if reader.index >= len(reader.results) {
		return false
	}
	result := reader.results[reader.index]
	reader.index++
	reader.current = &fakeRows{result: result}
	if result.err != nil {
		reader.err = result.err
	}
	return true
}

func (reader *fakeMultiResult) ResultReader() ResultReader { return reader.current }
func (reader *fakeMultiResult) Close() error               { return reader.err }

type fakeRows struct {
	result fakeResult
	index  int
}

func (rows *fakeRows) Columns() []Column { return append([]Column(nil), rows.result.columns...) }
func (rows *fakeRows) NextRow() bool     { return rows.index < len(rows.result.rows) }
func (rows *fakeRows) Values() [][]byte {
	values := rows.result.rows[rows.index]
	rows.index++
	return values
}
func (rows *fakeRows) Close() (CommandTag, error) { return rows.result.tag, rows.result.err }

func TestConnectionOptionsDefaultToVerifiedTLSAndNeverFormatSecrets(t *testing.T) {
	const password = "__BOAR_TEST_ONLY_POSTGRES_OPTIONS_PASSWORD__"
	t.Setenv("PGHOST", "database.example.invalid")
	t.Setenv("PGPORT", "5544")
	t.Setenv("PGUSER", "operator")
	t.Setenv("PGPASSWORD", password)
	t.Setenv("PGDATABASE", "app")
	t.Setenv("PGSSLMODE", "prefer")

	options := NewConnectionOptions("")
	config, err := options.connectConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != "database.example.invalid" || config.Port != 5544 || config.User != "operator" ||
		config.Database != "app" || config.Password != password || config.ConnectTimeout != 5*time.Second {
		t.Fatalf("standard PostgreSQL sources/defaults not applied: %#v", config.Config)
	}
	if config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || config.TLSConfig.ServerName != config.Host {
		t.Fatalf("default PostgreSQL TLS is not verify-full: %#v", config.TLSConfig)
	}
	for _, fallback := range config.Fallbacks {
		if fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify {
			t.Fatalf("default PostgreSQL fallback weakened TLS: %#v", fallback)
		}
	}
	for _, rendered := range []string{options.String(), fmt.Sprintf("%#v", options)} {
		if strings.Contains(rendered, password) || strings.Contains(rendered, "database.example.invalid") {
			t.Fatalf("connection options leaked a secret or endpoint: %s", rendered)
		}
	}

	insecure, err := options.WithTLSMode(TLSInsecure).connectConfig()
	if err != nil || insecure.TLSConfig == nil || !insecure.TLSConfig.InsecureSkipVerify {
		t.Fatalf("explicit insecure TLS mode mismatch: config=%#v err=%v", insecure, err)
	}
	disabled, err := options.WithTLSMode(TLSDisable).connectConfig()
	if err != nil || disabled.TLSConfig != nil {
		t.Fatalf("explicit disabled TLS mode mismatch: config=%#v err=%v", disabled, err)
	}
	sourced, err := options.WithTLSMode(TLSFromSource).connectConfig()
	if err != nil || sourced.TLSConfig == nil || !sourced.TLSConfig.InsecureSkipVerify {
		t.Fatalf("explicit source TLS mode did not preserve PGSSLMODE: config=%#v err=%v", sourced, err)
	}

	custom := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "custom.invalid"}
	configured, err := options.WithTLSConfig(custom).connectConfig()
	if err != nil || configured.TLSConfig == custom || configured.TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("custom TLS config was not cloned: config=%#v err=%v", configured, err)
	}
}

func TestExplicitTargetSelectsMatchingStandardPassfileEntry(t *testing.T) {
	const password = "__BOAR_TEST_ONLY_POSTGRES_OPTIONS_PASSWORD__"
	passfile := filepath.Join(t.TempDir(), "pgpass")
	contents := "environment.invalid:5432:wrong:wrong:wrong-password\n" +
		"explicit.invalid:5544:app:operator:" + password + "\n"
	if err := os.WriteFile(passfile, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PGHOST", "environment.invalid")
	t.Setenv("PGPORT", "5432")
	t.Setenv("PGDATABASE", "wrong")
	t.Setenv("PGUSER", "wrong")
	t.Setenv("PGPASSWORD", "")
	t.Setenv("PGPASSFILE", passfile)
	t.Setenv("PGSSLMODE", "verify-full")

	options := NewConnectionOptions("")
	options.Host = "explicit.invalid"
	options.Port = 5544
	options.Database = "app"
	options.Username = "operator"
	configuration, err := options.connectConfig()
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Host != options.Host || configuration.Port != options.Port ||
		configuration.Database != options.Database || configuration.User != options.Username || configuration.Password != password {
		t.Fatal("explicit PostgreSQL target did not select its matching standard passfile entry")
	}
	if configuration.TLSConfig == nil || configuration.TLSConfig.ServerName != options.Host ||
		configuration.TLSConfig.InsecureSkipVerify {
		t.Fatal("explicit PostgreSQL target did not preserve verify-full TLS")
	}
}

func TestQueryCollectsMultipleResultsCommandTagsAndTruncation(t *testing.T) {
	connector := &fakeConnector{queue: [][]fakeResult{{
		{
			columns: []Column{{Name: "id", DataTypeOID: 23}, {Name: "value", DataTypeOID: 25}},
			rows:    [][][]byte{{[]byte("1"), []byte("alpha")}, {[]byte("2"), nil}, {[]byte("3"), []byte("gamma")}},
			tag:     CommandTag{Text: "SELECT 3", RowsAffected: 3},
		},
		{tag: CommandTag{Text: "UPDATE 2", RowsAffected: 2}},
	}}}
	client, err := NewClient(ClientOptions{Connector: connector, DefaultLimits: Limits{MaxRows: 2, MaxBytes: 64}})
	if err != nil {
		t.Fatal(err)
	}
	request := QueryRequest{SQL: "select protected_sql_text", Limits: Limits{MaxRows: 2, MaxBytes: 64}}
	result, err := client.Query(context.Background(), NewConnectionOptions("postgres://node.invalid/app").WithTLSMode(TLSDisable), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sets) != 2 || len(result.Sets[0].Rows) != 2 || result.Sets[0].Rows[1][1].Null != true ||
		result.Sets[1].CommandTag != "UPDATE 2" || result.Sets[1].RowsAffected != 2 {
		t.Fatalf("multi-result projection mismatch: %#v", result)
	}
	if !result.Summary.Truncated || result.Summary.ObservedRows != 3 || result.Summary.EmittedRows != 2 ||
		!result.Sets[0].Truncated || result.Sets[0].Columns[0].DataTypeOID != 23 {
		t.Fatalf("query limits/metadata mismatch: %#v", result)
	}
	if len(connector.connections) != 1 || !connector.connections[0].closed || len(connector.connections[0].executed) != 1 {
		t.Fatalf("query did not use and close exactly one connection: %#v", connector.connections)
	}
	for _, rendered := range []string{request.String(), fmt.Sprintf("%#v", request), client.String()} {
		if strings.Contains(rendered, "protected_sql_text") {
			t.Fatalf("query/client formatting leaked SQL: %s", rendered)
		}
	}
}

func TestCheckedQueryVerifiesRecoveryOnSameConnectionBeforeUserSQL(t *testing.T) {
	queryResult := fakeResult{
		columns: []Column{{Name: "value"}}, rows: [][][]byte{{[]byte("ok")}},
		tag: CommandTag{Text: "SELECT 1", RowsAffected: 1},
	}
	for _, test := range []struct {
		name        string
		recovery    string
		expectation RecoveryExpectation
		wantError   bool
		wantQueries int
	}{
		{name: "primary accepted", recovery: "f", expectation: RecoveryPrimary, wantQueries: 2},
		{name: "standby accepted", recovery: "t", expectation: RecoveryStandby, wantQueries: 2},
		{name: "primary rejected", recovery: "t", expectation: RecoveryPrimary, wantError: true, wantQueries: 1},
		{name: "standby rejected", recovery: "f", expectation: RecoveryStandby, wantError: true, wantQueries: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			roleResult := fakeResult{
				columns: []Column{{Name: "pg_is_in_recovery"}}, rows: [][][]byte{{[]byte(test.recovery)}},
				tag: CommandTag{Text: "SELECT 1", RowsAffected: 1},
			}
			connector := &fakeConnector{executionQueue: [][][]fakeResult{{{roleResult}, {queryResult}}}}
			client, _ := NewClient(ClientOptions{Connector: connector})
			result, err := client.QueryChecked(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable),
				test.expectation, QueryRequest{SQL: "select protected_user_sql"})
			if test.wantError {
				var typed *Error
				if !errors.As(err, &typed) || typed.Kind != ErrorRoleMismatch || len(result.Sets) != 0 {
					t.Fatalf("role mismatch = result=%#v err=%#v", result, err)
				}
			} else if err != nil || len(result.Sets) != 1 {
				t.Fatalf("checked query = result=%#v err=%v", result, err)
			}
			if len(connector.connections) != 1 || !connector.connections[0].closed ||
				len(connector.connections[0].executed) != test.wantQueries {
				t.Fatalf("same-connection ordering = %#v", connector.connections)
			}
			if test.wantError && strings.Contains(strings.Join(connector.connections[0].executed, " "), "protected_user_sql") {
				t.Fatal("user SQL was sent after the role check failed")
			}
		})
	}
}

func TestQueryErrorPreservesSQLStateWithoutSQLOrServerMessage(t *testing.T) {
	const protected = "__BOAR_TEST_ONLY_POSTGRES_SQL_LITERAL__"
	queryError := &pgconn.PgError{Code: "42P01", Severity: "ERROR", Message: "relation " + protected + " does not exist"}
	connector := &fakeConnector{queue: [][]fakeResult{{{err: queryError}}}}
	client, _ := NewClient(ClientOptions{Connector: connector})
	_, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: "select " + protected})
	var typed *Error
	if !errors.As(err, &typed) || typed.Kind != ErrorDatabase || typed.SQLState != "42P01" {
		t.Fatalf("database error classification mismatch: %#v", err)
	}
	if strings.Contains(err.Error(), protected) || strings.Contains(fmt.Sprintf("%#v", err), protected) {
		t.Fatal("safe PostgreSQL error formatting leaked SQL/server detail")
	}
}

func TestCancellationBeforeConnectIsPreciseAndDoesNotCreateConnection(t *testing.T) {
	connector := &fakeConnector{}
	client, _ := NewClient(ClientOptions{Connector: connector})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Query(ctx, NewConnectionOptions(""), QueryRequest{SQL: "select 1"})
	var typed *Error
	if !errors.As(err, &typed) || typed.Kind != ErrorCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("preflight cancellation mismatch: %#v", err)
	}
	if len(connector.connections) != 0 {
		t.Fatal("canceled query opened a connection")
	}
}
