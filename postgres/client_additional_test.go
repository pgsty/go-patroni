package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type failingSink struct {
	stage string
	cause error
}

func (sink failingSink) BeginResult(context.Context, ResultInfo) error {
	if sink.stage == "begin" {
		return sink.cause
	}
	return nil
}

func (sink failingSink) WriteRow(context.Context, int, Row) error {
	if sink.stage == "row" {
		return sink.cause
	}
	return nil
}

func (sink failingSink) EndResult(context.Context, ResultSummary) error {
	if sink.stage == "end" {
		return sink.cause
	}
	return nil
}

type blockingConnector struct{}

func (blockingConnector) Connect(ctx context.Context, _ *pgx.ConnConfig) (Connection, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestByteLimitAlwaysProducesAResultPrefix(t *testing.T) {
	connector := &fakeConnector{queue: [][]fakeResult{{{
		columns: []Column{{Name: "value"}},
		rows:    [][][]byte{{[]byte("oversized")}, {[]byte("x")}},
		tag:     CommandTag{Text: "SELECT 2", RowsAffected: 2},
	}}}}
	client, _ := NewClient(ClientOptions{Connector: connector})
	result, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{
		SQL: "select values", Limits: Limits{MaxRows: 10, MaxBytes: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sets) != 1 || len(result.Sets[0].Rows) != 0 || result.Summary.ObservedRows != 2 ||
		result.Summary.EmittedRows != 0 || !result.Summary.Truncated || result.Sets[0].CommandTag != "SELECT 2" {
		t.Fatalf("byte-limited query was not a fully drained prefix: %#v", result)
	}
}

func TestUnlimitedDefaultsAndOneDimensionalOverride(t *testing.T) {
	queue := func() []fakeResult {
		return []fakeResult{{
			columns: []Column{{Name: "value"}},
			rows:    [][][]byte{{[]byte("12345")}, {[]byte("67890")}},
			tag:     CommandTag{Text: "SELECT 2", RowsAffected: 2},
		}}
	}
	connector := &fakeConnector{queue: [][]fakeResult{queue(), queue()}}
	client, _ := NewClient(ClientOptions{Connector: connector, DefaultLimits: Limits{Unlimited: true}})
	unlimited, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: "select values"})
	if err != nil || unlimited.Summary.EmittedRows != 2 || unlimited.Summary.Truncated {
		t.Fatalf("unlimited default mismatch: result=%#v err=%v", unlimited, err)
	}
	rowLimited, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{
		SQL: "select values", Limits: Limits{MaxRows: 1},
	})
	if err != nil || rowLimited.Summary.EmittedRows != 1 || !rowLimited.Summary.Truncated {
		t.Fatalf("one-dimensional override mismatch: result=%#v err=%v", rowLimited, err)
	}
}

func TestSinkFailuresCancelTheQueryAndCloseTheConnection(t *testing.T) {
	const protected = "__BOAR_TEST_ONLY_POSTGRES_SINK_DETAIL__"
	for _, stage := range []string{"begin", "row", "end"} {
		t.Run(stage, func(t *testing.T) {
			connector := &fakeConnector{queue: [][]fakeResult{{{
				columns: []Column{{Name: "value"}}, rows: [][][]byte{{[]byte("one")}},
				tag: CommandTag{Text: "SELECT 1", RowsAffected: 1},
			}}}}
			client, _ := NewClient(ClientOptions{Connector: connector})
			_, err := client.Stream(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable),
				QueryRequest{SQL: "select protected"}, failingSink{stage: stage, cause: errors.New(protected)})
			var typed *Error
			if !errors.As(err, &typed) || typed.Kind != ErrorSink || strings.Contains(err.Error(), protected) {
				t.Fatalf("sink error classification/redaction mismatch: %#v", err)
			}
			if len(connector.connections) != 1 || !connector.connections[0].closed ||
				connector.connections[0].executeContext == nil || !errors.Is(connector.connections[0].executeContext.Err(), context.Canceled) {
				t.Fatalf("sink failure did not cancel and close the query: %#v", connector.connections)
			}
		})
	}
}

func TestCloseAndTimeoutErrorsAreTyped(t *testing.T) {
	closeConnector := &fakeConnector{closeError: errors.New("private close detail")}
	client, _ := NewClient(ClientOptions{Connector: closeConnector})
	_, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: "select 1"})
	var typed *Error
	if !errors.As(err, &typed) || typed.Kind != ErrorTransport || typed.Stage != "close" {
		t.Fatalf("close error classification mismatch: %#v", err)
	}

	timed, _ := NewClient(ClientOptions{Connector: blockingConnector{}, Timeout: 10 * time.Millisecond})
	caller, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = timed.Query(caller, NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: "select 1"})
	if !errors.As(err, &typed) || typed.Kind != ErrorDeadline || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connect timeout classification mismatch: %#v", err)
	}
}

func TestInvalidInputsDoNotConnect(t *testing.T) {
	connector := &fakeConnector{}
	client, _ := NewClient(ClientOptions{Connector: connector})
	tests := []struct {
		name    string
		ctx     context.Context
		request QueryRequest
		sink    Sink
	}{
		{name: "nil context", request: QueryRequest{SQL: "select 1"}, sink: &resultCollector{}},
		{name: "nil sink", ctx: context.Background(), request: QueryRequest{SQL: "select 1"}},
		{name: "negative rows", ctx: context.Background(), request: QueryRequest{SQL: "select 1", Limits: Limits{MaxRows: -1}}, sink: &resultCollector{}},
		{name: "negative bytes", ctx: context.Background(), request: QueryRequest{SQL: "select 1", Limits: Limits{MaxBytes: -1}}, sink: &resultCollector{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Stream(test.ctx, NewConnectionOptions(""), test.request, test.sink)
			var typed *Error
			if !errors.As(err, &typed) || typed.Kind != ErrorConfiguration {
				t.Fatalf("invalid input classification mismatch: %#v", err)
			}
		})
	}
	if len(connector.connections) != 0 {
		t.Fatal("invalid query input opened a connection")
	}
	if _, err := NewClient(ClientOptions{DefaultLimits: Limits{MaxRows: -1}}); err == nil {
		t.Fatal("negative client default limit was accepted")
	}
}

func TestConnectionAndQueryJSONNeverContainConnectionOrSQL(t *testing.T) {
	const protected = "__BOAR_TEST_ONLY_POSTGRES_JSON_DETAIL__"
	options := NewConnectionOptions("postgres://user:password@" + protected + "/database").WithPassword("password")
	options.Host, options.Database, options.Username = protected, protected, protected
	request := QueryRequest{SQL: "select '" + protected + "'"}
	for _, value := range []any{options, request} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), protected) || strings.Contains(string(encoded), "password") {
			t.Fatalf("JSON leaked query/connection detail: %s", encoded)
		}
	}
}

func TestConcurrentOneShotQueriesAreRaceFree(t *testing.T) {
	const count = 32
	queue := make([][]fakeResult, count)
	for index := range queue {
		queue[index] = []fakeResult{{columns: []Column{{Name: "value"}}, rows: [][][]byte{{[]byte("ok")}}, tag: CommandTag{Text: "SELECT 1", RowsAffected: 1}}}
	}
	connector := &fakeConnector{queue: queue}
	client, _ := NewClient(ClientOptions{Connector: connector})
	errorsChannel := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := client.Query(context.Background(), NewConnectionOptions("").WithTLSMode(TLSDisable), QueryRequest{SQL: fmt.Sprintf("select %d", index)})
			if err == nil && (len(result.Sets) != 1 || len(result.Sets[0].Rows) != 1) {
				err = errors.New("unexpected query result")
			}
			errorsChannel <- err
		}(index)
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(connector.connections) != count {
		t.Fatalf("got %d one-shot connections, want %d", len(connector.connections), count)
	}
}
