package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	defaultQueryTimeout = 30 * time.Second
	defaultCloseTimeout = 5 * time.Second
	defaultMaxRows      = int64(10_000)
	defaultMaxBytes     = int64(16 << 20)
)

type ClientOptions struct {
	Connector     Connector
	Logger        *slog.Logger
	Timeout       time.Duration
	CloseTimeout  time.Duration
	DefaultLimits Limits
}

type Client struct {
	connector     Connector
	timeout       time.Duration
	closeTimeout  time.Duration
	defaultLimits Limits
	logger        *slog.Logger
}

func NewClient(options ClientOptions) (*Client, error) {
	connector := options.Connector
	if connector == nil {
		connector = nativeConnector{}
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultQueryTimeout
	}
	closeTimeout := options.CloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = defaultCloseTimeout
	}
	limits := options.DefaultLimits
	if limits.Unlimited {
		limits.MaxRows, limits.MaxBytes = 0, 0
	} else {
		if limits.MaxRows < 0 || limits.MaxBytes < 0 {
			return nil, configurationError("client", "default limits must not be negative")
		}
		if limits.MaxRows == 0 {
			limits.MaxRows = defaultMaxRows
		}
		if limits.MaxBytes == 0 {
			limits.MaxBytes = defaultMaxBytes
		}
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Client{connector: connector, timeout: timeout, closeTimeout: closeTimeout, defaultLimits: limits, logger: logger}, nil
}

func (client *Client) String() string {
	if client == nil {
		return "postgres.Client<nil>"
	}
	return fmt.Sprintf("postgres.Client{timeout:%s,closeTimeout:%s,maxRows:%d,maxBytes:%d,unlimited:%t}",
		client.timeout, client.closeTimeout, client.defaultLimits.MaxRows, client.defaultLimits.MaxBytes,
		client.defaultLimits.Unlimited)
}

func (client *Client) GoString() string { return client.String() }

func (client *Client) Query(ctx context.Context, connection ConnectionOptions, request QueryRequest) (QueryResult, error) {
	return client.QueryChecked(ctx, connection, RecoveryAny, request)
}

func (client *Client) QueryChecked(
	ctx context.Context,
	connection ConnectionOptions,
	expectation RecoveryExpectation,
	request QueryRequest,
) (QueryResult, error) {
	collector := &resultCollector{sets: make([]ResultSet, 0)}
	summary, err := client.StreamChecked(ctx, connection, expectation, request, collector)
	return QueryResult{Sets: collector.sets, Summary: summary}, err
}

func (client *Client) Stream(
	ctx context.Context,
	connectionOptions ConnectionOptions,
	request QueryRequest,
	sink Sink,
) (summary Summary, returnedError error) {
	return client.StreamChecked(ctx, connectionOptions, RecoveryAny, request, sink)
}

func (client *Client) StreamChecked(
	ctx context.Context,
	connectionOptions ConnectionOptions,
	expectation RecoveryExpectation,
	request QueryRequest,
	sink Sink,
) (summary Summary, returnedError error) {
	started := time.Now()
	if client != nil && client.logger != nil {
		defer func() { client.logQuery(ctx, started, expectation, summary, returnedError) }()
	}
	summary.Results = make([]ResultSummary, 0)
	if client == nil || client.connector == nil {
		return summary, configurationError("query", "client is nil")
	}
	if ctx == nil {
		return summary, configurationError("query", "context is nil")
	}
	if err := ctx.Err(); err != nil {
		return summary, newError(ErrorTransport, "query", err)
	}
	if sink == nil {
		return summary, configurationError("query", "sink is nil")
	}
	if !expectation.valid() {
		return summary, configurationError("role-check", "unknown recovery expectation")
	}
	limits, err := client.resolveLimits(request.Limits)
	if err != nil {
		return summary, err
	}
	operationContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	configuration, err := connectionOptions.connectConfig()
	if err != nil {
		return summary, err
	}
	if err := operationContext.Err(); err != nil {
		return summary, newError(ErrorTransport, "connect", err)
	}
	connection, err := client.connector.Connect(operationContext, configuration)
	if err != nil {
		return summary, newError(ErrorConnect, "connect", err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), client.closeTimeout)
		defer closeCancel()
		if err := connection.Close(closeContext); err != nil && returnedError == nil {
			returnedError = newError(ErrorTransport, "close", err)
		}
	}()

	streamContext, streamCancel := context.WithCancel(operationContext)
	defer streamCancel()
	if expectation != "" && expectation != RecoveryAny {
		inRecovery, err := checkRecoveryState(streamContext, connection)
		if err != nil {
			return summary, err
		}
		matches := expectation == RecoveryStandby && inRecovery || expectation == RecoveryPrimary && !inRecovery
		if !matches {
			return summary, newError(ErrorRoleMismatch, "role-check", errors.New("PostgreSQL recovery state does not match requested role"))
		}
	}
	reader := connection.Execute(streamContext, request.SQL)
	if reader == nil {
		return summary, newError(ErrorInvariant, "execute", errors.New("connector returned a nil result stream"))
	}
	for reader.NextResult() {
		rows := reader.ResultReader()
		if rows == nil {
			streamCancel()
			_ = reader.Close()
			return summary, newError(ErrorInvariant, "execute", errors.New("connector returned nil rows"))
		}
		columns := rows.Columns()
		result := ResultSummary{Index: len(summary.Results), Columns: append([]Column{}, columns...)}
		if err := sink.BeginResult(streamContext, ResultInfo{Index: result.Index, Columns: append([]Column{}, columns...)}); err != nil {
			streamCancel()
			_ = reader.Close()
			return summary, newError(ErrorSink, "begin-result", err)
		}
		for rows.NextRow() {
			values := rows.Values()
			if len(values) != len(columns) {
				streamCancel()
				_ = reader.Close()
				return summary, newError(ErrorInvariant, "row", errors.New("row value count differs from column count"))
			}
			row, bytes := copyRow(values)
			result.ObservedRows++
			result.ObservedBytes += bytes
			summary.ObservedRows++
			summary.ObservedBytes += bytes
			if canEmit(summary, bytes, limits) {
				if err := sink.WriteRow(streamContext, result.Index, row); err != nil {
					streamCancel()
					_ = reader.Close()
					return summary, newError(ErrorSink, "row", err)
				}
				result.EmittedRows++
				result.EmittedBytes += bytes
				summary.EmittedRows++
				summary.EmittedBytes += bytes
			} else {
				result.Truncated = true
				summary.Truncated = true
			}
		}
		tag, err := rows.Close()
		if err != nil {
			_ = reader.Close()
			return summary, newError(ErrorDatabase, "query", err)
		}
		result.CommandTag = tag.Text
		result.RowsAffected = tag.RowsAffected
		summary.Results = append(summary.Results, result)
		if err := sink.EndResult(streamContext, result); err != nil {
			streamCancel()
			_ = reader.Close()
			return summary, newError(ErrorSink, "end-result", err)
		}
	}
	if err := reader.Close(); err != nil {
		return summary, newError(ErrorDatabase, "query", err)
	}
	return summary, nil
}

func (client *Client) logQuery(
	ctx context.Context,
	started time.Time,
	expectation RecoveryExpectation,
	summary Summary,
	err error,
) {
	if client == nil || client.logger == nil {
		return
	}
	stage := "complete"
	errorKind := ErrorKind("")
	sqlState := ""
	var typed *Error
	if errors.As(err, &typed) {
		stage, errorKind, sqlState = typed.Stage, typed.Kind, typed.SQLState
	}
	attributes := []any{
		"stage", stage,
		"error_kind", errorKind,
		"sql_state", sqlState,
		"recovery_expectation", expectation,
		"result_count", len(summary.Results),
		"observed_rows", summary.ObservedRows,
		"emitted_rows", summary.EmittedRows,
		"truncated", summary.Truncated,
		"duration_ms", time.Since(started).Milliseconds(),
	}
	if ctx == nil {
		client.logger.Debug("postgres query", attributes...)
		return
	}
	client.logger.DebugContext(ctx, "postgres query", attributes...)
}

func checkRecoveryState(ctx context.Context, connection Connection) (bool, error) {
	reader := connection.Execute(ctx, "SELECT pg_catalog.pg_is_in_recovery()")
	if reader == nil {
		return false, newError(ErrorInvariant, "role-check", errors.New("connector returned a nil result stream"))
	}
	if !reader.NextResult() {
		if err := reader.Close(); err != nil {
			return false, newError(ErrorDatabase, "role-check", err)
		}
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned no result"))
	}
	rows := reader.ResultReader()
	if rows == nil {
		_ = reader.Close()
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned nil rows"))
	}
	if len(rows.Columns()) != 1 || !rows.NextRow() {
		_ = reader.Close()
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned an invalid shape"))
	}
	values := rows.Values()
	if len(values) != 1 || values[0] == nil || rows.NextRow() {
		_ = reader.Close()
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned an invalid value"))
	}
	if _, err := rows.Close(); err != nil {
		_ = reader.Close()
		return false, newError(ErrorDatabase, "role-check", err)
	}
	if reader.NextResult() {
		_ = reader.Close()
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned multiple results"))
	}
	if err := reader.Close(); err != nil {
		return false, newError(ErrorDatabase, "role-check", err)
	}
	switch string(values[0]) {
	case "t", "true", "1", "on":
		return true, nil
	case "f", "false", "0", "off":
		return false, nil
	default:
		return false, newError(ErrorInvariant, "role-check", errors.New("recovery check returned a non-boolean value"))
	}
}

func (client *Client) resolveLimits(request Limits) (Limits, error) {
	if request.MaxRows < 0 || request.MaxBytes < 0 {
		return Limits{}, configurationError("limits", "query limits must not be negative")
	}
	if request.Unlimited {
		return Limits{Unlimited: true}, nil
	}
	if request.MaxRows == 0 && request.MaxBytes == 0 && client.defaultLimits.Unlimited {
		return Limits{Unlimited: true}, nil
	}
	if request.MaxRows == 0 {
		request.MaxRows = client.defaultLimits.MaxRows
	}
	if request.MaxBytes == 0 {
		request.MaxBytes = client.defaultLimits.MaxBytes
	}
	return request, nil
}

func canEmit(summary Summary, rowBytes int64, limits Limits) bool {
	if limits.Unlimited {
		return true
	}
	// Truncation is always a prefix operation. Continue draining the server so
	// command completion and connection state are known, but never emit a later
	// small row after an earlier row exceeded a limit.
	if summary.Truncated {
		return false
	}
	rowsAvailable := limits.MaxRows == 0 || summary.EmittedRows < limits.MaxRows
	bytesAvailable := limits.MaxBytes == 0 ||
		(summary.EmittedBytes <= limits.MaxBytes && rowBytes <= limits.MaxBytes-summary.EmittedBytes)
	return rowsAvailable && bytesAvailable
}

type resultCollector struct{ sets []ResultSet }

func (collector *resultCollector) BeginResult(_ context.Context, info ResultInfo) error {
	collector.sets = append(collector.sets, ResultSet{
		Index: info.Index, Columns: append([]Column{}, info.Columns...), Rows: make([]Row, 0),
	})
	return nil
}

func (collector *resultCollector) WriteRow(_ context.Context, index int, row Row) error {
	if index < 0 || index >= len(collector.sets) {
		return errors.New("result index is outside collector")
	}
	cloned := make(Row, len(row))
	copy(cloned, row)
	collector.sets[index].Rows = append(collector.sets[index].Rows, cloned)
	return nil
}

func (collector *resultCollector) EndResult(_ context.Context, summary ResultSummary) error {
	if summary.Index < 0 || summary.Index >= len(collector.sets) {
		return errors.New("result index is outside collector")
	}
	set := &collector.sets[summary.Index]
	set.CommandTag = summary.CommandTag
	set.RowsAffected = summary.RowsAffected
	set.Truncated = summary.Truncated
	return nil
}

var _ Sink = (*resultCollector)(nil)
