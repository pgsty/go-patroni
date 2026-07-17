package postgres

import (
	"context"
	"fmt"
)

type Limits struct {
	MaxRows   int64 `json:"maxRows"`
	MaxBytes  int64 `json:"maxBytes"`
	Unlimited bool  `json:"unlimited"`
}

type QueryRequest struct {
	SQL    string `json:"-"`
	Limits Limits `json:"limits"`
}

// RecoveryExpectation controls the optional pg_is_in_recovery preflight used
// by patronictl-compatible role selection. The preflight and user SQL execute
// on the same one-shot connection, and user SQL is never sent on mismatch.
type RecoveryExpectation string

const (
	RecoveryAny     RecoveryExpectation = "ANY"
	RecoveryPrimary RecoveryExpectation = "PRIMARY"
	RecoveryStandby RecoveryExpectation = "STANDBY"
)

func (expectation RecoveryExpectation) valid() bool {
	return expectation == "" || expectation == RecoveryAny || expectation == RecoveryPrimary || expectation == RecoveryStandby
}

func (request QueryRequest) String() string {
	return fmt.Sprintf("postgres.QueryRequest{sql:[REDACTED],maxRows:%d,maxBytes:%d,unlimited:%t}",
		request.Limits.MaxRows, request.Limits.MaxBytes, request.Limits.Unlimited)
}

func (request QueryRequest) GoString() string { return request.String() }

type Column struct {
	Name                 string `json:"name"`
	TableOID             uint32 `json:"tableOID"`
	TableAttributeNumber uint16 `json:"tableAttributeNumber"`
	DataTypeOID          uint32 `json:"dataTypeOID"`
	DataTypeSize         int16  `json:"dataTypeSize"`
	TypeModifier         int32  `json:"typeModifier"`
	Format               int16  `json:"format"`
}

type Cell struct {
	Null  bool   `json:"null"`
	Text  string `json:"text,omitempty"`
	Bytes int64  `json:"bytes"`
}

type Row []Cell

type CommandTag struct {
	Text         string `json:"text"`
	RowsAffected int64  `json:"rowsAffected"`
}

type ResultInfo struct {
	Index   int      `json:"index"`
	Columns []Column `json:"columns"`
}

type ResultSummary struct {
	Index         int      `json:"index"`
	Columns       []Column `json:"columns"`
	CommandTag    string   `json:"commandTag"`
	RowsAffected  int64    `json:"rowsAffected"`
	ObservedRows  int64    `json:"observedRows"`
	EmittedRows   int64    `json:"emittedRows"`
	ObservedBytes int64    `json:"observedBytes"`
	EmittedBytes  int64    `json:"emittedBytes"`
	Truncated     bool     `json:"truncated"`
}

type Summary struct {
	Results       []ResultSummary `json:"results"`
	ObservedRows  int64           `json:"observedRows"`
	EmittedRows   int64           `json:"emittedRows"`
	ObservedBytes int64           `json:"observedBytes"`
	EmittedBytes  int64           `json:"emittedBytes"`
	Truncated     bool            `json:"truncated"`
}

type ResultSet struct {
	Index        int      `json:"index"`
	Columns      []Column `json:"columns"`
	Rows         []Row    `json:"rows"`
	CommandTag   string   `json:"commandTag"`
	RowsAffected int64    `json:"rowsAffected"`
	Truncated    bool     `json:"truncated"`
}

func (result ResultSet) String() string {
	return fmt.Sprintf("postgres.ResultSet{index:%d,columns:%d,rows:%d,commandTag:%q,truncated:%t}",
		result.Index, len(result.Columns), len(result.Rows), result.CommandTag, result.Truncated)
}

func (result ResultSet) GoString() string { return result.String() }

type QueryResult struct {
	Sets    []ResultSet `json:"sets"`
	Summary Summary     `json:"summary"`
}

func (result QueryResult) String() string {
	return fmt.Sprintf("postgres.QueryResult{sets:%d,rows:%d,bytes:%d,truncated:%t,data:[REDACTED]}",
		len(result.Sets), result.Summary.EmittedRows, result.Summary.EmittedBytes, result.Summary.Truncated)
}

func (result QueryResult) GoString() string { return result.String() }

// Sink receives query results synchronously. Implementations that perform I/O
// must honor ctx; returning an error cancels the active query and closes its
// one-shot connection.
type Sink interface {
	BeginResult(context.Context, ResultInfo) error
	WriteRow(context.Context, int, Row) error
	EndResult(context.Context, ResultSummary) error
}
