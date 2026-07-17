package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type ErrorKind string

const (
	ErrorConfiguration ErrorKind = "CONFIGURATION"
	ErrorConnect       ErrorKind = "CONNECT"
	ErrorDatabase      ErrorKind = "DATABASE"
	ErrorTransport     ErrorKind = "TRANSPORT"
	ErrorSink          ErrorKind = "SINK"
	ErrorCanceled      ErrorKind = "CANCELED"
	ErrorDeadline      ErrorKind = "DEADLINE"
	ErrorLimit         ErrorKind = "LIMIT"
	ErrorRoleMismatch  ErrorKind = "ROLE_MISMATCH"
	ErrorInvariant     ErrorKind = "INVARIANT"
)

// Error deliberately excludes SQL, connection strings, server messages, and
// filesystem paths from default formatting. Unwrap retains the original error
// for callers that explicitly need diagnostics.
type Error struct {
	Kind     ErrorKind
	Stage    string
	SQLState string
	cause    error
}

func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	message := "postgres " + err.Stage + ": " + string(err.Kind)
	if err.SQLState != "" {
		message += " sqlstate=" + err.SQLState
	}
	return message
}

func (err *Error) GoString() string { return err.Error() }

func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func newError(kind ErrorKind, stage string, cause error) *Error {
	err := &Error{Kind: kind, Stage: stage, cause: cause}
	var pgError *pgconn.PgError
	if errors.As(cause, &pgError) {
		err.Kind = ErrorDatabase
		err.SQLState = pgError.Code
	}
	if errors.Is(cause, context.Canceled) {
		err.Kind = ErrorCanceled
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		err.Kind = ErrorDeadline
	}
	return err
}

func configurationError(stage, reason string) *Error {
	return newError(ErrorConfiguration, stage, fmt.Errorf("%s", reason))
}
