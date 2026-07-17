package config

import "fmt"

type ErrorKind string

const (
	ErrorSyntax            ErrorKind = "SYNTAX"
	ErrorMultipleDocuments ErrorKind = "MULTIPLE_DOCUMENTS"
	ErrorRootType          ErrorKind = "ROOT_TYPE"
	ErrorContext           ErrorKind = "CONTEXT"
	ErrorUnsupported       ErrorKind = "UNSUPPORTED"
	ErrorNotFound          ErrorKind = "NOT_FOUND"
	ErrorProjection        ErrorKind = "PROJECTION"
)

// Error is a secret-safe typed load/resolve error.
type Error struct {
	Kind    ErrorKind
	Field   string
	Source  string
	Message string
	cause   error
}

func newError(kind ErrorKind, field, source, message string, cause error) *Error {
	return &Error{Kind: kind, Field: field, Source: source, Message: message, cause: cause}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	prefix := "configuration"
	if e.Source != "" {
		prefix += " " + e.Source
	}
	if e.Field != "" {
		prefix += " field " + e.Field
	}
	if e.Message == "" {
		return prefix
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

func (e *Error) GoString() string { return e.Error() }

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// ValidationError reports the exact operation-required field and its source.
type ValidationError struct {
	Operation Operation
	Field     string
	Source    Source
	Reason    string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("configuration field %s is required for %s: %s", e.Field, e.Operation, e.Reason)
}
