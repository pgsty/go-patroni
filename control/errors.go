// Package control owns BOAR orchestration contracts. Only this package may
// construct final operation outcomes; transports and adapters return evidence.
package control

import (
	"errors"
	"fmt"

	"github.com/pgsty/go-patroni/model"
)

// Category is the stable machine error taxonomy.
type Category string

const (
	CategoryUsage       Category = "USAGE"
	CategoryConfig      Category = "CONFIG"
	CategoryUnsupported Category = "UNSUPPORTED"
	CategoryAuth        Category = "AUTH"
	CategoryTLS         Category = "TLS"
	CategoryNotFound    Category = "NOT_FOUND"
	CategoryConflict    Category = "CONFLICT"
	CategoryUnreachable Category = "UNREACHABLE"
	CategoryFailed      Category = "FAILED"
	CategoryUnknown     Category = "UNKNOWN"
	CategoryInternal    Category = "INTERNAL"
)

var validCategories = map[Category]struct{}{
	CategoryUsage: {}, CategoryConfig: {}, CategoryUnsupported: {}, CategoryAuth: {}, CategoryTLS: {},
	CategoryNotFound: {}, CategoryConflict: {}, CategoryUnreachable: {}, CategoryFailed: {},
	CategoryUnknown: {}, CategoryInternal: {},
}

// Error contains only secret-safe public fields. cause remains available to
// errors.Is/As and sanitized outer logging, but is excluded from serialization.
type Error struct {
	Category  Category     `json:"category" yaml:"category"`
	Operation string       `json:"operation" yaml:"operation"`
	Target    model.Target `json:"target" yaml:"target"`
	Retryable bool         `json:"retryable" yaml:"retryable"`
	Message   string       `json:"message" yaml:"message"`
	Evidence  []Evidence   `json:"evidence" yaml:"evidence"`
	cause     error
}

func NewError(category Category, operation string, target model.Target, retryable bool, message string, cause error, evidence ...Evidence) *Error {
	return &Error{
		Category: category, Operation: operation, Target: target.Normalize(), Retryable: retryable,
		Message: message, Evidence: append([]Evidence{}, evidence...), cause: cause,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) GoString() string { return e.Error() }

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *Error) Validate() error {
	if e == nil {
		return errors.New("control error is nil")
	}
	if _, ok := validCategories[e.Category]; !ok {
		return fmt.Errorf("invalid control error category %q", e.Category)
	}
	if e.Operation == "" {
		return errors.New("control error operation is required")
	}
	if e.Message == "" {
		return errors.New("control error safe message is required")
	}
	for _, evidence := range e.Evidence {
		if err := evidence.Validate(); err != nil {
			return fmt.Errorf("control error evidence: %w", err)
		}
	}
	return nil
}

// ExitCode freezes BOAR's additive process contract.
func ExitCode(category Category) int {
	switch category {
	case CategoryFailed:
		return 1
	case CategoryUsage, CategoryConfig:
		return 2
	case CategoryUnsupported:
		return 3
	case CategoryAuth, CategoryTLS:
		return 4
	case CategoryNotFound:
		return 5
	case CategoryConflict:
		return 6
	case CategoryUnreachable:
		return 7
	case CategoryUnknown:
		return 8
	case CategoryInternal:
		return 9
	default:
		return 9
	}
}
