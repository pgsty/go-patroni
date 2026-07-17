package cli

import (
	"errors"
	"testing"

	"github.com/pgsty/go-patroni/control"
)

func TestCloseCommandRuntimePromotesCleanupFailure(t *testing.T) {
	cause := errors.New("test-only close failure")
	closed := 0
	runtime := &commandRuntime{close: func() error {
		closed++
		return cause
	}}
	var returnedError error
	closeCommandRuntime(runtime, &returnedError)
	if closed != 1 || !errors.Is(returnedError, cause) {
		t.Fatalf("runtime close result: closed=%d err=%v", closed, returnedError)
	}
	var typed *exitError
	if !errors.As(returnedError, &typed) || typed.category != control.CategoryInternal ||
		typed.code != control.ExitCode(control.CategoryInternal) {
		t.Fatalf("runtime close error classification: %#v", returnedError)
	}
}

func TestCloseCommandRuntimePreservesCommandFailure(t *testing.T) {
	operationError := &exitError{category: control.CategoryUsage, code: control.ExitCode(control.CategoryUsage), message: "test usage"}
	closeError := errors.New("test-only close failure")
	runtime := &commandRuntime{close: func() error { return closeError }}
	returnedError := error(operationError)
	closeCommandRuntime(runtime, &returnedError)
	if !errors.Is(returnedError, operationError) || !errors.Is(returnedError, closeError) {
		t.Fatalf("joined command/close error lost a cause: %v", returnedError)
	}
	if code := exitCode(returnedError); code != operationError.code {
		t.Fatalf("joined close error changed command exit code: got=%d want=%d", code, operationError.code)
	}
}
