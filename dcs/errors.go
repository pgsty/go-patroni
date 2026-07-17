package dcs

import "fmt"

type ErrorKind string

const (
	ErrorConfiguration  ErrorKind = "CONFIGURATION"
	ErrorAuthentication ErrorKind = "AUTHENTICATION"
	ErrorTransport      ErrorKind = "TRANSPORT"
	ErrorConflict       ErrorKind = "CONFLICT"
	ErrorCompacted      ErrorKind = "COMPACTED"
	ErrorLimit          ErrorKind = "LIMIT"
	ErrorDecode         ErrorKind = "DECODE"
	ErrorCanceled       ErrorKind = "CANCELED"
	ErrorDeadline       ErrorKind = "DEADLINE"
)

type DeliveryState string

const (
	DeliveryUnknown          DeliveryState = ""
	DeliveryNotSent          DeliveryState = "NOT_SENT"
	DeliveryMaybeSent        DeliveryState = "MAYBE_SENT"
	DeliveryResponseReceived DeliveryState = "RESPONSE_RECEIVED"
)

type Error struct {
	Kind      ErrorKind
	Operation string
	Key       string
	Delivery  DeliveryState
	cause     error
}

func NewError(kind ErrorKind, operation, key string, cause error) *Error {
	return &Error{Kind: kind, Operation: operation, Key: key, cause: cause}
}

func NewWriteError(kind ErrorKind, operation, key string, delivery DeliveryState, cause error) *Error {
	return &Error{Kind: kind, Operation: operation, Key: key, Delivery: delivery, cause: cause}
}

func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	message := "dcs " + err.Operation + ": " + string(err.Kind)
	if err.Key != "" {
		message += " key=" + err.Key
	}
	if err.Delivery != "" {
		message += " delivery=" + string(err.Delivery)
	}
	return message
}

func (err *Error) AmbiguousWrite() bool {
	return err != nil && err.Delivery == DeliveryMaybeSent
}

func (err *Error) GoString() string { return err.Error() }

func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

type ConflictError struct {
	Key              string
	ExpectedRevision int64
	ObservedRevision int64
}

func (err *ConflictError) Error() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("dcs compare-and-swap conflict key=%s expected_revision=%d observed_revision=%d",
		err.Key, err.ExpectedRevision, err.ObservedRevision)
}

func (err *ConflictError) GoString() string { return err.Error() }
