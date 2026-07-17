package patroni

import "fmt"

type ErrorKind string

const (
	ErrorRequest        ErrorKind = "REQUEST"
	ErrorAuthentication ErrorKind = "AUTHENTICATION"
	ErrorTransport      ErrorKind = "TRANSPORT"
	ErrorResponseBody   ErrorKind = "RESPONSE_BODY"
	ErrorDecode         ErrorKind = "DECODE"
)

type DeliveryState string

const (
	DeliveryNotSent          DeliveryState = "NOT_SENT"
	DeliveryMaybeSent        DeliveryState = "MAYBE_SENT"
	DeliveryResponseReceived DeliveryState = "RESPONSE_RECEIVED"
)

// Error intentionally omits the base URL, request body, response body, and
// underlying error text from formatting. Unwrap is available for explicit
// diagnostics and errors.Is/errors.As.
type Error struct {
	Kind       ErrorKind
	Method     string
	Endpoint   string
	Delivery   DeliveryState
	StatusCode int
	cause      error
}

func newError(kind ErrorKind, method, endpoint string, delivery DeliveryState, status int, cause error) *Error {
	return &Error{Kind: kind, Method: method, Endpoint: endpoint, Delivery: delivery, StatusCode: status, cause: cause}
}

func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	message := fmt.Sprintf("patroni %s %s: %s", err.Method, err.Endpoint, err.Kind)
	if err.StatusCode != 0 {
		message += fmt.Sprintf(" status=%d", err.StatusCode)
	}
	if err.Delivery != "" {
		message += " delivery=" + string(err.Delivery)
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

func (err *Error) AmbiguousWrite() bool {
	return err != nil && err.Delivery == DeliveryMaybeSent
}
