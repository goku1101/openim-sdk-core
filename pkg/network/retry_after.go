package network

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/tools/errs"
)

// RetryAfterError retains the SDK error contract and the HTTP admission deadline.
type RetryAfterError struct {
	errs.CodeError
	RetryAt time.Time
}

func (e *RetryAfterError) Unwrap() error { return e.CodeError }

func withRetryAfter(cause error, response *http.Response, now time.Time) error {
	var code errs.CodeError
	if !errors.As(cause, &code) {
		code = sdkerrs.New(sdkerrs.ErrNetwork.Code(), sdkerrs.ErrNetwork.Msg(), response.Status)
	}
	var until time.Time
	value := response.Header.Get("Retry-After")
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		until = now.Add(time.Duration(seconds) * time.Second)
	} else if date, err := http.ParseTime(value); err == nil && date.After(now) {
		until = date
	}
	return &RetryAfterError{CodeError: code, RetryAt: until}
}
