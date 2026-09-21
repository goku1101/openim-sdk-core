package network

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/openimsdk/tools/errs"
)

// Preserve the SDK code while retaining the transport cause for read retries.
type transportError struct {
	errs.CodeError
	cause error
}

func (e *transportError) Unwrap() error { return e.cause }

func retryablePageRead(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var transport *transportError
	if !errors.As(err, &transport) {
		return false
	}
	var network net.Error
	return errors.As(transport.cause, &network) && network.Timeout() || errors.Is(transport.cause, io.ErrUnexpectedEOF) || errors.Is(transport.cause, io.EOF)
}

// Retry the same read before inserting or advancing its page; writes are never replayed.
func fetchPageWithRetry[T any](ctx context.Context, api string, req any) (*T, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := CallApi[T](ctx, api, req)
		if err == nil || attempt >= 2 || !retryablePageRead(ctx, err) {
			return resp, err
		}
		timer := time.NewTimer(time.Second << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
