package syncv2

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

func isAdmissionRetry(err error) bool {
	var transport *HTTPError
	if errors.As(err, &transport) {
		return transport.StatusCode == 429 || transport.StatusCode == 502 || transport.StatusCode == 503 || transport.StatusCode == 504
	}
	var api *APIError
	if !errors.As(err, &api) {
		return false
	}
	return api.ErrCode == 8 && (api.ErrMsg == "SYNC_V2_READ_BUSY" || api.ErrMsg == "SYNC_V2_SCAN_BUSY" || api.ErrMsg == "SYNC_V2_BUILD_BUSY") || api.ErrCode == 14 && api.ErrMsg == "SYNC_V2_ADMISSION_UNAVAILABLE"
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 0
}

func waitAdmissionRetry(ctx context.Context, cause error, attempt int) error {
	delay := time.Second << min(attempt, 3)
	var transport *HTTPError
	if errors.As(cause, &transport) {
		delay = max(delay, transport.RetryAfter)
	}
	delay += time.Duration(rand.Int63n(int64(250 * time.Millisecond)))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
