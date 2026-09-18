package syncv2

import (
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
)

func TestRecoveryRequiresMatchingCodeAndReason(t *testing.T) {
	tests := []struct {
		code   int
		reason string
		want   Recovery
	}{
		{8, "SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH", ReduceBatch},
		{8, "SYNC_V2_READ_BYTES_REDUCE_BATCH", ReduceBatch},
		{8, "SYNC_V2_READ_CONCURRENCY", Retry},
		{8, "SYNC_V2_SNAPSHOT_QUOTA", Retry},
		{8, "SYNC_V2_CHAT_BATCH_BUSY", Retry},
		{9, "SYNC_V2_SNAPSHOT_EXPIRED", RebuildSnapshot},
		{9, "SYNC_V2_DIRECTORY_CHANGED", RebuildSnapshot},
		{3, "SYNC_V2_INVALID_DIRECTORY_TOKEN", Stop},
		{12, "SYNC_V2_DIRECTORY_TOKEN_GUARD_UNSUPPORTED", Renegotiate},
		{3, "SYNC_V2_DIRECTORY_CHANGED", Stop},
		{9, "SYNC_V2_CHANGE_CURSOR_EXPIRED", RebuildSnapshot},
		{9, "SYNC_V2_CHANGE_GENERATION_CHANGED", RebuildSnapshot},
		{3, "SYNC_V2_CHANGE_CURSOR_EXPIRED", Stop},
		{500, "ServerInternalError", Stop},
		{9, "SYNC_V2_POLICY_CHANGED", RebuildSnapshot},
		{9, "SYNC_V2_INVALID_STORED_ID", Stop},
		{10, "SYNC_V2_STATE_CHANGED", RefreshState},
		{10, "SYNC_V2_RESERVATION_EXPIRED", RebuildSnapshot},
		{12, "SYNC_V2_DOWNSTREAM_VERSION", Renegotiate},
		{14, "SYNC_V2_REDIS_UNAVAILABLE", Retry},
		{7, "SYNC_V2_CONVERSATION_UNAVAILABLE", Unavailable},
		{3, "SYNC_V2_STATE_CHANGED", Stop},
		{12, "other", Stop},
	}
	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			if got := RecoveryFor(&APIError{ErrCode: tt.code, ErrMsg: tt.reason}); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
	for _, err := range []error{errors.New("empty response"), &HTTPError{StatusCode: 404}, &HTTPError{StatusCode: 500}} {
		if got := RecoveryFor(err); got != Stop {
			t.Fatalf("generic failure changes protocol: %v", got)
		}
	}
}

func TestRecoveryRetriesWrappedConnectionReset(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://fixture.invalid/conversation/sync_capabilities", Err: &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}}
	if got := RecoveryFor(err); got != Retry {
		t.Fatalf("connection reset: got %v want Retry", got)
	}
	if got := RecoveryFor(errors.New("connection reset by peer")); got != Stop {
		t.Fatalf("untyped error must not be retried: %v", got)
	}
}

func TestRecoveryRetriesHTTPTransportUnexpectedEOF(t *testing.T) {
	for _, cause := range []error{io.EOF, io.ErrUnexpectedEOF} {
		transportError := &url.Error{Op: "Post", URL: "https://fixture.invalid/conversation/sync_capabilities", Err: cause}
		if got := RecoveryFor(transportError); got != Retry {
			t.Errorf("HTTP transport %v: got %v want Retry", cause, got)
		}
	}
	for _, err := range []error{io.ErrUnexpectedEOF, io.EOF, errors.New("unexpected EOF")} {
		if got := RecoveryFor(err); got != Stop {
			t.Fatalf("non-transport failure must not become retry: %v -> %v", err, got)
		}
	}
}

func TestRecoveryRetainsModeOnTemporaryFailure(t *testing.T) {
	for _, err := range []error{&HTTPError{StatusCode: 429}, &HTTPError{StatusCode: 503}, ErrCatchupPending} {
		if got := RecoveryFor(err); got != Retry {
			t.Fatalf("temporary failure selected %v", got)
		}
	}
}
