package syncv2

import (
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"io"
	"net"
	"net/url"
	"syscall"
)

type Recovery uint8

const (
	Stop Recovery = iota
	Retry
	ReduceBatch
	RebuildSnapshot
	RefreshState
	Renegotiate
	Unavailable
)

// RecoveryFor classifies an action, never performs retries or selects legacy
// mode. In particular a deployment error requires a fresh capability exchange.
func RecoveryFor(err error) Recovery {
	if errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		return RefreshState
	}
	if errors.Is(err, ErrCatchupPending) {
		return Retry
	}
	if errors.Is(err, ErrMetadataSnapshotRequired) {
		return RebuildSnapshot
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && (httpErr.StatusCode == 429 || httpErr.StatusCode == 502 || httpErr.StatusCode == 503 || httpErr.StatusCode == 504) {
		return Retry
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		if errors.Is(err, syscall.ECONNRESET) {
			return Retry
		}
		var transportError *url.Error
		if errors.As(err, &transportError) && (errors.Is(transportError.Err, io.ErrUnexpectedEOF) || errors.Is(transportError.Err, io.EOF)) {
			return Retry
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return Retry
		}
		return Stop
	}
	switch apiErr.ErrCode {
	case 7:
		if apiErr.ErrMsg == "SYNC_V2_CONVERSATION_UNAVAILABLE" {
			return Unavailable
		}
	case 8:
		switch apiErr.ErrMsg {
		case "SYNC_V2_MESSAGE_BATCH_LIMIT", "SYNC_V2_MESSAGE_BATCH_ITEM_LIMIT", "SYNC_V2_MESSAGE_BATCH_SEQS_LIMIT", "SYNC_V2_READ_BYTES_REDUCE_BATCH", "SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH", "SYNC_V2_BATCH_LIMIT", "SYNC_V2_REQUEST_BYTES":
			return ReduceBatch
		case "SYNC_V2_READ_CONCURRENCY", "SYNC_V2_SNAPSHOT_QUOTA", "SYNC_V2_READ_BUSY", "SYNC_V2_SCAN_BUSY", "SYNC_V2_BUILD_BUSY", "SYNC_V2_CHAT_BATCH_BUSY":
			return Retry
		}
	case 9:
		if apiErr.ErrMsg == "SYNC_V2_DIRECTORY_CHANGED" || apiErr.ErrMsg == "SYNC_V2_SNAPSHOT_EXPIRED" || apiErr.ErrMsg == "SYNC_V2_SCAN_EXPIRED" || apiErr.ErrMsg == "SYNC_V2_POLICY_CHANGED" || apiErr.ErrMsg == "SYNC_V2_CHANGE_CURSOR_EXPIRED" || apiErr.ErrMsg == "SYNC_V2_CHANGE_GENERATION_CHANGED" {
			return RebuildSnapshot
		}
	case 10:
		switch apiErr.ErrMsg {
		case "SYNC_V2_STATE_CHANGED":
			return RefreshState
		case "SYNC_V2_RESERVATION_EXPIRED":
			return RebuildSnapshot
		case "SYNC_V2_SNAPSHOT_NOT_READY":
			return Retry
		}
	case 12:
		if apiErr.ErrMsg == "SYNC_V2_CHAT_BATCH_UNSUPPORTED" || apiErr.ErrMsg == "SYNC_V2_DOWNSTREAM_VERSION" || apiErr.ErrMsg == "SYNC_V2_DIRECTORY_TOKEN_GUARD_UNSUPPORTED" {
			return Renegotiate
		}
	case 14:
		if apiErr.ErrMsg == "SYNC_V2_CHANGE_BARRIER_PENDING" || apiErr.ErrMsg == "SYNC_V2_REDIS_REQUIRED" || apiErr.ErrMsg == "SYNC_V2_REDIS_UNAVAILABLE" || apiErr.ErrMsg == "SYNC_V2_ADMISSION_UNAVAILABLE" {
			return Retry
		}
	}
	return Stop
}
