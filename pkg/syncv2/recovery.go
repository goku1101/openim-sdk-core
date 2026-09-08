package syncv2

import (
	"errors"
	"net"
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
	if errors.Is(err, ErrCatchupPending) {
		return Retry
	}
	if errors.Is(err, ErrMetadataSnapshotRequired) {
		return RebuildSnapshot
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
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
		case "SYNC_V2_READ_BYTES_REDUCE_BATCH", "SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH", "SYNC_V2_BATCH_LIMIT", "SYNC_V2_REQUEST_BYTES":
			return ReduceBatch
		case "SYNC_V2_READ_CONCURRENCY", "SYNC_V2_SNAPSHOT_QUOTA":
			return Retry
		}
	case 9:
		if apiErr.ErrMsg == "SYNC_V2_SNAPSHOT_EXPIRED" || apiErr.ErrMsg == "SYNC_V2_POLICY_CHANGED" {
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
		if apiErr.ErrMsg == "SYNC_V2_DOWNSTREAM_VERSION" {
			return Renegotiate
		}
	case 14:
		if apiErr.ErrMsg == "SYNC_V2_REDIS_REQUIRED" || apiErr.ErrMsg == "SYNC_V2_REDIS_UNAVAILABLE" {
			return Retry
		}
	}
	return Stop
}
