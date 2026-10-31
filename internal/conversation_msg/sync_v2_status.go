package conversation_msg

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
)

// This query never waits for the run gate, HTTP, SQLite or the conversation queue.
// A rebuilt JS runtime can observe a synchronization already in progress.
func (c *Conversation) GetConversationSyncState() sdk_struct.ConversationSyncState {
	state := sdk_struct.ConversationSyncState{Mode: "uninitialized", Phase: "idle"}
	if c == nil || c.syncV2 == nil {
		return state
	}
	runtime := c.syncV2
	if current := runtime.status.Load(); current != nil {
		state = *current
	}
	switch runtime.mode.Load() {
	case syncV2Legacy:
		state.Mode = "legacy"
	case syncV2Enabled:
		state.Mode = "v2"
	default:
		state.Mode = "negotiating"
	}
	return state
}

// A retryable background failure does not revoke an already usable first page.
// Cancellation and terminal errors still clear readiness for the session owner.
func conversationSyncV2FailureState(state sdk_struct.ConversationSyncState, cancelled bool, action syncv2.Recovery) sdk_struct.ConversationSyncState {
	if !cancelled && state.InitialDataReady && state.PriorityReady && (action == syncv2.Retry || action == syncv2.RefreshState || action == syncv2.Unavailable) {
		state.Phase = "retry_wait"
		return state
	}
	state.Phase = "failed"
	state.InitialDataReady = false
	state.PriorityReady = false
	return state
}
