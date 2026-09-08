package conversation_msg

import "github.com/openimsdk/openim-sdk-core/v3/sdk_struct"

// This query never waits for runMu, HTTP, SQLite or the conversation queue.
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
