package sdk_struct

// ConversationSyncState describes the current full native synchronization.
// Login status and readable local resources are not evidence of sync completion.
type ConversationSyncState struct {
	Mode     string `json:"mode"`
	Phase    string `json:"phase"`
	Progress int    `json:"progress"`
}
