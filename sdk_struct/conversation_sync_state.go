package sdk_struct

// ConversationSyncState describes the current full native synchronization.
// Login status and readable local resources are not evidence of sync completion.
type ConversationSyncState struct {
	UserID           string                  `json:"userID"`
	InitialDataReady bool                    `json:"initialDataReady"`
	Mode             string                  `json:"mode"`
	Phase            string                  `json:"phase"`
	Progress         int                     `json:"progress"`
	Detail           *ConversationSyncDetail `json:"detail,omitempty"`
	StoredMessages   int                     `json:"storedMessages"`
}

type ConversationSyncDetail struct {
	CurrentGroupMembers *int   `json:"currentGroupMembers,omitempty"`
	Stage               string `json:"stage"`
	Completed           int    `json:"completed"`
	Total               *int   `json:"total,omitempty"`
	Unit                string `json:"unit,omitempty"`
}
