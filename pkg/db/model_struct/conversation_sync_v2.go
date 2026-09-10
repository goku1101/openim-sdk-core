package model_struct

// LocalConversationSyncV2 is separate from LocalVersionSync. A staged selection
// does not replace legacy UIDList or authorize deletion of local conversations.
// Incomplete snapshots are rebuilt; detail/message stages can replay idempotently.
type LocalConversationSyncV2 struct {
	UserID          string            `gorm:"column:user_id;primaryKey"`
	PolicyVersion   string            `gorm:"column:policy_version"`
	Stage           string            `gorm:"column:stage"`
	SnapshotID      string            `gorm:"column:snapshot_id"`
	Generation      string            `gorm:"column:generation"`
	Revision        uint64            `gorm:"column:revision"`
	SelectedIDs     StringArray       `gorm:"column:selected_ids;type:text"`
	NotificationIDs StringArray       `gorm:"column:notification_ids;type:text"`
	IncludeSelf     bool              `gorm:"column:include_self"`
	VersionID       string            `gorm:"column:version_id"`
	Version         uint64            `gorm:"column:version"`
	PendingStateIDs StringArray       `gorm:"column:pending_state_ids;type:text"`
	UnavailableIDs  StringArray       `gorm:"column:unavailable_ids;type:text"`
	StateEpochs     map[string]uint64 `gorm:"column:state_epochs;serializer:json"`
}

func (LocalConversationSyncV2) TableName() string { return "local_conversation_sync_v2" }

// ScannedSeq is a scan cursor, not proof that every message through it exists.
// PendingSeqs must be retried independently; Epoch invalidates in-flight reads.
type LocalConversationSyncV2Stream struct {
	ConversationID   string  `gorm:"column:conversation_id;primaryKey"`
	ParentID         string  `gorm:"column:parent_id"`
	Notifications    bool    `gorm:"column:notifications"`
	Self             bool    `gorm:"column:self"`
	StateToken       string  `gorm:"column:state_token"`
	Initialized      bool    `gorm:"column:initialized"`
	MinSeq           int64   `gorm:"column:min_seq"`
	PermissionMaxSeq int64   `gorm:"column:permission_max_seq"`
	ScannedSeq       int64   `gorm:"column:scanned_seq"`
	PendingSeqs      []int64 `gorm:"column:pending_seqs;serializer:json"`
	Epoch            uint64  `gorm:"column:epoch"`
}

func (LocalConversationSyncV2Stream) TableName() string { return "local_conversation_sync_v2_streams" }

// Retry work belongs to the account database. Revision prevents an in-flight
// worker from acknowledging or postponing a newer task with the same key.
type LocalConversationSyncV2Retry struct {
	TaskID   string `gorm:"column:task_id;primaryKey"`
	Kind     string `gorm:"column:kind"`
	Payload  string `gorm:"column:payload;type:text"`
	Revision uint64 `gorm:"column:revision"`
	Attempts uint32 `gorm:"column:attempts"`
	DueAt    int64  `gorm:"column:due_at;index"`
}

func (LocalConversationSyncV2Retry) TableName() string { return "local_conversation_sync_v2_retries" }

// Visibility is local user intent, independent of server sequence permissions.
// Revision orders explicit local actions without relying on wall-clock time.
type LocalConversationSyncV2Visibility struct {
	ConversationID string `gorm:"column:conversation_id;primaryKey"`
	Hidden         bool   `gorm:"column:hidden"`
	Revision       uint64 `gorm:"column:revision"`
}

func (LocalConversationSyncV2Visibility) TableName() string {
	return "local_conversation_sync_v2_visibility"
}
