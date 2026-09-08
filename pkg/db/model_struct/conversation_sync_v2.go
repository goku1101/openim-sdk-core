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
