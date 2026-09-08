package model_struct

// LocalConversationSyncV2 is separate from LocalVersionSync. A staged selection
// does not replace legacy UIDList or authorize deletion of local conversations.
// Incomplete snapshots are rebuilt; detail/message stages can replay idempotently.
type LocalConversationSyncV2 struct {
	UserID          string      `gorm:"column:user_id;primaryKey"`
	PolicyVersion   string      `gorm:"column:policy_version"`
	Stage           string      `gorm:"column:stage"`
	SnapshotID      string      `gorm:"column:snapshot_id"`
	Generation      string      `gorm:"column:generation"`
	SelectedIDs     StringArray `gorm:"column:selected_ids;type:text"`
	NotificationIDs StringArray `gorm:"column:notification_ids;type:text"`
	IncludeSelf     bool        `gorm:"column:include_self"`
	VersionID       string      `gorm:"column:version_id"`
	Version         uint64      `gorm:"column:version"`
	PendingStateIDs StringArray `gorm:"column:pending_state_ids;type:text"`
	UnavailableIDs  StringArray `gorm:"column:unavailable_ids;type:text"`
}

func (LocalConversationSyncV2) TableName() string { return "local_conversation_sync_v2" }
