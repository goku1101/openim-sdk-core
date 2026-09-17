package model_struct

// LocalSendRequest is the original wire request of an unconfirmed send. It is
// account-local and independent of display/history projections.
type LocalSendRequest struct {
	ConversationID string `gorm:"primaryKey;type:char(128)"`
	ClientMsgID    string `gorm:"primaryKey;type:char(64)"`
	Payload        []byte
}
