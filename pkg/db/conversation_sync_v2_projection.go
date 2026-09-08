//go:build !js

package db

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"gorm.io/gorm"
)

func (d *DataBase) GetConversationSyncV2LatestMessage(ctx context.Context, id string, minSeq, maxSeq int64) ([]*model_struct.LocalChatLog, error) {
	if id == "" || minSeq < 0 || maxSeq < 0 {
		return nil, fmt.Errorf("sync v2: invalid projection bounds")
	}
	if err := d.initChatLog(ctx, id); err != nil {
		return nil, err
	}
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var messages []*model_struct.LocalChatLog
	err := d.conn.WithContext(ctx).Table(utils.GetTableName(id)).Where("status < ? AND (seq = 0 OR (seq >= ? AND seq <= ?))", constant.MsgStatusHasDeleted, max(int64(1), minSeq), maxSeq).Order("send_time DESC, seq DESC").Limit(1).Find(&messages).Error
	return messages, err
}

// A name lookup or message conversion may overlap a boundary event. Check both
// the metadata revision and captured stream epochs before publishing its result.
func (d *DataBase) ApplyConversationSyncV2Projection(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2, expectedEpochs map[string]uint64, conversations []*model_struct.LocalConversation) error {
	if checkpoint == nil || checkpoint.UserID != d.loginUserID {
		return fmt.Errorf("sync v2: invalid projection checkpoint")
	}
	selected := make(map[string]bool, len(checkpoint.SelectedIDs))
	for _, id := range checkpoint.SelectedIDs {
		selected[id] = true
	}
	ids := make([]string, 0, len(conversations))
	for _, conversation := range conversations {
		if conversation == nil || !selected[conversation.ConversationID] {
			return fmt.Errorf("sync v2: projection outside selection")
		}
		ids = append(ids, conversation.ConversationID)
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model_struct.LocalConversationSyncV2
		if err := tx.Where("user_id = ?", d.loginUserID).Take(&current).Error; err != nil {
			return err
		}
		if current.Generation != checkpoint.Generation || current.Revision != checkpoint.Revision || current.Stage != "messages" {
			return fmt.Errorf("sync v2: stale projection revision")
		}
		epochs, err := conversationSyncV2Epochs(tx, ids)
		if err != nil {
			return err
		}
		for _, conversation := range conversations {
			expected, ok := expectedEpochs[conversation.ConversationID]
			if !ok || epochs[conversation.ConversationID] != expected {
				return fmt.Errorf("sync v2: invalidated projection")
			}
			result := tx.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", conversation.ConversationID).Updates(map[string]interface{}{"show_name": conversation.ShowName, "face_url": conversation.FaceURL, "latest_msg": conversation.LatestMsg, "latest_msg_send_time": conversation.LatestMsgSendTime})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("sync v2: projection conversation disappeared")
			}
		}
		return nil
	})
}
