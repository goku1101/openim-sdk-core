//go:build !js

package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
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
			return fmt.Errorf("%w: stale projection revision", sdkerrs.ErrSyncV2StateChanged)
		}
		epochs, err := conversationSyncV2Epochs(tx, ids)
		if err != nil {
			return err
		}
		var visibility []model_struct.LocalConversationSyncV2Visibility
		if err := tx.Where("conversation_id IN (?) AND hidden = ?", ids, true).Find(&visibility).Error; err != nil {
			return err
		}
		hidden := make(map[string]bool, len(visibility))
		for _, row := range visibility {
			hidden[row.ConversationID] = true
		}
		previous := make(map[string]model_struct.LocalConversation, len(ids))
		for start := 0; start < len(ids); start += 500 {
			var rows []model_struct.LocalConversation
			if err := tx.Select("conversation_id", "show_name", "face_url", "latest_msg", "latest_msg_send_time").Where("conversation_id IN ?", ids[start:min(start+500, len(ids))]).Find(&rows).Error; err != nil {
				return err
			}
			for _, row := range rows {
				previous[row.ConversationID] = row
			}
		}
		for _, conversation := range conversations {
			expected, ok := expectedEpochs[conversation.ConversationID]
			if !ok || epochs[conversation.ConversationID] != expected {
				return fmt.Errorf("%w: invalidated projection", sdkerrs.ErrSyncV2StateChanged)
			}
			if hidden[conversation.ConversationID] {
				conversation.LatestMsg = ""
				conversation.LatestMsgSendTime = 0
			}
			old, exists := previous[conversation.ConversationID]
			if !exists {
				return fmt.Errorf("sync v2: projection conversation disappeared")
			}
			if old.ShowName == conversation.ShowName && old.FaceURL == conversation.FaceURL && old.LatestMsg == conversation.LatestMsg && old.LatestMsgSendTime == conversation.LatestMsgSendTime {
				continue
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

// Keep the same visible-message query while amortizing SQLite/GORM work across
// independent conversation tables. Each subquery still returns at most one row.
func (d *DataBase) GetConversationSyncV2LatestMessages(ctx context.Context, conversations []*model_struct.LocalConversation) (map[string]*model_struct.LocalChatLog, error) {
	for _, row := range conversations {
		if row == nil || row.ConversationID == "" || row.MinSeq < 0 || row.MaxSeq < 0 {
			return nil, fmt.Errorf("sync v2: invalid projection bounds")
		}
		if err := d.initChatLog(ctx, row.ConversationID); err != nil {
			return nil, err
		}
	}
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	statement := &gorm.Statement{DB: d.conn}
	if err := statement.Parse(&model_struct.LocalChatLog{}); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(statement.Schema.DBNames))
	for _, name := range statement.Schema.DBNames {
		fields = append(fields, statement.Quote(name))
	}
	columns := strings.Join(fields, ",")
	result := make(map[string]*model_struct.LocalChatLog, len(conversations))
	for start := 0; start < len(conversations); start += 100 {
		var queries []string
		var args []interface{}
		for _, row := range conversations[start:min(start+100, len(conversations))] {
			table := statement.Quote(utils.GetTableName(row.ConversationID))
			queries = append(queries, "SELECT ? AS sync_conversation_id, * FROM (SELECT "+columns+" FROM "+table+" WHERE status < ? AND (seq = 0 OR (seq >= ? AND seq <= ?)) ORDER BY send_time DESC, seq DESC LIMIT 1)")
			args = append(args, row.ConversationID, constant.MsgStatusHasDeleted, max(int64(1), row.MinSeq), row.MaxSeq)
		}
		var rows []struct {
			ID string `gorm:"column:sync_conversation_id"`
			model_struct.LocalChatLog
		}
		if err := d.conn.WithContext(ctx).Raw(strings.Join(queries, " UNION ALL "), args...).Scan(&rows).Error; err != nil {
			return nil, err
		}
		for i := range rows {
			result[rows[i].ID] = &rows[i].LocalChatLog
		}
	}
	return result, nil
}
