//go:build !js

package db

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"gorm.io/gorm"
)

// Read notifications only touch existing local rows. V2 state queries own unread
// counts, including permission and clear-history boundaries.
func (d *DataBase) MarkConversationSyncV2LocalRead(ctx context.Context, id string, seqs []int64) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		table := utils.GetConversationTableName(id)
		var count int64
		if err := tx.Raw("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count).Error; err != nil {
			return err
		}
		if count > 0 && len(seqs) > 0 {
			if err := tx.Table(table).Where("seq IN ? AND send_id != ?", seqs, d.loginUserID).Update("is_read", true).Error; err != nil {
				return err
			}
		}
		var conversation model_struct.LocalConversation
		err := tx.Where("conversation_id = ?", id).Take(&conversation).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if conversation.LatestMsg == "" {
			return nil
		}
		var latest sdk_struct.MsgStruct
		if err = json.Unmarshal([]byte(conversation.LatestMsg), &latest); err != nil {
			return err
		}
		if latest.SendID == d.loginUserID || latest.IsRead {
			return nil
		}
		for _, seq := range seqs {
			if latest.Seq == seq {
				latest.IsRead = true
				return tx.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", id).Update("latest_msg", utils.StructToJsonString(&latest)).Error
			}
		}
		return nil
	})
}

// Unlike legacy history reads, a receipt must not require creating a chat table.
func (d *DataBase) GetConversationSyncV2LocalReadMessages(ctx context.Context, id string, seqs []int64) ([]*model_struct.LocalChatLog, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var count int64
	table := utils.GetConversationTableName(id)
	if err := d.conn.WithContext(ctx).Raw("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count).Error; err != nil {
		return nil, err
	}
	if count == 0 || len(seqs) == 0 {
		return nil, nil
	}
	var rows []*model_struct.LocalChatLog
	err := d.conn.WithContext(ctx).Table(table).Where("seq IN ?", seqs).Order("send_time DESC").Find(&rows).Error
	return rows, err
}
