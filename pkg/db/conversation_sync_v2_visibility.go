//go:build !js

package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataBase) GetConversationSyncV2Visibility(ctx context.Context, id string) (*model_struct.LocalConversationSyncV2Visibility, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	row := &model_struct.LocalConversationSyncV2Visibility{ConversationID: id}
	err := d.conn.WithContext(ctx).Where("conversation_id = ?", id).Take(row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return row, nil
	}
	return row, err
}

func (d *DataBase) HideConversationSyncV2(ctx context.Context, id string) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", id).Updates(map[string]interface{}{"unread_count": 0, "latest_msg": "", "latest_msg_send_time": 0, "draft_text": "", "draft_text_time": 0})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("sync v2: hidden conversation missing")
		}
		// Reject metadata/activation responses that began before the local hide.
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"epoch": gorm.Expr("epoch + 1"), "state_token": ""})}).Create(&model_struct.LocalConversationSyncV2Stream{ConversationID: id, Epoch: 1}).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"hidden": true, "revision": gorm.Expr("revision + 1")})}).Create(&model_struct.LocalConversationSyncV2Visibility{ConversationID: id, Hidden: true, Revision: 1}).Error
	})
}

// A successful activation may not undo a hide that happened after it started.
func (d *DataBase) RevealConversationSyncV2(ctx context.Context, id string, revision uint64) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model_struct.LocalConversationSyncV2Visibility
		err := tx.Where("conversation_id = ?", id).Take(&row).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if row.Revision != revision {
			return fmt.Errorf("%w: newer local visibility action", sdkerrs.ErrSyncV2StateChanged)
		}
		if !row.Hidden {
			return nil
		}
		return tx.Model(&row).Updates(map[string]interface{}{"hidden": false, "revision": gorm.Expr("revision + 1")}).Error
	})
}

// Local send confirmations share the legacy preview command. Guard its write
// atomically with hidden intent so a late acknowledgement cannot reveal a row.
func (d *DataBase) UpdateConversationSyncV2LocalPreview(ctx context.Context, id, message string, sendTime int64) (bool, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	hidden := d.conn.WithContext(ctx).Model(&model_struct.LocalConversationSyncV2Visibility{}).Select("conversation_id").Where("conversation_id = ? AND hidden = ?", id, true)
	result := d.conn.WithContext(ctx).Model(&model_struct.LocalConversation{}).Where("conversation_id = ? AND NOT EXISTS (?)", id, hidden).Updates(map[string]interface{}{"latest_msg": message, "latest_msg_send_time": sendTime})
	return result.RowsAffected > 0, result.Error
}
