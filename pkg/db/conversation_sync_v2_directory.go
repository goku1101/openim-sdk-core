//go:build !js

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataBase) SaveConversationSyncV2DirectoryPage(ctx context.Context, before *model_struct.LocalConversationSyncV2, task *model_struct.ConversationSyncV2Task, page *model_struct.LocalConversationSyncV2Page) error {
	if before.UserID != d.loginUserID || page.Generation != before.Generation {
		return fmt.Errorf("sync v2: directory identity mismatch")
	}
	encoded, err := json.Marshal(task)
	if err != nil {
		return err
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		update := tx.Model(&model_struct.LocalConversationSyncV2{}).Where("user_id = ? AND generation = ? AND revision = ?", d.loginUserID, before.Generation, before.Revision).Updates(map[string]interface{}{"notification_task": string(encoded), "revision": gorm.Expr("revision + 1")})
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return fmt.Errorf("%w: stale directory page", sdkerrs.ErrSyncV2StateChanged)
		}
		if page.Position == 0 {
			if err := tx.Where("generation <> ?", page.Generation).Delete(&model_struct.LocalConversationSyncV2Page{}).Error; err != nil {
				return err
			}
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(page).Error
	})
}
func (d *DataBase) GetConversationSyncV2DirectoryPage(ctx context.Context, generation string, position int) (*model_struct.ConversationSyncV2TaskPage, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var row model_struct.LocalConversationSyncV2Page
	err := d.conn.WithContext(ctx).Where("generation = ? AND position = ?", generation, position).Take(&row).Error
	return row.Page, err
}
