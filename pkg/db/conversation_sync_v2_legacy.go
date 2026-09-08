//go:build !js

package db

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"gorm.io/gorm"
)

// Called only after an explicit unsupported capability response. Invalidating
// V2 readers and resetting the legacy metadata cursor precede selecting legacy.
func (d *DataBase) PrepareConversationSyncV2Legacy(ctx context.Context) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var checkpoint model_struct.LocalConversationSyncV2
		err := tx.Where("user_id = ?", d.loginUserID).Take(&checkpoint).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if checkpoint.Stage == "legacy" {
			return nil
		}
		if err = tx.Where("table_name = ? AND entity_id = ?", model_struct.LocalConversation{}.TableName(), d.loginUserID).Delete(&model_struct.LocalVersionSync{}).Error; err != nil {
			return err
		}
		if err = tx.Model(&model_struct.LocalConversationSyncV2Stream{}).Where("1 = 1").Updates(map[string]interface{}{"epoch": gorm.Expr("epoch + 1"), "state_token": ""}).Error; err != nil {
			return err
		}
		return tx.Model(&model_struct.LocalConversationSyncV2{}).Where("user_id = ?", d.loginUserID).Updates(map[string]interface{}{"stage": "legacy", "revision": gorm.Expr("revision + 1")}).Error
	})
}
