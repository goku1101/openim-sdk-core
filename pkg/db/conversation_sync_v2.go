//go:build !js

package db

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"gorm.io/gorm/clause"
)

func (d *DataBase) GetConversationSyncV2(ctx context.Context) (*model_struct.LocalConversationSyncV2, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var checkpoint model_struct.LocalConversationSyncV2
	if err := d.conn.WithContext(ctx).Where("user_id = ?", d.loginUserID).Take(&checkpoint).Error; err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (d *DataBase) SaveConversationSyncV2(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2) error {
	if checkpoint == nil || checkpoint.UserID != d.loginUserID {
		return fmt.Errorf("sync v2: checkpoint user mismatch")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	// UpdateAll persists false/zero/empty values when restarting a failed round.
	return d.conn.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(checkpoint).Error
}
