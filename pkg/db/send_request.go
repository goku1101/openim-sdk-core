//go:build !js

package db

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FreezeSendRequest commits before network dispatch. Concurrent retries share
// the first committed payload; reconnects and account restores cannot replace it.
func (d *DataBase) FreezeSendRequest(ctx context.Context, conversationID, clientMsgID string, payload []byte) ([]byte, error) {
	if err := d.initChatLog(ctx, conversationID); err != nil {
		return nil, err
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	row := model_struct.LocalSendRequest{ConversationID: conversationID, ClientMsgID: clientMsgID, Payload: payload}
	err := d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A late retry must not recreate a different payload after a concurrent
		// successful confirmation has released the original request.
		var message model_struct.LocalChatLog
		err := tx.Table(utils.GetTableName(conversationID)).Select("status").Where("client_msg_id = ?", clientMsgID).Take(&message).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && message.Status == constant.MsgStatusSendSuccess {
			return sdkerrs.ErrMsgRepeated
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
			return err
		}
		return tx.Where("conversation_id = ? AND client_msg_id = ?", conversationID, clientMsgID).Take(&row).Error
	})
	return row.Payload, err
}

func (d *DataBase) DeleteSendRequest(ctx context.Context, conversationID, clientMsgID string) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Where("conversation_id = ? AND client_msg_id = ?", conversationID, clientMsgID).Delete(&model_struct.LocalSendRequest{}).Error
}

func (d *DataBase) GetSendRequest(ctx context.Context, conversationID, clientMsgID string) ([]byte, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var row model_struct.LocalSendRequest
	err := d.conn.WithContext(ctx).Where("conversation_id = ? AND client_msg_id = ?", conversationID, clientMsgID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return row.Payload, err
}
