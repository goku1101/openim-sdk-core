//go:build !js

package db

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
)

// Caller holds mRWMutex, keeping bounds and message selection consistent with
// invalidation. This does not erase locally retained, currently hidden history.
func (d *DataBase) conversationSyncV2HistoryBounds(ctx context.Context, id string) (int64, int64, error) {
	var stream model_struct.LocalConversationSyncV2Stream
	if err := d.conn.WithContext(ctx).Where("conversation_id = ?", id).Take(&stream).Error; err != nil {
		return 0, 0, err
	}
	if !stream.Initialized || stream.StateToken == "" {
		return 0, 0, fmt.Errorf("%w: history permission unconfirmed", sdkerrs.ErrSyncV2StateChanged)
	}
	var conversation model_struct.LocalConversation
	if err := d.conn.WithContext(ctx).Where("conversation_id = ?", id).Take(&conversation).Error; err != nil {
		return 0, 0, err
	}
	lower, upper := max(int64(1), conversation.MinSeq, stream.MinSeq), conversation.MaxSeq
	if stream.PermissionMaxSeq > 0 {
		upper = min(upper, stream.PermissionMaxSeq)
	}
	return lower, upper, nil
}

func (d *DataBase) GetConversationSyncV2HistoryPage(ctx context.Context, id string, count int, startTime, startSeq int64, startID string, reverse bool) ([]*model_struct.LocalChatLog, error) {
	if err := d.initChatLog(ctx, id); err != nil {
		return nil, err
	}
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	lower, upper, err := d.conversationSyncV2HistoryBounds(ctx, id)
	if err != nil {
		return nil, err
	}
	order, symbol := "send_time DESC,seq DESC", "<"
	if reverse {
		order, symbol = "send_time ASC,seq ASC", ">"
	}
	query := d.conn.WithContext(ctx).Table(utils.GetTableName(id)).Where("(seq = 0 OR (seq >= ? AND seq <= ?))", lower, upper)
	if startTime > 0 {
		query = query.Where("(send_time "+symbol+" ? OR (send_time = ? AND (seq "+symbol+" ? OR (seq = 0 AND client_msg_id != ?))))", startTime, startTime, startSeq, startID)
	}
	var rows []*model_struct.LocalChatLog
	err = query.Order(order).Limit(count).Find(&rows).Error
	return rows, err
}

// Recheck after gap filling/recursive pages, since a permission event may have
// arrived since the first cached query. Filtering happens before API delivery.
func (d *DataBase) FilterConversationSyncV2History(ctx context.Context, id string, rows []*model_struct.LocalChatLog) ([]*model_struct.LocalChatLog, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	lower, upper, err := d.conversationSyncV2HistoryBounds(ctx, id)
	if err != nil {
		return nil, err
	}
	visible := make([]*model_struct.LocalChatLog, 0, len(rows))
	for _, row := range rows {
		if row.Seq == 0 || row.Seq >= lower && row.Seq <= upper {
			visible = append(visible, row)
		}
	}
	return visible, nil
}
