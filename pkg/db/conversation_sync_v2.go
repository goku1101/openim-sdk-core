//go:build !js

package db

import (
	"context"
	"fmt"
	"math"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"gorm.io/gorm"
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

// ApplyConversationSyncV2Details atomically merges server-owned fields and the
// next stage. Drafts, display metadata and local message history are untouched.
func (d *DataBase) ApplyConversationSyncV2Details(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2, response *pb.StateResponse) error {
	if checkpoint == nil || checkpoint.UserID != d.loginUserID || checkpoint.Stage != "details" || response == nil {
		return fmt.Errorf("sync v2: invalid details checkpoint")
	}
	selected := make(map[string]bool, len(checkpoint.SelectedIDs))
	for _, id := range checkpoint.SelectedIDs {
		selected[id] = true
	}
	unavailable := make(map[string]bool, len(response.UnavailableIDs))
	for _, id := range response.UnavailableIDs {
		if !selected[id] {
			return fmt.Errorf("sync v2: unavailable ID outside selection")
		}
		unavailable[id] = true
	}
	next := *checkpoint
	next.Stage = "messages"
	next.PendingStateIDs = nil
	next.UnavailableIDs = append(model_struct.StringArray(nil), response.UnavailableIDs...)
	states := make(map[string]*pb.State, len(response.States))
	for _, state := range response.States {
		if state == nil || !selected[state.ConversationID] || unavailable[state.ConversationID] || states[state.ConversationID] != nil {
			return fmt.Errorf("sync v2: invalid details response membership")
		}
		states[state.ConversationID] = state
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model_struct.LocalConversationSyncV2
		if err := tx.Where("user_id = ?", d.loginUserID).Take(&current).Error; err != nil {
			return err
		}
		if current.Generation != checkpoint.Generation || current.SnapshotID != checkpoint.SnapshotID || current.Stage != "details" || current.VersionID != checkpoint.VersionID || current.Version != checkpoint.Version {
			return fmt.Errorf("sync v2: stale details round")
		}
		epochs, err := conversationSyncV2Epochs(tx, checkpoint.SelectedIDs)
		if err != nil {
			return err
		}
		for _, id := range checkpoint.SelectedIDs {
			if epochs[id] != checkpoint.StateEpochs[id] {
				return fmt.Errorf("sync v2: details invalidated by conversation event")
			}
			if unavailable[id] {
				continue
			}
			state := states[id]
			if state == nil || state.Conversation == nil || !state.Known {
				next.PendingStateIDs = append(next.PendingStateIDs, id)
			}
			if state == nil || state.Conversation == nil {
				continue
			}
			detail := state.Conversation
			if detail.ConversationID != id || detail.OwnerUserID != d.loginUserID {
				return fmt.Errorf("sync v2: detail identity mismatch")
			}
			columns := map[string]interface{}{
				"conversation_id": id, "conversation_type": detail.ConversationType, "user_id": detail.UserID, "group_id": detail.GroupID,
				"recv_msg_opt": detail.RecvMsgOpt, "group_at_type": detail.GroupAtType, "is_pinned": state.IsPinned,
				"is_private_chat": detail.IsPrivateChat, "burn_duration": detail.BurnDuration,
				"attached_info": detail.AttachedInfo, "ex": detail.Ex, "msg_destruct_time": detail.MsgDestructTime, "is_msg_destruct": detail.IsMsgDestruct,
			}
			if state.Known {
				if state.UnreadCount < 0 || state.UnreadCount > math.MaxInt32 || state.MinSeq < 0 || state.MaxSeq < 0 {
					return fmt.Errorf("sync v2: invalid effective bounds or unread count")
				}
				columns["min_seq"], columns["max_seq"], columns["unread_count"] = state.MinSeq, state.MaxSeq, state.UnreadCount
			}
			updates := make([]string, 0, len(columns)-1)
			for key := range columns {
				if key != "conversation_id" {
					updates = append(updates, key)
				}
			}
			if err := tx.Table(model_struct.LocalConversation{}.TableName()).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.AssignmentColumns(updates)}).Create(columns).Error; err != nil {
				return err
			}
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&next).Error
	})
}

func (d *DataBase) SaveConversationSyncV2(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2) error {
	if checkpoint == nil || checkpoint.UserID != d.loginUserID {
		return fmt.Errorf("sync v2: checkpoint user mismatch")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	// UpdateAll persists false/zero/empty values when restarting a failed round.
	result := d.conn.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true, Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "? = 'snapshot' OR generation = ?", Vars: []interface{}{checkpoint.Stage, checkpoint.Generation}}}}}).Create(checkpoint)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("sync v2: stale checkpoint generation")
	}
	return nil
}
