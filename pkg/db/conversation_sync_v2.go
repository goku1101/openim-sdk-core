//go:build !js

package db

import (
	"context"
	"fmt"
	"math"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
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
	return d.applyConversationSyncV2Details(ctx, checkpoint, checkpoint, response, nil, checkpoint.SelectedIDs, false)
}

// Metadata version, current details and explicit deletions commit atomically.
// The old legacy UIDList and message tables are not repurposed or erased.
func (d *DataBase) ApplyConversationSyncV2Metadata(ctx context.Context, before, next *model_struct.LocalConversationSyncV2, response *pb.StateResponse, deleted []string) error {
	if next == nil {
		return fmt.Errorf("sync v2: invalid metadata checkpoint")
	}
	return d.ApplyConversationSyncV2MetadataSubset(ctx, before, next, response, deleted, next.SelectedIDs)
}

// A notification refresh preserves unrelated streams and their pending state.
func (d *DataBase) ApplyConversationSyncV2MetadataSubset(ctx context.Context, before, next *model_struct.LocalConversationSyncV2, response *pb.StateResponse, deleted, refreshed []string) error {
	if before == nil || next == nil || response == nil || before.UserID != d.loginUserID || next.UserID != d.loginUserID || before.Generation != next.Generation || before.SnapshotID != next.SnapshotID || before.VersionID != next.VersionID || next.Version < before.Version || next.Stage != "messages" || before.Stage != "messages" && before.Stage != "catchup" && before.Stage != "ready" {
		return fmt.Errorf("sync v2: invalid metadata checkpoint")
	}
	selected := make(map[string]bool, len(next.SelectedIDs))
	for _, id := range next.SelectedIDs {
		selected[id] = true
	}
	seen := make(map[string]bool, len(refreshed))
	for _, id := range refreshed {
		if !selected[id] || seen[id] {
			return fmt.Errorf("sync v2: metadata refresh outside selection or duplicated")
		}
		seen[id] = true
	}
	return d.applyConversationSyncV2Details(ctx, before, next, response, deleted, refreshed, false)
}

func (d *DataBase) applyConversationSyncV2Details(ctx context.Context, expected, checkpoint *model_struct.LocalConversationSyncV2, response *pb.StateResponse, deleted, refreshed []string, existingOnly bool) error {
	selected := make(map[string]bool, len(refreshed))
	for _, id := range refreshed {
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
	next.StateEpochs = make(map[string]uint64, len(checkpoint.StateEpochs))
	for id, epoch := range checkpoint.StateEpochs {
		next.StateEpochs[id] = epoch
	}
	if !existingOnly {
		next.Stage = "messages"
	}
	next.Revision = expected.Revision + 1
	next.PendingStateIDs = nil
	retained := make(map[string]bool, len(checkpoint.SelectedIDs))
	for _, id := range checkpoint.SelectedIDs {
		retained[id] = true
	}
	next.UnavailableIDs = append(model_struct.StringArray(nil), response.UnavailableIDs...)
	for _, id := range checkpoint.PendingStateIDs {
		if !selected[id] {
			next.PendingStateIDs = append(next.PendingStateIDs, id)
		}
	}
	for _, id := range checkpoint.UnavailableIDs {
		if retained[id] && !selected[id] {
			next.UnavailableIDs = append(next.UnavailableIDs, id)
		}
	}
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
		if current.Revision != expected.Revision || current.Generation != expected.Generation || current.SnapshotID != expected.SnapshotID || current.Stage != expected.Stage || current.VersionID != expected.VersionID || current.Version != expected.Version {
			return fmt.Errorf("%w: stale details round", sdkerrs.ErrSyncV2StateChanged)
		}
		var hiddenRows []model_struct.LocalConversationSyncV2Visibility
		if err := tx.Where("hidden = ? AND conversation_id IN (?)", true, refreshed).Find(&hiddenRows).Error; err != nil {
			return err
		}
		hidden := make(map[string]bool, len(hiddenRows))
		for _, row := range hiddenRows {
			hidden[row.ConversationID] = true
		}
		checkIDs := append(append([]string(nil), refreshed...), deleted...)
		epochs, err := conversationSyncV2Epochs(tx, checkIDs)
		if err != nil {
			return err
		}
		for _, id := range checkIDs {
			if epochs[id] != checkpoint.StateEpochs[id] {
				return fmt.Errorf("%w: details invalidated by conversation event", sdkerrs.ErrSyncV2StateChanged)
			}
		}
		for _, id := range deleted {
			if id == "" || selected[id] {
				return fmt.Errorf("sync v2: inconsistent deleted selection")
			}
			if err := tx.Where("conversation_id = ?", id).Delete(&model_struct.LocalConversation{}).Error; err != nil {
				return err
			}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"epoch": gorm.Expr("epoch + 1"), "state_token": ""})}).Create(&model_struct.LocalConversationSyncV2Stream{ConversationID: id, Epoch: 1}).Error; err != nil {
				return err
			}
		}
		existing := make(map[string]model_struct.LocalConversation, len(refreshed))
		unchangedPermissions := make(map[string]bool, len(refreshed))
		for start := 0; start < len(refreshed); start += 500 {
			var conversations []model_struct.LocalConversation
			if err := tx.Where("conversation_id IN ?", refreshed[start:min(start+500, len(refreshed))]).Find(&conversations).Error; err != nil {
				return err
			}
			for _, row := range conversations {
				existing[row.ConversationID] = row
			}
			var streams []model_struct.LocalConversationSyncV2Stream
			if err := tx.Where("conversation_id IN ?", refreshed[start:min(start+500, len(refreshed))]).Find(&streams).Error; err != nil {
				return err
			}
			for _, stream := range streams {
				state := states[stream.ConversationID]
				unchangedPermissions[stream.ConversationID] = !unavailable[stream.ConversationID] && stream.Initialized && state != nil && state.Known && state.Conversation != nil && state.StateToken != "" && state.StateToken == stream.StateToken && state.MinSeq == stream.MinSeq && state.PermissionMaxSeq == stream.PermissionMaxSeq
			}
		}
		for _, id := range refreshed {
			next.StateEpochs[id] = epochs[id]
			// Tail/read changes do not change the V2 permission token. Retire
			// in-flight history only for changed or unconfirmed permission bounds.
			if !unchangedPermissions[id] {
				next.StateEpochs[id] = epochs[id] + 1
				if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"epoch": gorm.Expr("epoch + 1"), "state_token": ""})}).Create(&model_struct.LocalConversationSyncV2Stream{ConversationID: id, Epoch: 1}).Error; err != nil {
					return err
				}
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
			if hidden[id] && state.Known && state.IsPinned {
				var previous model_struct.LocalConversation
				if err := tx.Where("conversation_id = ?", id).Take(&previous).Error; err != nil {
					return err
				}
				if !previous.IsPinned {
					if err := tx.Model(&model_struct.LocalConversationSyncV2Visibility{}).Where("conversation_id = ? AND hidden = ?", id, true).Updates(map[string]interface{}{"hidden": false, "revision": gorm.Expr("revision + 1")}).Error; err != nil {
						return err
					}
				}
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
			// A confirmed metadata tail makes the directory pageable without pulling
			// message bodies. Never replace an existing preview or reveal a hidden row.
			previous := existing[id]
			seedTail := state.Known && state.MaxSeq > 0 && state.LastMessageTime > 0 && !hidden[id] && previous.LatestMsgSendTime == 0 && previous.LatestMsg == ""
			if seedTail {
				columns["latest_msg_send_time"] = state.LastMessageTime
			}
			// Compare inside the transaction, after validating permission epochs.
			// Unchanged cache rows need neither an UPDATE nor an UPSERT.
			if previous, ok := existing[id]; ok && !seedTail && conversationSyncV2DetailsEqual(previous, state) {
				continue
			}
			updates := make([]string, 0, len(columns)-1)
			for key := range columns {
				if key != "conversation_id" {
					updates = append(updates, key)
				}
			}
			if existingOnly || !retained[id] {
				if err := tx.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", id).Updates(columns).Error; err != nil {
					return err
				}
			} else if err := tx.Table(model_struct.LocalConversation{}.TableName()).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.AssignmentColumns(updates)}).Create(columns).Error; err != nil {
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
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if checkpoint.Stage == "snapshot" {
			var previous model_struct.LocalConversationSyncV2
			result := tx.Select("stage").Where("user_id = ?", d.loginUserID).Limit(1).Find(&previous)
			if result.Error != nil {
				return result.Error
			}
			if previous.Stage == "legacy" {
				// Legacy writes can restore an entry without updating the V2 visibility
				// marker. Preserve their result when taking ownership back from legacy.
				visible := tx.Model(&model_struct.LocalConversation{}).Select("conversation_id").Where("latest_msg_send_time > ?", 0)
				if err := tx.Model(&model_struct.LocalConversationSyncV2Visibility{}).Where("hidden = ? AND conversation_id IN (?)", true, visible).Updates(map[string]interface{}{"hidden": false, "revision": gorm.Expr("revision + 1")}).Error; err != nil {
					return err
				}
			}
		}
		// UpdateAll persists false/zero/empty values when restarting a failed round.
		result := tx.Clauses(clause.OnConflict{UpdateAll: true, Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "? = 'snapshot' OR (generation = ? AND revision = ?)", Vars: []interface{}{checkpoint.Stage, checkpoint.Generation, checkpoint.Revision}}}}}).Create(checkpoint)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("%w: stale checkpoint generation", sdkerrs.ErrSyncV2StateChanged)
		}
		return nil
	})
}

func conversationSyncV2DetailsEqual(previous model_struct.LocalConversation, state *pb.State) bool {
	detail := state.Conversation
	return previous.ConversationType == detail.ConversationType &&
		previous.UserID == detail.UserID && previous.GroupID == detail.GroupID &&
		previous.RecvMsgOpt == detail.RecvMsgOpt && previous.GroupAtType == detail.GroupAtType &&
		previous.IsPinned == state.IsPinned && previous.IsPrivateChat == detail.IsPrivateChat &&
		previous.BurnDuration == detail.BurnDuration && previous.AttachedInfo == detail.AttachedInfo &&
		previous.Ex == detail.Ex && previous.MsgDestructTime == detail.MsgDestructTime &&
		previous.IsMsgDestruct == detail.IsMsgDestruct &&
		(!state.Known || (previous.MinSeq == state.MinSeq && previous.MaxSeq == state.MaxSeq && int64(previous.UnreadCount) == state.UnreadCount))
}

// Read only the server-owned metadata cached by the last committed version.
// Dynamic state (unread and visible bounds) is always fetched independently.
func (d *DataBase) GetConversationSyncV2CachedDetails(ctx context.Context, ids []string) (map[string]*conversation.Conversation, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	result := make(map[string]*conversation.Conversation, len(ids))
	var checkpoint model_struct.LocalConversationSyncV2
	if err := d.conn.WithContext(ctx).Where("user_id = ?", d.loginUserID).Take(&checkpoint).Error; err != nil {
		return nil, err
	}
	committed := make(map[string]bool, len(checkpoint.SelectedIDs))
	for _, id := range checkpoint.SelectedIDs {
		committed[id] = true
	}
	for _, id := range checkpoint.PendingStateIDs {
		delete(committed, id)
	}
	for _, id := range checkpoint.UnavailableIDs {
		delete(committed, id)
	}

	for start := 0; start < len(ids); start += 500 {
		var rows []model_struct.LocalConversation
		if err := d.conn.WithContext(ctx).Select("conversation_id", "conversation_type", "user_id", "group_id", "recv_msg_opt", "group_at_type", "is_private_chat", "burn_duration", "attached_info", "ex", "msg_destruct_time", "is_msg_destruct").Where("conversation_id IN ?", ids[start:min(start+500, len(ids))]).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			if !committed[row.ConversationID] {
				continue
			}
			result[row.ConversationID] = &conversation.Conversation{OwnerUserID: d.loginUserID, ConversationID: row.ConversationID, ConversationType: row.ConversationType, UserID: row.UserID, GroupID: row.GroupID, RecvMsgOpt: row.RecvMsgOpt, GroupAtType: row.GroupAtType, IsPrivateChat: row.IsPrivateChat, BurnDuration: row.BurnDuration, AttachedInfo: row.AttachedInfo, Ex: row.Ex, MsgDestructTime: row.MsgDestructTime, IsMsgDestruct: row.IsMsgDestruct}
		}
	}
	return result, nil
}
