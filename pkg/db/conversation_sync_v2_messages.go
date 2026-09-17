//go:build !js

package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *DataBase) GetConversationSyncV2Stream(ctx context.Context, id string) (*model_struct.LocalConversationSyncV2Stream, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	var stream model_struct.LocalConversationSyncV2Stream
	err := d.conn.WithContext(ctx).Where("conversation_id = ?", id).Take(&stream).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &model_struct.LocalConversationSyncV2Stream{ConversationID: id}, nil
	}
	if err != nil {
		return nil, err
	}
	return &stream, nil
}

func (d *DataBase) InvalidateConversationSyncV2Stream(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("sync v2: missing stream ID")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "conversation_id"}}, DoUpdates: clause.Assignments(map[string]interface{}{"epoch": gorm.Expr("epoch + 1"), "state_token": ""})}).Create(&model_struct.LocalConversationSyncV2Stream{ConversationID: id, Epoch: 1}).Error
}

// AcceptConversationSyncV2State compares a network query with the epoch captured
// before that query. Each accepted query reserves a new local epoch so even
// same-token history and sync responses cannot overwrite each other's state.
// Opaque tokens are compared for equality only, never ordered as event versions.
func (d *DataBase) AcceptConversationSyncV2State(ctx context.Context, epoch uint64, state *model_struct.LocalConversationSyncV2Stream) (*model_struct.LocalConversationSyncV2Stream, error) {
	if state == nil || state.ConversationID == "" || state.StateToken == "" {
		return nil, fmt.Errorf("sync v2: invalid stream state")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var accepted model_struct.LocalConversationSyncV2Stream
	err := d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model_struct.LocalConversationSyncV2Stream
		err := tx.Where("conversation_id = ?", state.ConversationID).Take(&current).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if current.Epoch != epoch {
			return fmt.Errorf("%w: stale state response", sdkerrs.ErrSyncV2StateChanged)
		}
		accepted = *state
		accepted.ScannedSeq = current.ScannedSeq
		accepted.Initialized = true
		if state.Notifications && !current.Initialized {
			var legacy model_struct.NotificationSeqs
			err := tx.Where("conversation_id = ?", state.ConversationID).Take(&legacy).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			accepted.ScannedSeq = max(accepted.ScannedSeq, legacy.Seq)
		}
		accepted.PendingSeqs = nil
		for _, seq := range current.PendingSeqs {
			if seq >= max(int64(1), state.MinSeq) && (state.PermissionMaxSeq == 0 || seq <= state.PermissionMaxSeq) {
				accepted.PendingSeqs = append(accepted.PendingSeqs, seq)
			}
		}
		accepted.Epoch = current.Epoch + 1
		if accepted.PermissionMaxSeq > 0 {
			accepted.ScannedSeq = min(accepted.ScannedSeq, accepted.PermissionMaxSeq)
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&accepted).Error
	})
	if err != nil {
		return nil, err
	}
	return &accepted, nil
}

// AcceptConversationSyncV2States accepts a notification-state batch in one
// transaction. The caller supplies the epoch captured before the network read;
// any stale member aborts the whole batch so no partial state is published.
func (d *DataBase) AcceptConversationSyncV2States(ctx context.Context, states []*model_struct.LocalConversationSyncV2Stream) (map[string]*model_struct.LocalConversationSyncV2Stream, error) {
	accepted := make(map[string]*model_struct.LocalConversationSyncV2Stream, len(states))
	if len(states) == 0 {
		return accepted, nil
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	err := d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ids := make([]string, 0, len(states))
		for _, state := range states {
			if state == nil || state.ConversationID == "" || state.StateToken == "" || !state.Notifications {
				return fmt.Errorf("sync v2: invalid notification state batch")
			}
			ids = append(ids, state.ConversationID)
		}
		var currentRows []model_struct.LocalConversationSyncV2Stream
		if err := tx.Where("conversation_id IN ?", ids).Find(&currentRows).Error; err != nil {
			return err
		}
		currentByID := make(map[string]model_struct.LocalConversationSyncV2Stream, len(currentRows))
		for _, row := range currentRows {
			currentByID[row.ConversationID] = row
		}
		var legacyRows []model_struct.NotificationSeqs
		if err := tx.Where("conversation_id IN ?", ids).Find(&legacyRows).Error; err != nil {
			return err
		}
		legacyByID := make(map[string]int64, len(legacyRows))
		for _, row := range legacyRows {
			legacyByID[row.ConversationID] = row.Seq
		}
		rows := make([]*model_struct.LocalConversationSyncV2Stream, 0, len(states))
		for _, state := range states {
			current := currentByID[state.ConversationID]
			if current.Epoch != state.Epoch {
				return fmt.Errorf("%w: stale state response", sdkerrs.ErrSyncV2StateChanged)
			}
			next := *state
			next.ScannedSeq = current.ScannedSeq
			next.Initialized = true
			if !current.Initialized {
				next.ScannedSeq = max(next.ScannedSeq, legacyByID[state.ConversationID])
			}
			next.PendingSeqs = nil
			for _, seq := range current.PendingSeqs {
				if seq >= max(int64(1), state.MinSeq) && (state.PermissionMaxSeq == 0 || seq <= state.PermissionMaxSeq) {
					next.PendingSeqs = append(next.PendingSeqs, seq)
				}
			}
			next.Epoch = current.Epoch + 1
			if next.PermissionMaxSeq > 0 {
				next.ScannedSeq = min(next.ScannedSeq, next.PermissionMaxSeq)
			}
			rows = append(rows, &next)
			accepted[next.ConversationID] = &next
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&rows).Error
	})
	if err != nil {
		return nil, err
	}
	return accepted, nil
}

func conversationSyncV2Epochs(tx *gorm.DB, ids []string) (map[string]uint64, error) {
	epochs := make(map[string]uint64, len(ids))
	for _, id := range ids {
		epochs[id] = 0
	}
	for start := 0; start < len(ids); start += 500 {
		var streams []model_struct.LocalConversationSyncV2Stream
		if err := tx.Select("conversation_id", "epoch").Where("conversation_id IN ?", ids[start:min(start+500, len(ids))]).Find(&streams).Error; err != nil {
			return nil, err
		}
		for _, stream := range streams {
			epochs[stream.ConversationID] = stream.Epoch
		}
	}
	return epochs, nil
}

func (d *DataBase) GetConversationSyncV2Epochs(ctx context.Context, ids []string) (map[string]uint64, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	return conversationSyncV2Epochs(d.conn.WithContext(ctx), ids)
}

func mergeConversationSyncV2Progress(current, progress *model_struct.LocalConversationSyncV2Stream, epoch uint64, resolved []int64) (*model_struct.LocalConversationSyncV2Stream, error) {
	if current.Epoch != epoch || current.StateToken != "" && current.StateToken != progress.StateToken {
		return nil, fmt.Errorf("%w: stale stream response", sdkerrs.ErrSyncV2StateChanged)
	}
	if progress.MinSeq < 0 || progress.PermissionMaxSeq < 0 || progress.ScannedSeq < 0 {
		return nil, fmt.Errorf("sync v2: invalid stream bounds")
	}
	next := *progress
	next.Epoch = epoch
	next.Initialized = true
	next.ScannedSeq = max(next.ScannedSeq, current.ScannedSeq)
	if next.PermissionMaxSeq > 0 {
		next.ScannedSeq = min(next.ScannedSeq, next.PermissionMaxSeq)
	}
	pending := make(map[int64]bool, len(current.PendingSeqs)+len(progress.PendingSeqs))
	for _, seq := range current.PendingSeqs {
		pending[seq] = true
	}
	for _, seq := range progress.PendingSeqs {
		pending[seq] = true
	}
	for _, seq := range resolved {
		delete(pending, seq)
	}
	next.PendingSeqs = nil
	for seq := range pending {
		if seq >= max(int64(1), next.MinSeq) && (next.PermissionMaxSeq == 0 || seq <= next.PermissionMaxSeq) {
			next.PendingSeqs = append(next.PendingSeqs, seq)
		}
	}
	sort.Slice(next.PendingSeqs, func(i, j int) bool { return next.PendingSeqs[i] < next.PendingSeqs[j] })
	return &next, nil
}

// Called only after notification handlers succeed. The legacy cursor remains
// before the earliest unresolved sequence so a protocol downgrade cannot skip it.
func (d *DataBase) CommitConversationSyncV2Notifications(ctx context.Context, epoch uint64, progress *model_struct.LocalConversationSyncV2Stream, resolved []int64) error {
	if progress == nil || progress.ConversationID == "" || !progress.Notifications {
		return fmt.Errorf("sync v2: invalid notification progress")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model_struct.LocalConversationSyncV2Stream
		err := tx.Where("conversation_id = ?", progress.ConversationID).Take(&current).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		next, err := mergeConversationSyncV2Progress(&current, progress, epoch, resolved)
		if err != nil {
			return err
		}
		if err = tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(next).Error; err != nil {
			return err
		}
		legacySeq := next.ScannedSeq
		if len(next.PendingSeqs) > 0 {
			legacySeq = min(legacySeq, next.PendingSeqs[0]-1)
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&model_struct.NotificationSeqs{ConversationID: next.ConversationID, Seq: legacySeq}).Error
	})
}

// CommitConversationSyncV2Messages stores actual chat messages and unresolved
// sequence bookkeeping atomically. It never synthesizes deleted messages.
func (d *DataBase) CommitConversationSyncV2Messages(ctx context.Context, epoch uint64, progress *model_struct.LocalConversationSyncV2Stream, messages []*model_struct.LocalChatLog) error {
	return d.commitConversationSyncV2Messages(ctx, epoch, progress, messages, nil)
}

func (d *DataBase) CommitConversationSyncV2History(ctx context.Context, epoch uint64, progress *model_struct.LocalConversationSyncV2Stream, messages []*model_struct.LocalChatLog, state *pb.State) error {
	if state == nil || progress == nil || !state.Known || state.ConversationID != progress.ConversationID || state.StateToken != progress.StateToken || state.MinSeq != progress.MinSeq || state.PermissionMaxSeq != progress.PermissionMaxSeq || state.MaxSeq < 0 || state.UnreadCount < 0 || state.UnreadCount > math.MaxInt32 {
		return fmt.Errorf("sync v2: invalid history state")
	}
	return d.commitConversationSyncV2Messages(ctx, epoch, progress, messages, state)
}

func (d *DataBase) commitConversationSyncV2Messages(ctx context.Context, epoch uint64, progress *model_struct.LocalConversationSyncV2Stream, messages []*model_struct.LocalChatLog, state *pb.State) error {
	if progress == nil || progress.ConversationID == "" || progress.Notifications {
		return fmt.Errorf("sync v2: invalid chat progress")
	}
	if err := d.initChatLog(ctx, progress.ConversationID); err != nil {
		return err
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model_struct.LocalConversationSyncV2Stream
		err := tx.Where("conversation_id = ?", progress.ConversationID).Take(&current).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		resolved := make([]int64, 0, len(messages))
		for _, message := range messages {
			if message == nil || message.ClientMsgID == "" || message.Seq < max(int64(1), progress.MinSeq) || progress.PermissionMaxSeq > 0 && message.Seq > progress.PermissionMaxSeq {
				return fmt.Errorf("sync v2: invalid message identity or bounds")
			}
			resolved = append(resolved, message.Seq)
		}
		next, err := mergeConversationSyncV2Progress(&current, progress, epoch, resolved)
		if err != nil {
			return err
		}
		if len(messages) > 0 {
			updates := clause.AssignmentColumns([]string{"server_msg_id", "send_id", "recv_id", "sender_platform_id", "sender_nick_name", "sender_face_url", "session_type", "msg_from", "content_type", "content", "seq", "send_time", "create_time", "attached_info", "ex"})
			updates = append(updates, clause.Assignment{Column: clause.Column{Name: "status"}, Value: gorm.Expr("CASE WHEN status >= ? THEN status ELSE excluded.status END", constant.MsgStatusHasDeleted)}, clause.Assignment{Column: clause.Column{Name: "is_read"}, Value: gorm.Expr("CASE WHEN is_read THEN is_read ELSE excluded.is_read END")})
			if err := tx.Table(utils.GetTableName(progress.ConversationID)).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "client_msg_id"}}, DoUpdates: updates}).Create(messages).Error; err != nil {
				return err
			}
		}
		if state != nil {
			result := tx.Model(&model_struct.LocalConversation{}).Where("conversation_id = ?", progress.ConversationID).Updates(map[string]interface{}{"min_seq": state.MinSeq, "max_seq": state.MaxSeq, "unread_count": state.UnreadCount})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("sync v2: history conversation not loaded")
			}
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(next).Error
	})
}

// Confirm cached scan completion against freshly fetched authoritative bounds.
// The epoch comparison rejects permission invalidations since the request began.
// Changed bounds always go through the normal scan/commit path. During warm
// recovery an unchanged stream with historical holes is returned as false: keep
// its durable retry task, without reserving an epoch for a scan doing no work.
func (d *DataBase) UnchangedConversationSyncV2Streams(ctx context.Context, epochs map[string]uint64, states []*pb.State, deferHistoricalPending bool) (map[string]bool, error) {
	d.mRWMutex.RLock()
	defer d.mRWMutex.RUnlock()
	unchanged := make(map[string]bool)
	for start := 0; start < len(states); start += 500 {
		batch := states[start:min(start+500, len(states))]
		ids := make([]string, 0, len(batch))
		for _, s := range batch {
			if s != nil {
				ids = append(ids, s.ConversationID)
			}
		}
		if len(ids) == 0 {
			continue
		}
		var rows []model_struct.LocalConversationSyncV2Stream
		if err := d.conn.WithContext(ctx).Where("conversation_id IN ?", ids).Find(&rows).Error; err != nil {
			return nil, err
		}
		byID := make(map[string]*model_struct.LocalConversationSyncV2Stream, len(rows))
		for i := range rows {
			byID[rows[i].ConversationID] = &rows[i]
		}
		for _, state := range batch {
			if state == nil || !state.Known || state.StateToken == "" || state.MinSeq < 0 || state.MaxSeq < 0 || state.PermissionMaxSeq < 0 {
				continue
			}
			row := byID[state.ConversationID]
			epoch, ok := epochs[state.ConversationID]
			if !ok || row == nil || !row.Initialized || row.Epoch != epoch || row.StateToken != state.StateToken || row.MinSeq != state.MinSeq || row.PermissionMaxSeq != state.PermissionMaxSeq || (len(row.PendingSeqs) != 0 && !deferHistoricalPending) {
				continue
			}
			upper := state.MaxSeq
			if state.PermissionMaxSeq > 0 {
				upper = min(upper, state.PermissionMaxSeq)
			}
			if row.ScannedSeq >= upper {
				unchanged[state.ConversationID] = len(row.PendingSeqs) == 0
			}
		}
	}
	return unchanged, nil
}
