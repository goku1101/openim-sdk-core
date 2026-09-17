//go:build !js

package db

import (
	"context"
	"fmt"
	"slices"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"gorm.io/gorm"
)

// Changes refresh only named rows. A profile/read hint alone does not promote
// a deferred conversation; pin/unread state is read from the authoritative API.
func (d *DataBase) ApplyConversationSyncV2Changes(ctx context.Context, before *model_struct.LocalConversationSyncV2, epochs map[string]uint64, states *pb.StateResponse, parents []string, self bool) error {
	if before == nil || before.UserID != d.loginUserID || states == nil || before.Stage != "messages" {
		return fmt.Errorf("sync v2: invalid change checkpoint")
	}
	next := *before
	next.SelectedIDs = append(model_struct.StringArray(nil), before.SelectedIDs...)
	next.NotificationIDs = append(model_struct.StringArray(nil), before.NotificationIDs...)
	next.IncludeSelf = before.IncludeSelf || self
	next.StateEpochs = make(map[string]uint64, len(before.StateEpochs)+len(epochs))
	for id, epoch := range before.StateEpochs {
		next.StateEpochs[id] = epoch
	}
	for id, epoch := range epochs {
		next.StateEpochs[id] = epoch
	}
	var refreshed []string
	for _, state := range states.States {
		if state == nil {
			return fmt.Errorf("sync v2: missing change state")
		}
		id := state.ConversationID
		refreshed = append(refreshed, id)
		if state.Known && state.Conversation != nil && (state.IsPinned || state.UnreadCount > 0) && !slices.Contains(next.SelectedIDs, id) {
			next.SelectedIDs = append(next.SelectedIDs, id)
		}
	}
	refreshed = append(refreshed, states.UnavailableIDs...)
	for _, id := range append(append([]string(nil), parents...), refreshed...) {
		if id == "" {
			return fmt.Errorf("sync v2: empty change parent")
		}
		if !slices.Contains(next.NotificationIDs, id) {
			next.NotificationIDs = append(next.NotificationIDs, id)
		}
	}
	return d.applyConversationSyncV2Details(ctx, before, &next, states, nil, refreshed, false)
}

func (d *DataBase) CommitConversationSyncV2ChangeCursor(ctx context.Context, before *model_struct.LocalConversationSyncV2, cursor, until, stage string) (*model_struct.LocalConversationSyncV2, error) {
	if before == nil || before.UserID != d.loginUserID || cursor == "" || !before.RecoveryBaselineReady || (stage != "ready" && stage != "messages") {
		return nil, fmt.Errorf("sync v2: invalid cursor commit")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var next model_struct.LocalConversationSyncV2
	err := d.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model_struct.LocalConversationSyncV2{}).Where("user_id = ? AND generation = ? AND revision = ? AND change_cursor = ? AND sync_domain_id = ? AND recovery_version = ?", d.loginUserID, before.Generation, before.Revision, before.ChangeCursor, before.SyncDomainID, before.RecoveryVersion).Updates(map[string]interface{}{"change_cursor": cursor, "change_until_cursor": until, "stage": stage, "revision": gorm.Expr("revision + 1")})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: stale change cursor", sdkerrs.ErrSyncV2StateChanged)
		}
		return tx.Where("user_id = ?", d.loginUserID).Take(&next).Error
	})
	return &next, err
}
