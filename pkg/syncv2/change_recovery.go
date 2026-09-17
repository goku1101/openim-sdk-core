package syncv2

import (
	"context"
	"fmt"
	"github.com/openimsdk/tools/log"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

const RecoveryVersion = "conversation-sync-changes-v1"

// Cursor commits compare the revision as well as the old opaque token. A late
// callback from the same generation must not move a committed cursor backwards.
type ChangeStore interface {
	CoordinatorStore
	ApplyConversationSyncV2Changes(context.Context, *model_struct.LocalConversationSyncV2, map[string]uint64, *pb.StateResponse, []string, bool) error
	CommitConversationSyncV2ChangeCursor(context.Context, *model_struct.LocalConversationSyncV2, string, string, string) (*model_struct.LocalConversationSyncV2, error)
}

func persistPendingBeforeCursor(ctx context.Context, result *SyncResult, hooks CoordinatorHooks) error {
	pending := len(result.Checkpoint.PendingStateIDs) > 0
	for _, streams := range []*StreamResults{result.Chats, result.Notifications} {
		pending = pending || streams != nil && len(streams.PendingIDs) > 0
	}
	if !pending {
		return nil
	}
	if hooks.PersistPending == nil {
		return fmt.Errorf("sync v2: durable retry persistence required")
	}
	return hooks.PersistPending(ctx, result)
}

func (c *Client) recoverChanges(ctx context.Context, op string, checkpoint *model_struct.LocalConversationSyncV2, caps *pb.Capabilities, base CoordinatorStore, hooks CoordinatorHooks) (*SyncResult, error) {
	store, ok := base.(ChangeStore)
	if !ok {
		return nil, fmt.Errorf("sync v2: change cursor storage unavailable")
	}
	log.ZInfo(ctx, "sync v2 incremental recovery started", "continuation", checkpoint.ChangeUntilCursor != "")
	result := &SyncResult{Enabled: true, Capabilities: caps, Checkpoint: checkpoint}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ReportDetailProgress(ctx, "changes", "", 0, nil)
		before := result.Checkpoint
		page, err := c.Changes(ctx, op, &pb.ChangeRequest{UserID: before.UserID, Cursor: before.ChangeCursor, UntilCursor: before.ChangeUntilCursor, PageSize: min(caps.MaxChangePageSize, 500)})
		if err != nil {
			return nil, err
		}
		if page.SyncDomainID != caps.SyncDomainID || page.NextCursor == "" || page.UntilCursor == "" || before.ChangeUntilCursor != "" && before.ChangeUntilCursor != page.UntilCursor || page.CaughtUp && page.HasMore || len(page.Changes) > int(min(caps.MaxChangePageSize, 500)) || page.HasMore && page.NextCursor == before.ChangeCursor {
			return nil, fmt.Errorf("sync v2: inconsistent change page")
		}
		var chats, notifications []string
		seenChats, seenNotifications := map[string]bool{}, map[string]bool{}
		self := false
		for _, change := range page.Changes {
			if change == nil || change.EventCursor == "" {
				return nil, fmt.Errorf("sync v2: invalid change event")
			}
			switch change.StreamKind {
			case "chat":
				if change.ConversationID == "" {
					return nil, fmt.Errorf("sync v2: empty change parent")
				}
				if !seenChats[change.ConversationID] {
					seenChats[change.ConversationID] = true
					chats = append(chats, change.ConversationID)
				}
			case "notification":
				if change.ConversationID == "" {
					return nil, fmt.Errorf("sync v2: empty notification parent")
				}
				if !seenNotifications[change.ConversationID] {
					seenNotifications[change.ConversationID] = true
					notifications = append(notifications, change.ConversationID)
				}
			case "self":
				if change.ConversationID != "" {
					return nil, fmt.Errorf("sync v2: invalid self parent")
				}
				self = true
			default:
				return nil, fmt.Errorf("sync v2: unsupported change stream")
			}
		}
		result.Chats, result.Notifications = nil, nil
		if len(page.Changes) > 0 {
			// Keep the bounded watermark across crashes/retries, without acknowledging
			// any event until its side effects have completed.
			if before.ChangeUntilCursor == "" {
				before, err = store.CommitConversationSyncV2ChangeCursor(ctx, before, before.ChangeCursor, page.UntilCursor, "messages")
				if err != nil {
					return nil, err
				}
			}
			epochs, err := store.GetConversationSyncV2Epochs(ctx, chats)
			if err != nil {
				return nil, err
			}
			states, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: before.UserID, ConversationIDs: chats, IncludeDetails: true}, caps)
			if err != nil {
				return nil, err
			}
			if err = store.ApplyConversationSyncV2Changes(ctx, before, epochs, states, notifications, self); err != nil {
				return nil, err
			}
			result.Checkpoint, err = store.GetConversationSyncV2(ctx)
			if err != nil {
				return nil, err
			}
			result.Notifications, err = c.SyncStreams(ctx, op, &pb.StateRequest{UserID: before.UserID, ConversationIDs: notifications, Notifications: true, IncludeSelf: self}, caps, store, hooks.Commit)
			if err != nil {
				return nil, err
			}
			if len(result.Notifications.PendingIDs) > 0 {
				return nil, fmt.Errorf("%w: change permission or notification pending", ErrCatchupPending)
			}
			result.Checkpoint, err = store.GetConversationSyncV2(ctx)
			if err != nil {
				return nil, err
			}
			selected := map[string]bool{}
			for _, id := range result.Checkpoint.SelectedIDs {
				selected[id] = true
			}
			var affected []string
			for _, id := range chats {
				if selected[id] {
					affected = append(affected, id)
				}
			}
			chatCtx := context.WithValue(ctx, deferHistoricalChatHolesKey{}, true)
			result.Chats, err = c.SyncStreams(chatCtx, op, &pb.StateRequest{UserID: before.UserID, ConversationIDs: affected}, caps, store, hooks.Commit)
			if err != nil {
				return nil, err
			}
			result.Checkpoint, err = store.GetConversationSyncV2(ctx)
			if err != nil {
				return nil, err
			}
			projection := *result.Checkpoint
			projection.SelectedIDs = affected
			if len(affected) > 0 {
				if err = hooks.Publish(ctx, &projection, func(done, total int) { reportCount(ctx, "publishing", "conversations", done, total) }); err != nil {
					return nil, err
				}
			}
			// Missing details cannot be acknowledged. Unknown sequence bounds must
			// remain durably pending before this page is acknowledged.
			for _, state := range states.States {
				if state.Conversation == nil {
					return nil, fmt.Errorf("%w: change permission or notification pending", ErrCatchupPending)
				}
			}
			if err = persistPendingBeforeCursor(ctx, result, hooks); err != nil {
				return nil, err
			}
		}
		current, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		if current.Generation != before.Generation || current.ChangeCursor != before.ChangeCursor || current.SyncDomainID != before.SyncDomainID || current.RecoveryVersion != before.RecoveryVersion {
			return nil, fmt.Errorf("%w: change recovery superseded", sdkerrs.ErrSyncV2StateChanged)
		}
		until, stage := page.UntilCursor, "messages"
		if page.CaughtUp {
			until = ""
			stage = "ready"
		}
		result.Checkpoint, err = store.CommitConversationSyncV2ChangeCursor(ctx, current, page.NextCursor, until, stage)
		if err != nil {
			return nil, err
		}
		log.ZInfo(ctx, "sync v2 incremental page committed", "changes", len(page.Changes), "chats", len(chats), "notifications", len(notifications), "self", self, "caughtUp", page.CaughtUp)
		if page.CaughtUp {
			if hooks.Progress != nil {
				hooks.Progress(99)
			}
			return result, nil
		}
		if !page.HasMore {
			return nil, ErrCatchupPending
		}
	}
}
