package syncv2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

var errScanBaselineRequired = errors.New("sync v2: scan needs snapshot baseline")

func validateScanPage(p *pb.ScanResponse, scope string, task *model_struct.ConversationSyncV2ScanScope, size int32) error {
	if p == nil || !p.Live || p.Scope != scope || p.PolicyVersion != PolicyVersion || p.VersionID == "" || p.ExpiresAt <= time.Now().Unix() || p.Scanned < 0 || p.Scanned > size || len(p.ConversationIDs) > int(p.Scanned) || p.HasMore && (p.Cursor == "" || p.Cursor == task.Cursor) || p.IncludeSelf != (scope == "notifications" && !task.Started) {
		return fmt.Errorf("sync v2: invalid scan page")
	}
	if task.Started && p.BaselineCursor != task.Baseline {
		return fmt.Errorf("sync v2: scan baseline changed")
	}
	seen := map[string]bool{}
	for _, id := range p.ConversationIDs {
		if id == "" || seen[id] {
			return fmt.Errorf("sync v2: invalid scan IDs")
		}
		seen[id] = true
	}
	return nil
}

func (c *Client) synchronizeScan(ctx context.Context, op, user string, store CoordinatorStore, hooks CoordinatorHooks, caps *pb.Capabilities, cp *model_struct.LocalConversationSyncV2) (*SyncResult, error) {
	if caps.MaxScanPageSize <= 0 {
		return nil, fmt.Errorf("sync v2: invalid scan page limit")
	}
	if cp == nil || cp.ScanTask == nil || cp.UserID != user || cp.SyncDomainID != caps.SyncDomainID || cp.PolicyVersion != PolicyVersion || cp.RecoveryVersion != caps.IncrementalRecoveryVersion {
		var identity [16]byte
		if _, err := rand.Read(identity[:]); err != nil {
			return nil, err
		}
		cp = &model_struct.LocalConversationSyncV2{UserID: user, Generation: hex.EncodeToString(identity[:]), PolicyVersion: PolicyVersion, Stage: "snapshot", SyncDomainID: caps.SyncDomainID, RecoveryVersion: caps.IncrementalRecoveryVersion, ScanTask: &model_struct.ConversationSyncV2ScanTask{PageSize: min(100, caps.MaxScanPageSize)}}
		if err := store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
	}
	task := cp.ScanTask
	if task.PageSize <= 0 || task.PageSize > caps.MaxScanPageSize {
		return nil, errScanBaselineRequired
	}
	generation := cp.Generation
	save := func() error {
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		if fresh.Generation != generation {
			return fmt.Errorf("sync v2: scan superseded")
		}
		fresh.ScanTask = task
		if err = store.SaveConversationSyncV2(ctx, fresh); err != nil {
			return err
		}
		cp = fresh
		return nil
	}
	if !task.SelfDone {
		streams, err := c.SyncStreams(ctx, op, &pb.StateRequest{UserID: user, Notifications: true, IncludeSelf: true}, caps, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		if len(streams.PendingIDs) > 0 || len(streams.UnavailableIDs) > 0 {
			return nil, ErrCatchupPending
		}
		task.SelfDone = true
		if err = save(); err != nil {
			return nil, err
		}
	}
	if task.InitialReady {
		if hooks.PrepareCached != nil {
			if err := hooks.PrepareCached(ctx); err != nil {
				return nil, err
			}
		}
		if hooks.Background != nil {
			if err := hooks.Background(ctx); err != nil {
				return nil, err
			}
		}
	}
	process := func(scope string, part *model_struct.ConversationSyncV2ScanScope) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if part.Done {
			return nil
		}
		if part.Page == nil {
			p, err := c.Scan(ctx, op, &pb.ScanRequest{UserID: user, Scope: scope, PageSize: task.PageSize, Cursor: part.Cursor})
			if err != nil {
				var api *APIError
				if errors.As(err, &api) && api.ErrCode == 12 {
					return errScanBaselineRequired
				}
				return err
			}
			if err = validateScanPage(p, scope, part, task.PageSize); err != nil {
				return err
			}
			if p.BaselineCursor == "" {
				return errScanBaselineRequired
			}
			part.Baseline = p.BaselineCursor
			part.Started = true
			part.Page = &model_struct.ConversationSyncV2TaskPage{Scope: scope, IDs: p.ConversationIDs, Cursor: p.Cursor, HasMore: p.HasMore}
			// Persist the received page before consuming it. A crash replays this exact
			// page rather than reading a changed live view at the old cursor.
			cp.ScanTask = task
			if cp.VersionID == "" {
				cp.VersionID = p.VersionID
				cp.Version = p.Version
				cp.Stage = "messages"
			}
			if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
				return err
			}
		}
		page := part.Page
		pageCtx := ctx
		if scope == "chat" {
			pageCtx = context.WithValue(ctx, preparedChatStatesKey{}, &preparedChatStates{})
			if err := c.priorityDetails(pageCtx, op, cp, page.IDs, caps, store); err != nil {
				return err
			}
		}
		streams, err := c.SyncStreams(pageCtx, op, &pb.StateRequest{UserID: user, ConversationIDs: page.IDs, Notifications: scope == "notifications"}, caps, store, hooks.Commit)
		if err != nil {
			return err
		}
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		if fresh.Generation != generation {
			return fmt.Errorf("sync v2: scan superseded")
		}
		cp = fresh
		result := &SyncResult{Enabled: true, Capabilities: caps, Checkpoint: cp}
		if scope == "chat" {
			result.Chats = streams
		} else {
			result.Notifications = streams
			for _, id := range page.IDs {
				if !slices.Contains(cp.NotificationIDs, id) {
					cp.NotificationIDs = append(cp.NotificationIDs, id)
				}
			}
		}
		if err = persistPendingBeforeCursor(ctx, result, hooks); err != nil {
			return err
		}
		if scope == "chat" {
			projection := *cp
			projection.SelectedIDs = page.IDs
			if err = hooks.Publish(ctx, &projection, func(int, int) {}); err != nil {
				return err
			}
		}
		part.Cursor = page.Cursor
		part.Done = !page.HasMore
		part.Page = nil
		cp.ScanTask = task
		if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
			return err
		}
		ReportDetailProgress(ctx, "snapshot", "", 0, nil)
		if len(page.IDs) == 0 && page.HasMore {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		return nil
	}
	// First notification and chat pages form the bounded foreground phase.
	if !task.InitialReady {
		if !task.Notifications.Started || task.Notifications.Page != nil {
			if err := process("notifications", &task.Notifications); err != nil {
				return nil, err
			}
		}
		if !task.Chat.Started || task.Chat.Page != nil {
			if err := process("chat", &task.Chat); err != nil {
				return nil, err
			}
		}
		states, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: user, ConversationIDs: cp.SelectedIDs, IncludeDetails: true}, caps)
		if err != nil {
			return nil, err
		}
		// The first chat page's own notification streams must be ready as well.
		streams, err := c.SyncStreams(ctx, op, &pb.StateRequest{UserID: user, ConversationIDs: cp.SelectedIDs, Notifications: true}, caps, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		// Unknown parent streams remain durable work; they do not invalidate
		// the independently readable first chat page. Self notifications above
		// still gate account-wide permission changes.
		projection := *cp
		projection.NotificationIDs = append(append([]string(nil), cp.NotificationIDs...), cp.SelectedIDs...)
		if err = persistPendingBeforeCursor(ctx, &SyncResult{Enabled: true, Capabilities: caps, Checkpoint: &projection, Notifications: streams}, hooks); err != nil {
			return nil, err
		}
		if hooks.PrepareInitial != nil {
			if err = hooks.PrepareInitial(ctx, &Round{Enabled: true, PriorityComplete: true, Capabilities: caps, Checkpoint: cp, ChatStates: states}); err != nil {
				return nil, err
			}
		}
		task.InitialReady = true
		if err = save(); err != nil {
			return nil, err
		}
		if hooks.Background != nil {
			if err = hooks.Background(ctx); err != nil {
				return nil, err
			}
		}
	}
	for !task.Chat.Done || !task.Notifications.Done {
		if err := process("chat", &task.Chat); err != nil {
			return nil, err
		}
		if err := process("notifications", &task.Notifications); err != nil {
			return nil, err
		}
	}
	if err := hooks.PrepareRelated(ctx); err != nil {
		return nil, err
	}
	// Each baseline is a full feed, not a scope-filtered feed. Persist the round
	// index with its initial cursor; never compare opaque cursor values.
	for task.RecoveryRound < 2 {
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		cp = fresh
		if cp.Generation != generation {
			return nil, fmt.Errorf("sync v2: scan superseded")
		}
		if cp.ChangeCursor == "" {
			cp.ChangeCursor = task.Chat.Baseline
			if task.RecoveryRound == 1 {
				cp.ChangeCursor = task.Notifications.Baseline
			}
			cp.RecoveryBaselineReady = true
			cp.Stage = "messages"
			cp.ScanTask = task
			if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
				return nil, err
			}
		}
		result, err := c.recoverChanges(ctx, op, cp, caps, store, hooks)
		if err != nil {
			return nil, err
		}
		cp = result.Checkpoint
		task.RecoveryRound++
		if task.RecoveryRound < 2 {
			cp.ChangeCursor = ""
			cp.ChangeUntilCursor = ""
			cp.Stage = "messages"
		}
		cp.ScanTask = task
		if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
	}
	cp.ScanTask = nil
	if err := store.SaveConversationSyncV2(ctx, cp); err != nil {
		return nil, err
	}
	return &SyncResult{Enabled: true, Capabilities: caps, Checkpoint: cp}, nil
}
