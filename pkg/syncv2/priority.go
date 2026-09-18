package syncv2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// synchronizePriority consumes bounded pages rather than materializing the
// complete notification directory before making the first conversations usable.
func (c *Client) synchronizePriority(ctx context.Context, op, user string, store CoordinatorStore, hooks CoordinatorHooks, caps *pb.Capabilities, before *model_struct.LocalConversationSyncV2) (*SyncResult, error) {
	if caps.MaxNotificationPriority < 0 || caps.MaxNotificationPriority > 500 {
		return nil, fmt.Errorf("sync v2: invalid priority limit")
	}
	cp := before
	resume := !hooks.ForceSnapshot && cp != nil && cp.UserID == user && cp.PolicyVersion == PolicyVersion && cp.SyncDomainID == caps.SyncDomainID && cp.RecoveryVersion == caps.IncrementalRecoveryVersion && cp.NotificationTask != nil && cp.NotificationTask.Status != "invalidated" && cp.SnapshotID != "" && cp.ChangeCursor != ""
	if resume && !directoryCaptured(cp.NotificationTask) && cp.NotificationTask.ExpiresAt <= time.Now().Unix() {
		resume = false
	}
	if resume {
		token := cp.NotificationTask.DirectoryToken
		raw, err := hex.DecodeString(token)
		if err != nil || len(raw) != 32 {
			return nil, &APIError{ErrCode: 3, ErrMsg: "SYNC_V2_INVALID_DIRECTORY_TOKEN"}
		}
		// One guarded batch before ANY old page or stream is consumed. The empty
		// parent list still checks the directory; self is a valid notification query.
		_, err = c.States(ctx, op, &pb.StateRequest{UserID: user, Notifications: true, IncludeSelf: true, ExpectedDirectoryToken: token})
		if err != nil {
			if RecoveryFor(err) == RebuildSnapshot {
				cp.NotificationTask.Status = "invalidated"
				if saveErr := store.SaveConversationSyncV2(ctx, cp); saveErr != nil {
					return nil, saveErr
				}
			}
			return nil, err
		}
	} else {
		first, err := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: user, Scope: "notifications", PageSize: min(caps.MaxPageSize, 500), NotificationPriorityPrefix: true})
		if err != nil {
			return nil, err
		}
		raw, tokenErr := hex.DecodeString(first.DirectoryToken)
		if first.SnapshotID == "" || first.BaselineCursor == "" || first.VersionID == "" || first.PolicyVersion != PolicyVersion || first.Scope != "notifications" || first.ExpiresAt <= time.Now().Unix() || tokenErr != nil || len(raw) != 32 || first.NotificationPriorityCount < 0 || first.NotificationPriorityCount > caps.MaxNotificationPriority || int64(first.NotificationPriorityCount) > first.Total || !first.IncludeSelf || (first.Mode != "full" && first.Mode != "filtered") {
			return nil, fmt.Errorf("sync v2: invalid priority snapshot")
		}
		var generation [16]byte
		if _, err = rand.Read(generation[:]); err != nil {
			return nil, err
		}
		cp = &model_struct.LocalConversationSyncV2{UserID: user, Generation: hex.EncodeToString(generation[:]), PolicyVersion: PolicyVersion, Stage: "snapshot", SnapshotID: first.SnapshotID, VersionID: first.VersionID, Version: first.Version, ChangeCursor: first.BaselineCursor, SyncDomainID: caps.SyncDomainID, RecoveryVersion: caps.IncrementalRecoveryVersion, IncludeSelf: true,
			NotificationTask: &model_struct.ConversationSyncV2Task{SnapshotVersion: first.Version, SnapshotVersionID: first.VersionID, BaselineCursor: first.BaselineCursor, DirectoryToken: first.DirectoryToken, ExpiresAt: first.ExpiresAt, Mode: first.Mode, PriorityCount: int(first.NotificationPriorityCount), NotificationTotal: int(first.Total), ChatTotal: -1, Status: "building_snapshot"}}
		if err = setPriorityPage(cp, first, min(caps.MaxPageSize, 500)); err != nil {
			return nil, err
		}
		if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
		cp.Stage = "messages"
		if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
	}
	if cp.Stage == "snapshot" {
		cp.Stage = "messages"
		if err := store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
	}
	result := &SyncResult{Enabled: true, Capabilities: caps, Checkpoint: cp}
	save := func(task *model_struct.ConversationSyncV2Task) error {
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		if fresh.Generation != cp.Generation {
			return fmt.Errorf("sync v2: priority task superseded")
		}
		fresh.NotificationTask = task
		if err = store.SaveConversationSyncV2(ctx, fresh); err != nil {
			return err
		}
		cp = fresh
		result.Checkpoint = cp
		return nil
	}
	// Streams own their transactional scanned sequence and message commits.
	// A crash before task advancement safely replays the page without redelivery.
	persistStreams := func(streams *StreamResults, notifications bool) error {
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		result.Checkpoint = fresh
		result.Chats, result.Notifications = nil, nil
		if notifications {
			result.Notifications = streams
		} else {
			result.Chats = streams
		}
		return persistPendingBeforeCursor(ctx, result, hooks)
	}
	task := cp.NotificationTask
	if hooks.Progress != nil {
		hooks.Progress(PriorityProgress(task))
	}
	if !task.SelfDone {
		streams, err := c.SyncStreams(ctx, op, &pb.StateRequest{UserID: user, Notifications: true, IncludeSelf: true}, caps, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		if err = persistStreams(streams, true); err != nil {
			return nil, err
		}
		task.SelfDone = true
		if err = save(task); err != nil {
			return nil, err
		}
	}
	// Preserve the notification page while fetching a bounded chat preview.
	publishInitial := func() error {
		states, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: user, ConversationIDs: cp.SelectedIDs, IncludeDetails: true}, caps)
		if err != nil {
			return err
		}
		if hooks.PrepareInitial != nil {
			if err = hooks.PrepareInitial(ctx, &Round{Enabled: true, PriorityComplete: true, Capabilities: caps, Checkpoint: cp, ChatStates: states}); err != nil {
				return err
			}
		}
		task.PriorityReady = true
		task.Status = "background_tail"
		if err = save(task); err != nil {
			return err
		}
		if hooks.Background != nil {
			return hooks.Background(ctx)
		}
		return nil
	}
	if task.PriorityReady {
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
	for !task.NotificationsDone || !task.ChatsDone {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !directoryCaptured(task) && time.Now().Unix() >= task.ExpiresAt {
			return nil, &APIError{ErrCode: 9, ErrMsg: "SYNC_V2_SNAPSHOT_EXPIRED"}
		}
		// At the priority boundary, project the same priority parents. No full chat
		// directory scan is needed before the initial page can be used.
		if !task.PriorityReady && task.NotificationProcessed >= task.PriorityCount {
			preview, readErr := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: user, SnapshotID: cp.SnapshotID, Scope: "chat", PageSize: min(caps.MaxPageSize, 50)})
			if readErr != nil {
				return nil, readErr
			}
			if preview.Scope != "chat" {
				return nil, fmt.Errorf("sync v2: mismatched preview scope")
			}
			previewCheckpoint := *cp
			previewTask := *task
			previewTask.ChatProcessed = 0
			previewTask.ChatCursor = ""
			previewCheckpoint.NotificationTask = &previewTask
			if err := setPriorityPage(&previewCheckpoint, preview, min(caps.MaxPageSize, 50)); err != nil {
				return nil, err
			}
			if err := c.priorityDetails(ctx, op, cp, preview.ConversationIDs, caps, store); err != nil {
				return nil, err
			}
			var refreshErr error
			cp, refreshErr = store.GetConversationSyncV2(ctx)
			if refreshErr != nil {
				return nil, refreshErr
			}
			ids := cp.NotificationIDs
			if len(ids) > task.PriorityCount {
				ids = ids[:task.PriorityCount]
			}
			if err := c.priorityDetails(ctx, op, cp, ids, caps, store); err != nil {
				return nil, err
			}
			var err error
			cp, err = store.GetConversationSyncV2(ctx)
			if err != nil {
				return nil, err
			}
			result.Checkpoint = cp
			if err = publishInitial(); err != nil {
				return nil, err
			}
		}
		if task.PriorityReady && !directoryCaptured(task) {
			var err error
			cp, err = c.capturePriorityDirectory(ctx, op, cp, caps, store)
			if err != nil {
				return nil, err
			}
			task = cp.NotificationTask
			result.Checkpoint = cp
		}
		if task.Page == nil && directoryCaptured(task) {
			spool := store.(priorityDirectoryStore)
			var err error
			task.Page, err = spool.GetConversationSyncV2DirectoryPage(ctx, cp.Generation, task.NextPage)
			if err != nil {
				return nil, err
			}
		}
		if task.Page == nil {
			scope, cursor := "notifications", task.NotificationCursor
			if task.NotificationsDone {
				scope, cursor = "chat", task.ChatCursor
			}
			page, err := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: user, SnapshotID: cp.SnapshotID, Scope: scope, Cursor: cursor, PageSize: min(caps.MaxPageSize, 500)})
			if err != nil {
				return nil, err
			}
			if page.Scope != scope {
				return nil, fmt.Errorf("sync v2: mismatched snapshot scope")
			}
			if err = setPriorityPage(cp, page, min(caps.MaxPageSize, 500)); err != nil {
				return nil, err
			}
			task = cp.NotificationTask
			if err = save(task); err != nil {
				return nil, err
			}
		}
		page := task.Page
		if page.Offset < 0 || page.Offset > len(page.IDs) {
			return nil, fmt.Errorf("sync v2: invalid saved page position")
		}
		end := len(page.IDs)
		if page.Scope == "notifications" && !task.PriorityReady {
			end = min(end, page.Offset+task.PriorityCount-task.NotificationProcessed)
		}
		ids := page.IDs[page.Offset:end]
		pageCtx := ctx
		if page.Scope == "chat" {
			pageCtx = context.WithValue(ctx, preparedChatStatesKey{}, &preparedChatStates{})
			if err := c.priorityDetails(pageCtx, op, cp, ids, caps, store); err != nil {
				return nil, err
			}
			var err error
			cp, err = store.GetConversationSyncV2(ctx)
			if err != nil {
				return nil, err
			}
		}
		streams, err := c.SyncStreams(priorityPageContext(pageCtx, task, page.Scope, hooks), op, &pb.StateRequest{UserID: user, ConversationIDs: ids, Notifications: page.Scope == "notifications"}, caps, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		if err = persistStreams(streams, page.Scope == "notifications"); err != nil {
			return nil, err
		}
		if page.Scope == "chat" {
			projection := *result.Checkpoint
			projection.SelectedIDs = ids
			if err = hooks.Publish(ctx, &projection, func(int, int) {}); err != nil {
				return nil, err
			}
			task.ChatProcessed += len(ids)
		} else {
			task.NotificationProcessed += len(ids)
		}
		page.Offset = end
		// Task cursor advances only after durable messages and retries are committed.
		if page.Offset == len(page.IDs) {
			if directoryCaptured(task) {
				task.NextPage++
			}
			if page.Scope == "notifications" {
				task.NotificationCursor = page.Cursor
				task.NotificationsDone = !page.HasMore
			} else {
				task.ChatCursor = page.Cursor
				task.ChatsDone = !page.HasMore
			}
			task.Page = nil
		}
		// Merge the directory into the latest checkpoint, preserving foreground work.
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		if fresh.Generation != cp.Generation {
			return nil, fmt.Errorf("sync v2: priority task superseded")
		}
		if page.Scope == "notifications" {
			for _, id := range ids {
				if !slices.Contains(fresh.NotificationIDs, id) {
					fresh.NotificationIDs = append(fresh.NotificationIDs, id)
				}
			}
		}
		fresh.NotificationTask = task
		if err = store.SaveConversationSyncV2(ctx, fresh); err != nil {
			return nil, err
		}
		cp = fresh
		result.Checkpoint = cp
		if page.Scope == "chat" {
			reportCount(ctx, "messages", "conversations", task.ChatProcessed, task.ChatTotal)
		} else {
			reportCount(ctx, "notifications", "streams", task.NotificationProcessed, task.NotificationTotal)
		}
		if hooks.Progress != nil {
			hooks.Progress(PriorityProgress(task))
		}
	}
	if !task.PriorityReady {
		if err := publishInitial(); err != nil {
			return nil, err
		}
	}
	if err := hooks.PrepareRelated(ctx); err != nil {
		return nil, err
	}
	var err error
	cp, err = store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if cp.Generation != result.Checkpoint.Generation {
		return nil, fmt.Errorf("sync v2: priority task superseded")
	}
	cp.RecoveryBaselineReady = true
	if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
		return nil, err
	}
	result, err = c.recoverChanges(ctx, op, cp, caps, store, hooks)
	if err != nil {
		return nil, err
	}
	// Release only after both directories and the bounded changes watermark commit.
	_ = c.Release(ctx, op, &pb.ReleaseRequest{UserID: user, SnapshotID: cp.SnapshotID})
	result.Checkpoint.NotificationTask = nil
	if err = store.SaveConversationSyncV2(ctx, result.Checkpoint); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) priorityDetails(ctx context.Context, op string, cp *model_struct.LocalConversationSyncV2, ids []string, caps *pb.Capabilities, store CoordinatorStore) error {
	epochs, err := store.GetConversationSyncV2Epochs(ctx, ids)
	if err != nil {
		return err
	}
	states, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: cp.UserID, ConversationIDs: ids, IncludeDetails: true}, caps)
	if err != nil {
		return err
	}
	next := *cp
	next.SelectedIDs = append(model_struct.StringArray(nil), cp.SelectedIDs...)
	next.StateEpochs = epochs
	next.Stage = "messages"
	for _, id := range ids {
		if !slices.Contains(next.SelectedIDs, id) {
			next.SelectedIDs = append(next.SelectedIDs, id)
		}
	}
	subset, ok := store.(MetadataSubsetStore)
	if !ok {
		return fmt.Errorf("sync v2: page storage unavailable")
	}
	if err := subset.ApplyConversationSyncV2MetadataSubset(ctx, cp, &next, states, nil, ids); err != nil {
		return err
	}
	if prepared, ok := ctx.Value(preparedChatStatesKey{}).(*preparedChatStates); ok {
		current, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		if current.Generation != cp.Generation || current.Revision != cp.Revision+1 {
			return fmt.Errorf("sync v2: priority details superseded")
		}
		prepared.states = make(map[string]*pb.State, len(states.States))
		for _, state := range states.States {
			prepared.states[state.ConversationID] = state
		}
		prepared.unavailable = make(map[string]bool, len(states.UnavailableIDs))
		for _, id := range states.UnavailableIDs {
			prepared.unavailable[id] = true
		}
		// These epochs belong to the committed metadata transaction. Never
		// recapture live epochs to make an old response appear current.
		prepared.epochs = current.StateEpochs
	}
	return nil
}

func setPriorityPage(cp *model_struct.LocalConversationSyncV2, p *pb.SnapshotResponse, limit int32) error {
	t := cp.NotificationTask
	if p.SnapshotID != cp.SnapshotID || p.PolicyVersion != cp.PolicyVersion || p.VersionID != t.SnapshotVersionID || p.Version != t.SnapshotVersion || p.BaselineCursor != t.BaselineCursor || p.DirectoryToken != t.DirectoryToken || p.ExpiresAt != t.ExpiresAt || p.Mode != t.Mode || p.Total < 0 || p.Total > 100000 || len(p.ConversationIDs) > int(limit) {
		return fmt.Errorf("sync v2: inconsistent priority page")
	}
	processed, cursor := t.NotificationProcessed, t.NotificationCursor
	if p.Scope == "chat" {
		processed, cursor = t.ChatProcessed, t.ChatCursor
		if t.ChatTotal < 0 {
			t.ChatTotal = int(p.Total)
		}
		if t.ChatTotal != int(p.Total) {
			return fmt.Errorf("sync v2: changed chat total")
		}
	} else if p.Scope != "notifications" || int(p.Total) != t.NotificationTotal || int(p.NotificationPriorityCount) != t.PriorityCount {
		return fmt.Errorf("sync v2: changed notification total")
	}
	if p.HasMore && (p.Cursor == "" || p.Cursor == cursor || len(p.ConversationIDs) == 0 || processed+len(p.ConversationIDs) >= int(p.Total)) || !p.HasMore && (p.Cursor != "" || processed+len(p.ConversationIDs) != int(p.Total)) {
		return fmt.Errorf("sync v2: non advancing priority page")
	}
	seen := map[string]bool{}
	for _, id := range p.ConversationIDs {
		if id == "" || seen[id] {
			return fmt.Errorf("sync v2: duplicate priority parent")
		}
		seen[id] = true
	}
	t.Page = &model_struct.ConversationSyncV2TaskPage{Scope: p.Scope, IDs: p.ConversationIDs, Cursor: p.Cursor, HasMore: p.HasMore}
	return nil
}
