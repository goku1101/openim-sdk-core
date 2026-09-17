package syncv2

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/tools/log"
	"gorm.io/gorm"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

var ErrCatchupPending = errors.New("sync v2: metadata still introduces notification parents")

type CoordinatorStore interface {
	DetailsStore
	MetadataStore
	BatchStreamStore
}

// Hooks bind the protocol to existing SDK facilities. Publish must durably
// refresh conversation projections before the checkpoint can become ready.
// The caller owns start/failure/finish notifications; finish follows success.
type CoordinatorHooks struct {
	// Used only after a classified server error requires a fresh snapshot.
	Background     func(context.Context) error
	ForceSnapshot  bool
	PersistPending func(context.Context, *SyncResult) error
	PrepareInitial func(context.Context, *Round) error
	// A validated persistent baseline can be displayed without rereading every
	// stream. This callback does not complete incremental or permission recovery.
	PrepareCached  func(context.Context) error
	Negotiated     func(context.Context, *pb.Capabilities) error
	PrepareRelated func(context.Context) error
	ReadMetadata   func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error)
	Commit         CommitStream
	Publish        func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error
	Progress       func(int)
}

type SyncResult struct {
	Enabled       bool
	Checkpoint    *model_struct.LocalConversationSyncV2
	Capabilities  *pb.Capabilities
	Chats         *StreamResults
	Notifications *StreamResults
}

// Synchronize performs a single negotiated round. Errors never select legacy;
// a caller may rebuild a snapshot on the classified error, with bounded retries.
// Pending state or storage holes remain retryable after the UI becomes ready.
func (c *Client) Synchronize(ctx context.Context, op, userID string, store CoordinatorStore, hooks CoordinatorHooks) (*SyncResult, error) {
	if store == nil || hooks.PrepareRelated == nil || hooks.ReadMetadata == nil || hooks.Commit == nil || hooks.Publish == nil {
		return nil, fmt.Errorf("sync v2: incomplete coordinator dependencies")
	}
	caps, err := c.Capabilities(ctx, op, &pb.UserRequest{UserID: userID})
	if err != nil {
		return nil, err
	}
	if hooks.Negotiated != nil {
		if err = hooks.Negotiated(ctx, caps); err != nil {
			return nil, err
		}
	}
	if !caps.ConversationSyncV2 {
		return &SyncResult{Enabled: false, Capabilities: caps}, nil
	}
	if caps.IncrementalRecovery && (caps.IncrementalRecoveryVersion != RecoveryVersion || caps.SyncDomainID == "" || caps.MaxChangePageSize <= 0) {
		return nil, fmt.Errorf("sync v2: incompatible incremental recovery contract")
	}
	lastProgress := 0
	report := func(value int) {
		if hooks.Progress != nil && value > lastProgress {
			lastProgress = value
			hooks.Progress(value)
		}
	}
	streamContext := func(start, end int) context.Context {
		return withStreamProgress(ctx, func(done, total int) {
			if total > 0 {
				report(start + (end-start)*done/total)
			}
		})
	}
	// A completed snapshot selection and committed stream cursors survive login.
	// Check the server's metadata baseline before deciding to replace them.
	checkpointBefore, err := store.GetConversationSyncV2(ctx)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	validRecoveryCheckpoint := caps.IncrementalRecovery && !hooks.ForceSnapshot && checkpointBefore != nil && checkpointBefore.UserID == userID && checkpointBefore.PolicyVersion == PolicyVersion && checkpointBefore.SyncDomainID == caps.SyncDomainID && checkpointBefore.RecoveryVersion == caps.IncrementalRecoveryVersion && checkpointBefore.ChangeCursor != "" && checkpointBefore.Generation != "" && checkpointBefore.VersionID != "" && (checkpointBefore.Stage == "ready" || checkpointBefore.Stage == "messages")
	if validRecoveryCheckpoint {
		if hooks.PrepareCached != nil && (checkpointBefore.RecoveryBaselineReady || checkpointBefore.NotificationTask == nil) {
			if err = hooks.PrepareCached(ctx); err != nil {
				return nil, err
			}
		}
		if checkpointBefore.RecoveryBaselineReady {
			if err = hooks.PrepareRelated(ctx); err != nil {
				return nil, err
			}
			checkpointBefore, err = reloadAfterRelated(ctx, store, checkpointBefore)
			if err != nil {
				return nil, err
			}
			return c.recoverChanges(ctx, op, checkpointBefore, caps, store, hooks)
		}
	}
	if caps.NotificationPriorityPrefix && caps.DirectoryTokenGuard && caps.IncrementalRecovery {
		return c.synchronizePriority(ctx, op, userID, store, hooks, caps, checkpointBefore)
	}
	// A committed details checkpoint already owns the snapshot selection and
	// C0. Interrupted initial message consumption resumes that baseline; only
	// completion permits the changes-only fast path.
	resumeBaseline := validRecoveryCheckpoint && !checkpointBefore.RecoveryBaselineReady && checkpointBefore.NotificationTask == nil
	resume := resumeBaseline || !caps.IncrementalRecovery && !hooks.ForceSnapshot && err == nil && checkpointBefore != nil && checkpointBefore.NotificationTask == nil && checkpointBefore.UserID == userID && checkpointBefore.PolicyVersion == PolicyVersion && checkpointBefore.Generation != "" && checkpointBefore.VersionID != "" && (checkpointBefore.Stage == "ready" || checkpointBefore.Stage == "messages" || checkpointBefore.Stage == "catchup")
	if resume && !resumeBaseline {
		delta, readErr := hooks.ReadMetadata(ctx, checkpointBefore.Version, checkpointBefore.VersionID)
		if readErr != nil {
			return nil, readErr
		}
		if _, planErr := PlanMetadataDelta(checkpointBefore, delta); errors.Is(planErr, ErrMetadataSnapshotRequired) {
			resume = false
		} else if planErr != nil {
			return nil, planErr
		}
	}
	var round *Round
	var restoredSelection *Selection
	if resume {
		if !resumeBaseline {
			restoredSelection, err = c.ReadSnapshot(ctx, op, userID, min(caps.MaxPageSize, 500))
			if err != nil {
				return nil, err
			}
			if restoredSelection.VersionID != checkpointBefore.VersionID || restoredSelection.Version < checkpointBefore.Version {
				return nil, ErrMetadataSnapshotRequired
			}
		}
		// Durable historical chat holes are retried after startup with backoff.
		// New sequences and all notification consumption still run in this round.
		ctx = context.WithValue(ctx, deferHistoricalChatHolesKey{}, true)
		round = &Round{Enabled: true, Capabilities: caps, Checkpoint: checkpointBefore}
	} else {
		log.ZInfo(ctx, "sync v2 snapshot baseline started", "incrementalRecovery", caps.IncrementalRecovery, "forced", hooks.ForceSnapshot)
		round, err = c.prepareRoundWithCapabilities(ctx, op, userID, store, caps, report)
		if err != nil {
			return nil, err
		}
		round, err = commitPreparedDetails(ctx, round, store)
		if err != nil {
			return nil, err
		}
	}
	result := &SyncResult{Enabled: round.Enabled, Capabilities: round.Capabilities, Checkpoint: round.Checkpoint}
	if !round.Enabled {
		return result, nil
	}
	report(10)
	if hooks.PrepareInitial != nil && !resume {
		if err = hooks.PrepareInitial(ctx, round); err != nil {
			return nil, err
		}
		// Preview notification consumers may have advanced metadata.
		checkpoint, readErr := store.GetConversationSyncV2(ctx)
		if readErr != nil {
			return nil, readErr
		}
		if checkpoint.Generation != round.Checkpoint.Generation {
			return nil, fmt.Errorf("sync v2: initial page superseded")
		}
		round.Checkpoint = checkpoint
	}
	if err = hooks.PrepareRelated(ctx); err != nil {
		return nil, err
	}
	round.Checkpoint, err = reloadAfterRelated(ctx, store, round.Checkpoint)
	if err != nil {
		return nil, err
	}
	report(20)
	checkpoint := mergeRestoredSelection(round.Checkpoint, restoredSelection)
	// The second pass also catches new notification parents and pinned chats
	// discovered by metadata catchup. Online pushes continue after this round.
	caughtUp := false
	for pass := 0; pass < 3; pass++ {
		notificationStart := [3]int{20, 55, 80}[pass]
		notificationEnd := [3]int{30, 65, 83}[pass]
		chatStart := [3]int{30, 65, 83}[pass]
		chatEnd := [3]int{55, 80, 86}[pass]
		checkedParents := make(map[string]bool, len(checkpoint.NotificationIDs))
		for _, id := range checkpoint.NotificationIDs {
			checkedParents[id] = true
		}
		result.Notifications, err = c.SyncStreams(streamContext(notificationStart, notificationEnd), op, &pb.StateRequest{UserID: userID, ConversationIDs: checkpoint.NotificationIDs, Notifications: true, IncludeSelf: checkpoint.IncludeSelf}, round.Capabilities, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		report(notificationEnd)
		// A notification handler may itself have refreshed metadata in the queue.
		// Read its committed revision instead of publishing the prepared copy.
		checkpoint, err = store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		if checkpoint.Generation != round.Checkpoint.Generation {
			return nil, fmt.Errorf("sync v2: round superseded during notification consumption")
		}
		checkpoint = mergeRestoredSelection(checkpoint, restoredSelection)
		delta, err := hooks.ReadMetadata(ctx, checkpoint.Version, checkpoint.VersionID)
		if err != nil {
			return nil, err
		}
		metadataCtx := ctx
		chatCtx := streamContext(chatStart, chatEnd)
		if resume {
			prepared := &preparedChatStates{}
			metadataCtx = context.WithValue(ctx, preparedChatStatesKey{}, prepared)
			chatCtx = context.WithValue(chatCtx, preparedChatStatesKey{}, prepared)
		}
		checkpoint, err = c.ApplyMetadataDelta(metadataCtx, op, checkpoint, delta, round.Capabilities, store)
		if err != nil {
			return nil, err
		}
		restoredSelection = nil
		report(chatStart)
		result.Chats, err = c.SyncStreams(chatCtx, op, &pb.StateRequest{UserID: userID, ConversationIDs: checkpoint.SelectedIDs}, round.Capabilities, store, hooks.Commit)
		if err != nil {
			return nil, err
		}
		report(chatEnd)
		newParents := false
		for _, id := range checkpoint.NotificationIDs {
			if !checkedParents[id] {
				newParents = true
				break
			}
		}
		// Restored checkpoints already have a completed baseline. A second
		// full pass is needed only when catchup introduced notification parents.
		if (resume || pass >= 1) && !newParents {
			caughtUp = true
			break
		}
	}
	if !caughtUp {
		return nil, ErrCatchupPending
	}
	// Projection may take multiple batches on an existing large account. Report
	// only committed batches, reserving 99 for the durable ready checkpoint.
	ReportDetailProgress(ctx, "publishing", "conversations", 0, nil)
	publishStart := lastProgress
	if err = hooks.Publish(ctx, checkpoint, func(done, total int) {
		reportCount(ctx, "publishing", "conversations", done, total)
		if total > 0 {
			report(publishStart + (98-publishStart)*done/total)
		}
	}); err != nil {
		return nil, err
	}
	next := *checkpoint
	next.Stage = "ready"
	if caps.IncrementalRecovery {
		if err = persistPendingBeforeCursor(ctx, result, hooks); err != nil {
			return nil, err
		}
		next.RecoveryBaselineReady = true
	}
	if err = store.SaveConversationSyncV2(ctx, &next); err != nil {
		return nil, err
	}
	result.Checkpoint = &next
	if caps.IncrementalRecovery {
		return c.recoverChanges(ctx, op, &next, caps, store, hooks)
	}
	report(99)
	return result, nil
}

// Foreground activation may add selected conversations while directories are
// fetched. Continue from the committed checkpoint, never the pre-wait copy.
func reloadAfterRelated(ctx context.Context, store CoordinatorStore, before *model_struct.LocalConversationSyncV2) (*model_struct.LocalConversationSyncV2, error) {
	current, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if current == nil || current.Generation != before.Generation || current.UserID != before.UserID || current.PolicyVersion != before.PolicyVersion || current.SyncDomainID != before.SyncDomainID || current.RecoveryVersion != before.RecoveryVersion {
		return nil, fmt.Errorf("sync v2: background baseline replaced during related synchronization")
	}
	return current, nil
}

// A snapshot selects currently unread chats that need not appear in the metadata
// delta. Merge its IDs without replacing the durable message cursors or advancing
// the old metadata baseline. The following metadata transaction commits the union.
func mergeRestoredSelection(before *model_struct.LocalConversationSyncV2, selection *Selection) *model_struct.LocalConversationSyncV2 {
	if selection == nil {
		return before
	}
	next := *before
	merge := func(existing, fresh []string) model_struct.StringArray {
		out := append(model_struct.StringArray(nil), existing...)
		seen := make(map[string]bool, len(out))
		for _, id := range out {
			seen[id] = true
		}
		for _, id := range fresh {
			if !seen[id] {
				out = append(out, id)
				seen[id] = true
			}
		}
		return out
	}
	next.SelectedIDs = merge(before.SelectedIDs, selection.ChatIDs)
	next.NotificationIDs = merge(before.NotificationIDs, selection.NotificationIDs)
	next.IncludeSelf = before.IncludeSelf || selection.IncludeSelf
	return &next
}
