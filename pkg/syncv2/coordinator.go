package syncv2

import (
	"context"
	"errors"
	"fmt"

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
	PrepareInitial func(context.Context, *Round) error
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
	round, err := c.prepareRoundWithCapabilities(ctx, op, userID, store, caps, report)
	if err != nil {
		return nil, err
	}
	round, err = commitPreparedDetails(ctx, round, store)
	if err != nil {
		return nil, err
	}
	result := &SyncResult{Enabled: round.Enabled, Capabilities: round.Capabilities, Checkpoint: round.Checkpoint}
	if !round.Enabled {
		return result, nil
	}
	report(10)
	if hooks.PrepareInitial != nil {
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
	report(20)
	checkpoint := round.Checkpoint
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
		delta, err := hooks.ReadMetadata(ctx, checkpoint.Version, checkpoint.VersionID)
		if err != nil {
			return nil, err
		}
		checkpoint, err = c.ApplyMetadataDelta(ctx, op, checkpoint, delta, round.Capabilities, store)
		if err != nil {
			return nil, err
		}
		report(chatStart)
		result.Chats, err = c.SyncStreams(streamContext(chatStart, chatEnd), op, &pb.StateRequest{UserID: userID, ConversationIDs: checkpoint.SelectedIDs}, round.Capabilities, store, hooks.Commit)
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
		if pass >= 1 && !newParents {
			caughtUp = true
			break
		}
	}
	if !caughtUp {
		return nil, ErrCatchupPending
	}
	// Projection may take multiple batches on an existing large account. Report
	// only committed batches, reserving 99 for the durable ready checkpoint.
	publishStart := lastProgress
	if err = hooks.Publish(ctx, checkpoint, func(done, total int) {
		if total > 0 {
			report(publishStart + (98-publishStart)*done/total)
		}
	}); err != nil {
		return nil, err
	}
	next := *checkpoint
	next.Stage = "ready"
	if err = store.SaveConversationSyncV2(ctx, &next); err != nil {
		return nil, err
	}
	result.Checkpoint = &next
	report(99)
	return result, nil
}
