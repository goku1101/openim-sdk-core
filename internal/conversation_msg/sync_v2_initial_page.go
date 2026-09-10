package conversation_msg

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// Prepare a readable first page before the full related-data/history catchup.
// This does not emit OnSyncServerFinish or open the application's write gate.
func (c *Conversation) prepareConversationSyncV2InitialPage(ctx context.Context, round *syncv2.Round) error {
	const pageSize = 50
	drafts := make(map[string]int64)
	ids := round.Checkpoint.SelectedIDs
	for start := 0; start < len(ids); start += 300 {
		rows, err := c.db.GetMultipleConversationDB(ctx, ids[start:min(start+300, len(ids))])
		if err != nil {
			return err
		}
		for _, row := range rows {
			drafts[row.ConversationID] = row.DraftTextTime
		}
	}
	initialIDs := syncv2.InitialPageIDs(round.ChatStates.States, drafts, pageSize)
	op := ccontext.Info(ctx).OperationID()
	notifications, err := c.syncV2.client.SyncStreams(ctx, op, &pb.StateRequest{UserID: c.loginUserID, ConversationIDs: initialIDs, Notifications: true, IncludeSelf: true}, round.Capabilities, c.syncV2.store, c.ConsumeConversationSyncV2)
	if err != nil {
		return err
	}
	if len(notifications.PendingIDs) > 0 || len(notifications.UnavailableIDs) > 0 {
		return nil // Unconfirmed notification boundaries must wait for full sync.
	}
	chats, err := c.syncV2.client.SyncStreams(ctx, op, &pb.StateRequest{UserID: c.loginUserID, ConversationIDs: initialIDs}, round.Capabilities, c.syncV2.store, c.ConsumeConversationSyncV2)
	if err != nil {
		return err
	}
	if len(chats.PendingIDs) > 0 || len(chats.UnavailableIDs) > 0 {
		return nil
	}
	current, err := c.syncV2.store.GetConversationSyncV2(ctx)
	if err != nil {
		return err
	}
	if current.Generation != round.Checkpoint.Generation {
		return fmt.Errorf("sync v2: initial page superseded")
	}
	projection := *current
	projection.SelectedIDs = initialIDs
	if err := c.publishConversationSyncV2(ctx, &projection); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state := c.GetConversationSyncState()
	state.InitialDataReady = true
	c.syncV2.status.Store(&state)
	// The unchanged percentage prompts listeners to read the separate native flag.
	c.ConversationListener().OnSyncServerProgress(state.Progress)
	return nil
}
