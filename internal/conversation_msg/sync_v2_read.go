package conversation_msg

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/protocol/sdkws"
	"gorm.io/gorm"
)

func (c *Conversation) doConversationSyncV2SelfRead(ctx context.Context, tips *sdkws.MarkAsReadTips) error {
	if err := c.syncV2.store.MarkConversationSyncV2LocalRead(ctx, tips.ConversationID, tips.Seqs); err != nil {
		return err
	}
	// Initial synchronization re-reads authoritative state after notification
	// consumption. Live receipts must do the same before publishing unread changes.
	if c.GetConversationSyncState().Phase != "ready" {
		return nil
	}
	if err := c.refreshConversationSyncV2ReadState(ctx, tips.ConversationID); err != nil {
		return err
	}
	c.unreadChangeTrigger(ctx, tips.ConversationID, false)
	return nil
}

// Read receipts refresh only an existing conversation. They do not activate
// chats or scan the loaded selection as a metadata catch-up would.
func (c *Conversation) refreshConversationSyncV2ReadState(ctx context.Context, id string) error {
	if _, err := c.db.GetConversation(ctx, id); errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	runtime := c.syncV2
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if caps == nil {
		return syncv2.ErrStatePending
	}
	before, err := runtime.store.GetConversationSyncV2(ctx)
	if err != nil {
		return err
	}
	epochs, err := runtime.store.GetConversationSyncV2Epochs(ctx, []string{id})
	if err != nil {
		return err
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	response, err := runtime.client.StatesBatched(ctx, ccontext.Info(ctx).OperationID(), &pb.StateRequest{UserID: before.UserID, ConversationIDs: []string{id}, IncludeDetails: true}, caps)
	if err != nil {
		return err
	}
	if len(response.UnavailableIDs) > 0 {
		if len(response.UnavailableIDs) != 1 || response.UnavailableIDs[0] != id || len(response.States) != 0 {
			return fmt.Errorf("sync v2: invalid unavailable read state")
		}
		c.conversationSyncMutex.Lock()
		defer c.conversationSyncMutex.Unlock()
		return runtime.store.InvalidateConversationSyncV2Stream(ctx, id)
	}
	if len(response.States) == 0 {
		return syncv2.ErrStatePending
	}
	if len(response.States) != 1 || response.States[0] == nil || response.States[0].ConversationID != id {
		return fmt.Errorf("sync v2: inconsistent read state")
	}
	if !response.States[0].Known || response.States[0].Conversation == nil {
		return syncv2.ErrStatePending
	}
	return runtime.store.RefreshConversationSyncV2ReadState(ctx, before, epochs[id], response.States[0])
}
