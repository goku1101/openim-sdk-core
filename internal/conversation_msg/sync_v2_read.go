package conversation_msg

import (
	"context"
	"github.com/openimsdk/protocol/sdkws"
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
	if err := c.refreshConversationSyncV2Metadata(ctx); err != nil {
		return err
	}
	c.unreadChangeTrigger(ctx, tips.ConversationID, false)
	return nil
}
