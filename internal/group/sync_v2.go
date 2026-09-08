package group

import (
	"context"
	"fmt"
)

// SetConversationSyncV2Invalidator is configured before notification workers.
// The native conversation runtime owns the negotiated mode and stream ID.
func (g *Group) SetConversationSyncV2Invalidator(invalidate func(context.Context, string) error) {
	g.conversationSyncV2Invalidator = invalidate
}

func (g *Group) invalidateConversationSyncV2(ctx context.Context, groupID string) error {
	if g.conversationSyncV2Invalidator == nil {
		return nil
	}
	if groupID == "" {
		return fmt.Errorf("sync v2: missing permission event group ID")
	}
	return g.conversationSyncV2Invalidator(ctx, groupID)
}
