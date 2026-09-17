package conversation_msg

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/tools/log"
)

func (c *Conversation) activateConversationSyncV2(ctx context.Context, id string) (*model_struct.LocalConversation, error) {
	ctx = syncv2.WithForegroundRead(ctx)
	for attempt := 0; ; attempt++ {
		result, err := c.activateConversationSyncV2Once(ctx, id)
		stateChanged := err != nil && (errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || strings.Contains(err.Error(), "sync v2: local state changed")) && !strings.Contains(err.Error(), "newer local visibility action")
		if err == nil || !stateChanged || attempt == 5 {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		log.ZWarn(ctx, "sync v2 activation state changed; retrying", err, "conversationID", id, "attempt", attempt+1)
		time.Sleep(time.Duration(25*(attempt+1)) * time.Millisecond)
	}
}

// Called outside the Conversation consumer: publishing awaits that queue.
func (c *Conversation) activateConversationSyncV2Once(ctx context.Context, id string) (*model_struct.LocalConversation, error) {
	runtime := c.syncV2
	if err := runtime.acquireRun(ctx); err != nil {
		return nil, err
	}
	defer runtime.releaseRun()
	// Detail activation and conversation-change notifications share the same
	// local stream epochs. Serialize the network read/commit window so a burst
	// of notification events cannot invalidate the response between those two
	// steps. Projection is intentionally outside this lock because it waits for
	// the conversation event queue.
	c.conversationSyncMutex.Lock()
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if runtime.mode.Load() != syncV2Enabled || caps == nil {
		c.conversationSyncMutex.Unlock()
		return nil, syncv2.ErrStatePending
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	visibility, err := runtime.store.GetConversationSyncV2Visibility(ctx, id)
	if err != nil {
		c.conversationSyncMutex.Unlock()
		return nil, err
	}
	checkpoint, err := runtime.client.ActivateConversation(ctx, ccontext.Info(ctx).OperationID(), id, caps, runtime.store)
	if err != nil {
		c.conversationSyncMutex.Unlock()
		return nil, err
	}
	if err = runtime.store.RevealConversationSyncV2(ctx, id, visibility.Revision); err != nil {
		c.conversationSyncMutex.Unlock()
		return nil, err
	}
	c.conversationSyncMutex.Unlock()
	// Only update this chat's display projection. The full persisted selection
	// remains intact and is not replaced with this one-item view.
	projection := *checkpoint
	projection.SelectedIDs = []string{id}
	if err = c.publishConversationSyncV2(ctx, &projection); err != nil {
		return nil, err
	}
	return c.db.GetConversation(ctx, id)
}
