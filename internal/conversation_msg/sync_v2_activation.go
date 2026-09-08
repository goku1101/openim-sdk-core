package conversation_msg

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
)

// Called outside the Conversation consumer: publishing awaits that queue.
func (c *Conversation) activateConversationSyncV2(ctx context.Context, id string) (*model_struct.LocalConversation, error) {
	runtime := c.syncV2
	runtime.runMu.Lock()
	defer runtime.runMu.Unlock()
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if runtime.mode.Load() != syncV2Enabled || caps == nil {
		return nil, syncv2.ErrStatePending
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	checkpoint, err := runtime.client.ActivateConversation(ctx, ccontext.Info(ctx).OperationID(), id, caps, runtime.store)
	if err != nil {
		return nil, err
	}
	// Only update this chat's display projection. The full persisted selection
	// remains intact and is not replaced with this one-item view.
	projection := *checkpoint
	projection.SelectedIDs = []string{id}
	if err = c.publishConversationSyncV2(ctx, &projection); err != nil {
		return nil, err
	}
	return c.db.GetConversation(ctx, id)
}
