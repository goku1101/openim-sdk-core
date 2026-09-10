package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type ActivationStore interface {
	GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error)
	GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error)
	ActivateConversationSyncV2(context.Context, *model_struct.LocalConversationSyncV2, uint64, *pb.State) error
}

// ActivateConversation is only for explicit open, pinning or valid chat events.
// Ordinary profile/read notifications must not call this entry point.
func (c *Client) ActivateConversation(ctx context.Context, op, id string, caps *pb.Capabilities, store ActivationStore) (*model_struct.LocalConversationSyncV2, error) {
	if id == "" || store == nil {
		return nil, fmt.Errorf("sync v2: invalid activation")
	}
	for attempt := 0; attempt < 3; attempt++ {
		current, err := c.activateConversationAttempt(ctx, op, id, caps, store)
		if err == nil {
			return current, nil
		}
		if !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || attempt == 2 {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("sync v2: activation retry exhausted")
}

func (c *Client) activateConversationAttempt(ctx context.Context, op, id string, caps *pb.Capabilities, store ActivationStore) (*model_struct.LocalConversationSyncV2, error) {
	before, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	epochs, err := store.GetConversationSyncV2Epochs(ctx, []string{id})
	if err != nil {
		return nil, err
	}
	response, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: before.UserID, ConversationIDs: []string{id}, IncludeDetails: true}, caps)
	if err != nil {
		return nil, err
	}
	if len(response.UnavailableIDs) > 0 {
		return nil, &APIError{ErrCode: 7, ErrMsg: "SYNC_V2_CONVERSATION_UNAVAILABLE"}
	}
	if len(response.States) == 0 {
		return nil, ErrStatePending
	}
	if len(response.States) != 1 || response.States[0] == nil || response.States[0].ConversationID != id {
		return nil, fmt.Errorf("sync v2: inconsistent activation state")
	}
	state := response.States[0]
	if !state.Known || state.Conversation == nil {
		return nil, ErrStatePending
	}
	if err = store.ActivateConversationSyncV2(ctx, before, epochs[id], state); err != nil {
		return nil, err
	}
	current, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if current.Generation != before.Generation || current.Revision != before.Revision+1 {
		return nil, fmt.Errorf("%w: activation superseded", sdkerrs.ErrSyncV2StateChanged)
	}
	return current, nil
}
