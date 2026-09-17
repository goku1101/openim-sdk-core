package syncv2

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type PendingStateStore interface {
	GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error)
	GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error)
	RefreshConversationSyncV2ReadState(context.Context, *model_struct.LocalConversationSyncV2, uint64, *pb.State) error
}

// RevalidatePendingState refreshes bounds without activating deferred history.
// Unknown state remains pending; foreground reads still enforce their own checks.
func (c *Client) RevalidatePendingState(ctx context.Context, op, id string, caps *pb.Capabilities, store PendingStateStore) (*pb.State, error) {
	if id == "" || store == nil {
		return nil, fmt.Errorf("sync v2: invalid pending state")
	}
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
	// A revoked/unavailable target is not promoted or interpreted as empty history.
	if len(response.UnavailableIDs) > 0 {
		return nil, &APIError{ErrCode: 7, ErrMsg: "SYNC_V2_CONVERSATION_UNAVAILABLE"}
	}
	if len(response.States) != 1 || response.States[0] == nil || response.States[0].ConversationID != id {
		return nil, fmt.Errorf("sync v2: inconsistent pending state response")
	}
	state := response.States[0]
	if !state.Known || state.Conversation == nil {
		return nil, ErrStatePending
	}
	if err = store.RefreshConversationSyncV2ReadState(ctx, before, epochs[id], state); err != nil {
		return nil, err
	}
	return state, nil
}
