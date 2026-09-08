package syncv2

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type CheckpointWriter interface {
	SaveConversationSyncV2(context.Context, *model_struct.LocalConversationSyncV2) error
}

// Round contains network results awaiting the conversation/message consumers.
// Preparing it must not advance their committed metadata or message cursors.
type Round struct {
	Enabled            bool
	Capabilities       *pb.Capabilities
	Selection          *Selection
	ChatStates         *pb.StateResponse
	NotificationStates *pb.StateResponse
	Checkpoint         *model_struct.LocalConversationSyncV2
}

func (c *Client) PrepareRound(ctx context.Context, op, userID string, loadedIDs []string, store CheckpointWriter) (*Round, error) {
	caps, err := c.Capabilities(ctx, op, &pb.UserRequest{UserID: userID})
	if err != nil {
		return nil, err
	}
	if !caps.ConversationSyncV2 {
		return &Round{Enabled: false, Capabilities: caps}, nil
	}
	if store == nil {
		return nil, fmt.Errorf("sync v2: checkpoint storage is required")
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: userID, PolicyVersion: PolicyVersion, Stage: "snapshot", SelectedIDs: append(model_struct.StringArray(nil), loadedIDs...)}
	if err = store.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		return nil, err
	}
	selection, err := c.ReadSnapshot(ctx, op, userID, min(caps.MaxPageSize, 500))
	if err != nil {
		return nil, err
	}
	// Existing devices keep already-loaded chats and local drafts/history. This
	// union controls requests only; absence never becomes a local delete action.
	seen := make(map[string]bool, len(selection.ChatIDs)+len(loadedIDs))
	for _, id := range selection.ChatIDs {
		seen[id] = true
	}
	for _, id := range loadedIDs {
		if !seen[id] {
			selection.ChatIDs = append(selection.ChatIDs, id)
			seen[id] = true
		}
	}
	checkpoint.SelectedIDs = selection.ChatIDs
	checkpoint.NotificationIDs = selection.NotificationIDs
	checkpoint.IncludeSelf = selection.IncludeSelf
	checkpoint.VersionID, checkpoint.Version = selection.VersionID, selection.Version
	checkpoint.Stage = "details"
	if err = store.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		return nil, err
	}
	chat, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: userID, ConversationIDs: selection.ChatIDs, IncludeDetails: true}, caps)
	if err != nil {
		return nil, err
	}
	notifications, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: userID, ConversationIDs: selection.NotificationIDs, Notifications: true, IncludeSelf: selection.IncludeSelf}, caps)
	if err != nil {
		return nil, err
	}
	return &Round{Enabled: true, Capabilities: caps, Selection: selection, ChatStates: chat, NotificationStates: notifications, Checkpoint: checkpoint}, nil
}
