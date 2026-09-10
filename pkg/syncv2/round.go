package syncv2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type RoundStore interface {
	SaveConversationSyncV2(context.Context, *model_struct.LocalConversationSyncV2) error
	GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error)
}

type DetailsStore interface {
	RoundStore
	ApplyConversationSyncV2Details(context.Context, *model_struct.LocalConversationSyncV2, *pb.StateResponse) error
	GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error)
}

// PrepareDetails advances only after the database commits the details and
// checkpoint together. Message delivery and final completion remain separate.
func (c *Client) PrepareDetails(ctx context.Context, op, userID string, store DetailsStore) (*Round, error) {
	round, err := c.PrepareRound(ctx, op, userID, store)
	if err != nil {
		return nil, err
	}
	return commitPreparedDetails(ctx, round, store)
}

func commitPreparedDetails(ctx context.Context, round *Round, store DetailsStore) (*Round, error) {
	if !round.Enabled {
		return round, nil
	}
	if err := store.ApplyConversationSyncV2Details(ctx, round.Checkpoint, round.ChatStates); err != nil {
		return nil, err
	}
	checkpoint, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if checkpoint.Generation != round.Checkpoint.Generation || checkpoint.SnapshotID != round.Selection.SnapshotID || checkpoint.Stage != "messages" {
		return nil, fmt.Errorf("sync v2: details round superseded")
	}
	round.Checkpoint = checkpoint
	return round, nil
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

func (c *Client) PrepareRound(ctx context.Context, op, userID string, store RoundStore) (*Round, error) {
	caps, err := c.Capabilities(ctx, op, &pb.UserRequest{UserID: userID})
	if err != nil {
		return nil, err
	}
	return c.prepareRoundWithCapabilities(ctx, op, userID, store, caps, nil)
}

func (c *Client) prepareRoundWithCapabilities(ctx context.Context, op, userID string, store RoundStore, caps *pb.Capabilities, progress func(int)) (*Round, error) {
	if !caps.ConversationSyncV2 {
		return &Round{Enabled: false, Capabilities: caps}, nil
	}
	if store == nil {
		return nil, fmt.Errorf("sync v2: checkpoint storage is required")
	}
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, err
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: userID, Generation: hex.EncodeToString(generation[:]), PolicyVersion: PolicyVersion, Stage: "snapshot"}
	if err := store.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		return nil, err
	}
	selection, err := c.ReadSnapshot(ctx, op, userID, min(caps.MaxPageSize, 500))
	if err != nil {
		return nil, err
	}
	// Local history remains in its own tables. A new round requests only the
	// server selection; explicit activation can extend this round independently.
	checkpoint.SelectedIDs = selection.ChatIDs
	checkpoint.SnapshotID = selection.SnapshotID
	checkpoint.NotificationIDs = selection.NotificationIDs
	checkpoint.IncludeSelf = selection.IncludeSelf
	checkpoint.VersionID, checkpoint.Version = selection.VersionID, selection.Version
	checkpoint.Stage = "details"
	checkpoint.StateEpochs, err = store.GetConversationSyncV2Epochs(ctx, checkpoint.SelectedIDs)
	if err != nil {
		return nil, err
	}
	if err = store.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		return nil, err
	}
	if progress != nil {
		progress(2)
	}
	stateContext := func(start, end int) context.Context {
		return withStateProgress(ctx, func(done, total int) {
			if progress != nil && total > 0 {
				progress(start + (end-start)*done/total)
			}
		})
	}
	chat, err := c.StatesBatched(stateContext(2, 6), op, &pb.StateRequest{UserID: userID, ConversationIDs: selection.ChatIDs, IncludeDetails: true}, caps)
	if err != nil {
		return nil, err
	}
	notifications, err := c.StatesBatched(stateContext(6, 9), op, &pb.StateRequest{UserID: userID, ConversationIDs: selection.NotificationIDs, Notifications: true, IncludeSelf: selection.IncludeSelf}, caps)
	if err != nil {
		return nil, err
	}
	return &Round{Enabled: true, Capabilities: caps, Selection: selection, ChatStates: chat, NotificationStates: notifications, Checkpoint: checkpoint}, nil
}
