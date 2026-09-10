package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

var ErrMetadataSnapshotRequired = errors.New("sync v2: metadata requires a new snapshot")

type MetadataPlan struct {
	Next       *model_struct.LocalConversationSyncV2
	DeletedIDs []string
}

// Metadata changes are not chat activation events, except for pinning. New or
// changed parents still join the independent notification selection.
func PlanMetadataDelta(before *model_struct.LocalConversationSyncV2, delta *conversation.GetIncrementalConversationResp) (*MetadataPlan, error) {
	if before == nil || delta == nil || before.UserID == "" {
		return nil, fmt.Errorf("sync v2: missing metadata checkpoint")
	}
	if delta.Full || delta.VersionID != before.VersionID || delta.Version < before.Version {
		return nil, ErrMetadataSnapshotRequired
	}
	plan := &MetadataPlan{Next: new(model_struct.LocalConversationSyncV2)}
	*plan.Next = *before
	plan.Next.VersionID, plan.Next.Version = delta.VersionID, delta.Version
	plan.Next.Stage = "messages"
	plan.Next.SelectedIDs = nil
	plan.Next.NotificationIDs = append(model_struct.StringArray(nil), before.NotificationIDs...)
	deleted := make(map[string]bool, len(delta.Delete))
	for _, id := range delta.Delete {
		if id == "" {
			return nil, fmt.Errorf("sync v2: empty deleted ID")
		}
		if !deleted[id] {
			plan.DeletedIDs = append(plan.DeletedIDs, id)
		}
		deleted[id] = true
	}
	loaded := make(map[string]bool, len(before.SelectedIDs))
	for _, id := range before.SelectedIDs {
		if !deleted[id] && !loaded[id] {
			plan.Next.SelectedIDs = append(plan.Next.SelectedIDs, id)
			loaded[id] = true
		}
	}
	notifications := make(map[string]bool, len(before.NotificationIDs))
	for _, id := range before.NotificationIDs {
		notifications[id] = true
	}
	changes := append(append([]*conversation.Conversation(nil), delta.Update...), delta.Insert...)
	for _, change := range changes {
		if change == nil || change.ConversationID == "" {
			return nil, fmt.Errorf("sync v2: invalid metadata change")
		}
		id := change.ConversationID
		// Preserve the existing incremental protocol's explicit-delete precedence.
		if deleted[id] {
			continue
		}
		if !notifications[id] {
			plan.Next.NotificationIDs = append(plan.Next.NotificationIDs, id)
			notifications[id] = true
		}
		if change.IsPinned && !loaded[id] {
			plan.Next.SelectedIDs = append(plan.Next.SelectedIDs, id)
			loaded[id] = true
		}
	}
	return plan, nil
}

type MetadataStore interface {
	GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error)
	ApplyConversationSyncV2Metadata(context.Context, *model_struct.LocalConversationSyncV2, *model_struct.LocalConversationSyncV2, *pb.StateResponse, []string) error
	GetConversationSyncV2(context.Context) (*model_struct.LocalConversationSyncV2, error)
}

type MetadataSubsetStore interface {
	MetadataStore
	ApplyConversationSyncV2MetadataSubset(context.Context, *model_struct.LocalConversationSyncV2, *model_struct.LocalConversationSyncV2, *pb.StateResponse, []string, []string) error
}

// ApplyMetadataDelta re-reads authoritative boundaries even when metadata did
// not change: message arrival and read receipts need not advance that version.
func (c *Client) ApplyMetadataDelta(ctx context.Context, op string, before *model_struct.LocalConversationSyncV2, delta *conversation.GetIncrementalConversationResp, caps *pb.Capabilities, store MetadataStore) (*model_struct.LocalConversationSyncV2, error) {
	return c.applyMetadataDelta(ctx, op, before, delta, caps, store, nil)
}

// ApplyMetadataForIDs refreshes mutated/notified and incrementally changed chats
// only when selected; pin activation still comes from the authoritative delta.
func (c *Client) ApplyMetadataForIDs(ctx context.Context, op string, before *model_struct.LocalConversationSyncV2, delta *conversation.GetIncrementalConversationResp, caps *pb.Capabilities, store MetadataSubsetStore, ids []string) (*model_struct.LocalConversationSyncV2, error) {
	if len(ids) == 0 {
		return c.ApplyMetadataDelta(ctx, op, before, delta, caps, store)
	}
	return c.applyMetadataDelta(ctx, op, before, delta, caps, store, ids)
}

func (c *Client) applyMetadataDelta(ctx context.Context, op string, before *model_struct.LocalConversationSyncV2, delta *conversation.GetIncrementalConversationResp, caps *pb.Capabilities, store MetadataStore, affectedIDs []string) (*model_struct.LocalConversationSyncV2, error) {
	if store == nil {
		return nil, fmt.Errorf("sync v2: metadata persistence unavailable")
	}
	plan, err := PlanMetadataDelta(before, delta)
	if err != nil {
		return nil, err
	}
	refreshed := plan.Next.SelectedIDs
	if affectedIDs != nil {
		changed := make(map[string]bool)
		for _, id := range affectedIDs {
			if id == "" {
				return nil, fmt.Errorf("sync v2: empty affected conversation ID")
			}
			changed[id] = true
		}
		for _, change := range delta.Update {
			changed[change.ConversationID] = true
		}
		for _, change := range delta.Insert {
			changed[change.ConversationID] = true
		}
		refreshed = make([]string, 0, len(changed))
		for _, id := range plan.Next.SelectedIDs {
			if changed[id] {
				refreshed = append(refreshed, id)
			}
		}
	}
	plan.Next.StateEpochs, err = store.GetConversationSyncV2Epochs(ctx, append(append([]string(nil), plan.Next.SelectedIDs...), plan.DeletedIDs...))
	if err != nil {
		return nil, err
	}
	states, err := c.metadataStatesBatched(ctx, op, before.UserID, refreshed, caps)
	if err != nil {
		return nil, err
	}
	if affectedIDs == nil {
		err = store.ApplyConversationSyncV2Metadata(ctx, before, plan.Next, states, plan.DeletedIDs)
	} else {
		err = store.(MetadataSubsetStore).ApplyConversationSyncV2MetadataSubset(ctx, before, plan.Next, states, plan.DeletedIDs, refreshed)
	}
	if err != nil {
		return nil, err
	}
	current, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if current.Revision != before.Revision+1 || current.Generation != before.Generation || current.VersionID != plan.Next.VersionID || current.Version != plan.Next.Version {
		return nil, fmt.Errorf("sync v2: metadata round superseded")
	}
	return current, nil
}
