package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
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
	states, err := c.metadataStatesForDelta(ctx, op, before.UserID, refreshed, delta, caps, store)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		if affectedIDs == nil {
			err = store.ApplyConversationSyncV2Metadata(ctx, before, plan.Next, states, plan.DeletedIDs)
		} else {
			err = store.(MetadataSubsetStore).ApplyConversationSyncV2MetadataSubset(ctx, before, plan.Next, states, plan.DeletedIDs, refreshed)
		}
		if err == nil {
			break
		}
		if attempt >= 2 || !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
			return nil, err
		}
		current, readErr := store.GetConversationSyncV2(ctx)
		if readErr != nil {
			return nil, readErr
		}
		// A changed checkpoint needs a new metadata delta, not a replay of this one.
		if current.Revision != before.Revision || current.Generation != before.Generation {
			return nil, err
		}
		epochs, readErr := store.GetConversationSyncV2Epochs(ctx, append(append([]string(nil), refreshed...), plan.DeletedIDs...))
		if readErr != nil {
			return nil, readErr
		}
		for _, id := range plan.DeletedIDs {
			if epochs[id] != plan.Next.StateEpochs[id] {
				return nil, err
			}
		}
		changed := make(map[string]bool)
		var ids []string
		for _, id := range refreshed {
			if epochs[id] != plan.Next.StateEpochs[id] {
				changed[id] = true
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil, err
		}
		fresh, readErr := c.metadataStatesBatched(ctx, op, before.UserID, ids, caps)
		if readErr != nil {
			return nil, readErr
		}
		// Retain unaffected results. The transaction rechecks captured epochs,
		// so a further notification can never make stale permissions commit.
		nextStates := &pb.StateResponse{}
		for _, state := range states.States {
			if !changed[state.ConversationID] {
				nextStates.States = append(nextStates.States, state)
			}
		}
		for _, id := range states.UnavailableIDs {
			if !changed[id] {
				nextStates.UnavailableIDs = append(nextStates.UnavailableIDs, id)
			}
		}
		nextStates.States = append(nextStates.States, fresh.States...)
		nextStates.UnavailableIDs = append(nextStates.UnavailableIDs, fresh.UnavailableIDs...)
		states = nextStates
		for _, id := range ids {
			plan.Next.StateEpochs[id] = epochs[id]
		}
	}

	current, err := store.GetConversationSyncV2(ctx)
	if err != nil {
		return nil, err
	}
	if current.Revision != before.Revision+1 || current.Generation != before.Generation || current.VersionID != plan.Next.VersionID || current.Version != plan.Next.Version {
		return nil, fmt.Errorf("sync v2: metadata round superseded")
	}
	if prepared, ok := ctx.Value(preparedChatStatesKey{}).(*preparedChatStates); ok {
		prepared.states = make(map[string]*pb.State, len(states.States))
		for _, state := range states.States {
			prepared.states[state.ConversationID] = state
		}
		prepared.unavailable = make(map[string]bool, len(states.UnavailableIDs))
		for _, id := range states.UnavailableIDs {
			prepared.unavailable[id] = true
		}
		prepared.epochs = current.StateEpochs
	}
	return current, nil
}

// During recovery the metadata version identifies changed details. Fresh state
// remains mandatory for tails, reads and permissions, which have separate clocks.
type cachedMetadataStore interface {
	GetConversationSyncV2CachedDetails(context.Context, []string) (map[string]*conversation.Conversation, error)
}

func (c *Client) metadataStatesForDelta(ctx context.Context, op, userID string, ids []string, delta *conversation.GetIncrementalConversationResp, caps *pb.Capabilities, store MetadataStore) (*pb.StateResponse, error) {
	cachedStore, ok := store.(cachedMetadataStore)
	if _, warm := ctx.Value(preparedChatStatesKey{}).(*preparedChatStates); !warm || !ok {
		return c.metadataStatesBatched(ctx, op, userID, ids, caps)
	}
	cached, err := cachedStore.GetConversationSyncV2CachedDetails(ctx, ids)
	if err != nil {
		return nil, err
	}
	changed := make(map[string]bool)
	for _, row := range delta.Update {
		changed[row.ConversationID] = true
	}
	for _, row := range delta.Insert {
		changed[row.ConversationID] = true
	}
	var details, statesOnly []string
	for _, id := range ids {
		if changed[id] || cached[id] == nil {
			details = append(details, id)
		} else {
			statesOnly = append(statesOnly, id)
		}
	}
	result, err := c.metadataStatesWithDetails(ctx, op, userID, statesOnly, caps, false)
	if err != nil {
		return nil, err
	}
	for _, state := range result.States {
		state.Conversation = cached[state.ConversationID]
	}
	fresh, err := c.metadataStatesBatched(ctx, op, userID, details, caps)
	if err != nil {
		return nil, err
	}
	result.States = append(result.States, fresh.States...)
	result.UnavailableIDs = append(result.UnavailableIDs, fresh.UnavailableIDs...)
	return result, nil
}
