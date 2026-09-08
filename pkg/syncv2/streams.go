package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type BatchStreamStore interface {
	StreamStore
	GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error)
}

// Pending and unavailable streams remain separate from successfully scanned
// streams. Neither outcome authorizes deleting their local history.
type StreamResults struct {
	Completed      int
	PendingIDs     []string
	UnavailableIDs []string
}

// SyncStreams batches state reads while consuming streams sequentially. Epochs
// are captured before each network batch, not after it or just before its pull.
func (c *Client) SyncStreams(ctx context.Context, op string, req *pb.StateRequest, caps *pb.Capabilities, store BatchStreamStore, commit CommitStream) (*StreamResults, error) {
	if req == nil || caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 || store == nil || commit == nil || req.IncludeDetails || req.IncludeSelf && !req.Notifications {
		return nil, fmt.Errorf("sync v2: invalid stream batch")
	}
	result := new(StreamResults)
	processed, total := 0, len(req.ConversationIDs)
	if req.IncludeSelf {
		total++
	}
	progress, _ := ctx.Value(streamProgressKey{}).(func(int, int))
	advance := func() {
		processed++
		if progress != nil {
			progress(processed, total)
		}
	}
	consume := func(parents []string, self bool) error {
		queries := make(map[string]*pb.PullRequest, len(parents)+1)
		ids := make([]string, 0, len(parents)+1)
		add := func(parent string, self bool) error {
			query := &pb.PullRequest{UserID: req.UserID, ConversationID: parent, Notifications: req.Notifications, Self: self}
			id, err := pullStreamID(query)
			if err != nil {
				return err
			}
			if _, ok := queries[id]; ok {
				return fmt.Errorf("sync v2: duplicate stream ID")
			}
			ids = append(ids, id)
			queries[id] = query
			return nil
		}
		for _, id := range parents {
			if err := add(id, false); err != nil {
				return err
			}
		}
		if self {
			if err := add("", true); err != nil {
				return err
			}
		}
		epochs, err := store.GetConversationSyncV2Epochs(ctx, ids)
		if err != nil {
			return err
		}
		response, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: parents, Notifications: req.Notifications, IncludeSelf: self}, caps)
		if err != nil {
			return err
		}
		states := make(map[string]*pb.State, len(response.States))
		for _, state := range response.States {
			if state == nil || queries[state.ConversationID] == nil || states[state.ConversationID] != nil {
				return fmt.Errorf("sync v2: inconsistent batch state")
			}
			states[state.ConversationID] = state
		}
		unavailable := make(map[string]bool, len(response.UnavailableIDs))
		for _, parent := range response.UnavailableIDs {
			query := &pb.PullRequest{UserID: req.UserID, ConversationID: parent, Notifications: req.Notifications}
			id, err := pullStreamID(query)
			if err != nil || queries[id] == nil || states[id] != nil || unavailable[id] {
				return fmt.Errorf("sync v2: inconsistent unavailable stream")
			}
			unavailable[id] = true
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			if unavailable[id] {
				result.UnavailableIDs = append(result.UnavailableIDs, id)
				advance()
				continue
			}
			if states[id] == nil || !states[id].Known {
				result.PendingIDs = append(result.PendingIDs, id)
				advance()
				continue
			}
			epoch, ok := epochs[id]
			if !ok {
				return fmt.Errorf("sync v2: missing captured epoch")
			}
			stream, err := c.syncStreamState(ctx, op, queries[id], caps, store, commit, epoch, states[id])
			if errors.Is(err, ErrStatePending) {
				result.PendingIDs = append(result.PendingIDs, id)
				advance()
				continue
			}
			if err != nil {
				return err
			}
			if len(stream.PendingSeqs) > 0 {
				result.PendingIDs = append(result.PendingIDs, id)
			} else {
				result.Completed++
			}
			advance()
		}
		return nil
	}
	size := min(int(caps.MaxBatchSize), 500)
	for start := 0; start < len(req.ConversationIDs); start += size {
		if err := consume(req.ConversationIDs[start:min(start+size, len(req.ConversationIDs))], false); err != nil {
			return nil, err
		}
	}
	if req.IncludeSelf {
		if err := consume(nil, true); err != nil {
			return nil, err
		}
	}
	return result, nil
}
