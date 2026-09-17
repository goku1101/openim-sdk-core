package syncv2

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"golang.org/x/sync/errgroup"
)

// Fetch independent notification state batches together, then consume events in
// their original order. Epochs are captured before each request and checked again
// before consumption, because an earlier notification can invalidate a later one.
func (c *Client) prefetchNotificationStates(ctx context.Context, op string, req *pb.StateRequest, caps *pb.Capabilities, store BatchStreamStore) (*preparedChatStates, error) {
	result := &preparedChatStates{states: map[string]*pb.State{}, unavailable: map[string]bool{}, epochs: map[string]uint64{}}
	size := min(int(caps.MaxBatchSize), 500)
	for start := 0; start < len(req.ConversationIDs); start += 4 * size {
		end := min(start+4*size, len(req.ConversationIDs))
		count := (end - start + size - 1) / size
		results := make([]*preparedChatStates, count)
		epochsByBatch := make([]map[string]uint64, count)
		for index := 0; index < count; index++ {
			index := index
			parents := req.ConversationIDs[start+index*size : min(start+(index+1)*size, end)]
			ids := make([]string, 0, len(parents))
			for _, parent := range parents {
				id, err := pullStreamID(&pb.PullRequest{UserID: req.UserID, ConversationID: parent, Notifications: true})
				if err != nil {
					return nil, err
				}
				ids = append(ids, id)
			}
			epochs, err := store.GetConversationSyncV2Epochs(ctx, ids)
			if err != nil {
				return nil, err
			}
			epochsByBatch[index] = epochs
		}
		group, waveCtx := errgroup.WithContext(ctx)
		for index := 0; index < count; index++ {
			index := index
			parents := req.ConversationIDs[start+index*size : min(start+(index+1)*size, end)]
			epochs := epochsByBatch[index]
			group.Go(func() error {
				response, err := c.StatesBatched(waveCtx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: parents, Notifications: true}, caps)
				if err != nil {
					return err
				}
				batch := &preparedChatStates{states: map[string]*pb.State{}, unavailable: map[string]bool{}, epochs: epochs}
				for _, state := range response.States {
					if state == nil || batch.states[state.ConversationID] != nil {
						return fmt.Errorf("sync v2: invalid prefetched notification state")
					}
					if _, ok := epochs[state.ConversationID]; !ok {
						return fmt.Errorf("sync v2: notification state outside request")
					}
					batch.states[state.ConversationID] = state
				}
				for _, parent := range response.UnavailableIDs {
					streamID, streamErr := pullStreamID(&pb.PullRequest{UserID: req.UserID, ConversationID: parent, Notifications: true})
					if _, ok := epochs[streamID]; streamErr != nil || !ok || batch.states[streamID] != nil || batch.unavailable[parent] {
						return fmt.Errorf("sync v2: invalid unavailable notification")
					}
					batch.unavailable[parent] = true
				}
				results[index] = batch
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}
		for _, batch := range results {
			for id, state := range batch.states {
				result.states[id] = state
			}
			for id, epoch := range batch.epochs {
				result.epochs[id] = epoch
			}
			for id, value := range batch.unavailable {
				result.unavailable[id] = value
			}
		}
	}
	return result, nil
}
