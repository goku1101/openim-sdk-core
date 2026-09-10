package syncv2

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"golang.org/x/sync/errgroup"
)

// Metadata refreshes keep the entire authoritative selection, but overlap two
// independent HTTP batches. A failed wave is cancelled and never reaches the
// database. Merge and progress publication remain ordered on the caller.
func (c *Client) metadataStatesBatched(ctx context.Context, op, userID string, ids []string, caps *pb.Capabilities) (*pb.StateResponse, error) {
	if caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 {
		return nil, fmt.Errorf("sync v2: missing request limits")
	}
	batchSize := min(int(caps.MaxBatchSize), 500)
	out := new(pb.StateResponse)
	progress, _ := ctx.Value(stateProgressKey{}).(func(int, int))
	for start := 0; start < len(ids); start += 2 * batchSize {
		end := min(start+2*batchSize, len(ids))
		batches := (end - start + batchSize - 1) / batchSize
		results := make([]*pb.StateResponse, batches)
		group, waveCtx := errgroup.WithContext(ctx)
		// Per-request progress would restart at zero for each concurrent batch.
		// Publish cumulative progress only after the complete wave has succeeded.
		waveCtx = withStateProgress(waveCtx, nil)
		for index := 0; index < batches; index++ {
			index := index
			batchStart := start + index*batchSize
			selected := ids[batchStart:min(batchStart+batchSize, end)]
			group.Go(func() error {
				result, err := c.StatesBatched(waveCtx, op, &pb.StateRequest{UserID: userID, ConversationIDs: selected, IncludeDetails: true}, caps)
				if err != nil {
					return err
				}
				results[index] = result
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}
		for _, result := range results {
			out.States = append(out.States, result.States...)
			out.UnavailableIDs = append(out.UnavailableIDs, result.UnavailableIDs...)
		}
		if progress != nil {
			progress(end, len(ids))
		}
	}
	return out, nil
}
