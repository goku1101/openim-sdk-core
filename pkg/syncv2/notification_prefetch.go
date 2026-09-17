package syncv2

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
	"sync"
)

type notificationPullKey struct{}
type notificationPull struct {
	request  *pb.PullRequest
	response *pb.PullResponse
	err      error
	done     chan struct{}
}

// Only the next four first pages are held in memory. Reads use the existing
// account admission gate; handlers and durable cursors keep their original order.
func (c *Client) consumeNotificationsPrefetched(ctx context.Context, op string, ids []string, queries map[string]*pb.PullRequest, states map[string]*pb.State, accepted map[string]*model_struct.LocalConversationSyncV2Stream, caps *pb.Capabilities, store BatchStreamStore, scan func(context.Context, int) error, publish func(int)) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	futures := make([]*notificationPull, len(ids))
	start := func(i int) {
		id := ids[i]
		stream, state := accepted[id], states[id]
		if stream == nil || state == nil || len(stream.PendingSeqs) > 0 {
			return
		}
		upper := state.MaxSeq
		if state.PermissionMaxSeq > 0 {
			upper = min(upper, state.PermissionMaxSeq)
		}
		lower := max(int64(1), state.MinSeq, stream.ScannedSeq+1)
		if lower > upper {
			return
		}
		q := proto.Clone(queries[id]).(*pb.PullRequest)
		q.StateToken = stream.StateToken
		end := lower + min(int64(min(int(caps.MaxBatchSize), 500)-1), upper-lower)
		for n := lower; ; n++ {
			q.Seqs = append(q.Seqs, n)
			if n == end {
				break
			}
		}
		future := &notificationPull{request: q, done: make(chan struct{})}
		futures[i] = future
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer close(future.done)
			future.response, future.err = c.PullBatched(ctx, op, q, caps)
		}()
	}
	for i := 0; i < min(4, len(ids)); i++ {
		start(i)
	}
	for i, id := range ids {
		runCtx := ctx
		if future := futures[i]; future != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-future.done:
			}
			// Earlier notifications can revoke another stream while its read is in flight.
			epochs, err := store.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				return err
			}
			if epochs[id] != accepted[id].Epoch {
				return sdkerrs.ErrSyncV2StateChanged
			}
			runCtx = context.WithValue(ctx, notificationPullKey{}, future)
		}
		if err := scan(runCtx, i); err != nil {
			return err
		}
		futures[i] = nil
		publish(i)
		if i+4 < len(ids) {
			start(i + 4)
		}
	}
	return nil
}
