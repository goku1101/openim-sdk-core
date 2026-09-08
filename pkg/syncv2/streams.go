package syncv2

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
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

// SyncStreams batches state reads and bounds chat pulls to four workers.
// Notifications and commit callbacks remain sequential. Epochs
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

		const (
			completed int8 = iota + 1
			pending
			denied
		)
		outcomes := make([]int8, len(ids))
		var commitMu sync.Mutex
		serialCommit := func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
			commitMu.Lock()
			defer commitMu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			return commit(ctx, p, messages)
		}
		scan := func(ctx context.Context, index int) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			id := ids[index]
			if unavailable[id] {
				outcomes[index] = denied
				return nil
			}
			if states[id] == nil || !states[id].Known {
				outcomes[index] = pending
				return nil
			}
			epoch, ok := epochs[id]
			if !ok {
				return fmt.Errorf("sync v2: missing captured epoch")
			}
			stream, err := c.syncStreamState(ctx, op, queries[id], caps, store, serialCommit, epoch, states[id])
			if RecoveryFor(err) == Unavailable {
				outcomes[index] = denied
				return nil
			}
			if errors.Is(err, ErrStatePending) {
				outcomes[index] = pending
				return nil
			}
			if err != nil {
				return err
			}
			if len(stream.PendingSeqs) > 0 {
				outcomes[index] = pending
			} else {
				outcomes[index] = completed
			}
			return nil
		}
		publish := func(i int) {
			id := ids[i]
			switch outcomes[i] {
			case completed:
				result.Completed++
			case pending:
				result.PendingIDs = append(result.PendingIDs, id)
			case denied:
				result.UnavailableIDs = append(result.UnavailableIDs, id)
			}
			advance()
		}
		if req.Notifications || len(ids) < 2 {
			for i := range ids {
				if err := scan(ctx, i); err != nil {
					return err
				}
				publish(i)
			}
		} else {
			workCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			jobs := make(chan int, len(ids))
			for i := range ids {
				jobs <- i
			}
			close(jobs)
			var workers sync.WaitGroup
			var first sync.Once
			var firstErr error
			for worker := 0; worker < min(4, len(ids)); worker++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for i := range jobs {
						if workCtx.Err() != nil {
							return
						}
						if err := scan(workCtx, i); err != nil {
							first.Do(func() { firstErr = err; cancel() })
							return
						}
					}
				}()
			}
			workers.Wait()
			if firstErr != nil {
				return firstErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if !req.Notifications && len(ids) >= 2 {
			for i := range ids {
				publish(i)
			}
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
