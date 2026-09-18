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

// BatchStateStore may accept notification states in one local transaction.
// Implementations must preserve the same epoch checks as the single-state API.
type BatchStateStore interface {
	AcceptConversationSyncV2States(context.Context, []*model_struct.LocalConversationSyncV2Stream) (map[string]*model_struct.LocalConversationSyncV2Stream, error)
}

// Native stores can confirm unchanged streams in one read without reserving
// thousands of epochs for scans that have no work to commit.
// Carries state already validated and committed by metadata, with the exact
// epochs from that transaction, never epochs sampled after the network read.
type preparedChatStatesKey struct{}
type preparedChatStates struct {
	states      map[string]*pb.State
	unavailable map[string]bool
	epochs      map[string]uint64
}

type unchangedStreamStore interface {
	UnchangedConversationSyncV2Streams(context.Context, map[string]uint64, []*pb.State, bool) (map[string]bool, error)
}

// Pending and unavailable streams remain separate from successfully scanned
// streams. Neither outcome authorizes deleting their local history.
type StreamResults struct {
	Completed      int
	PendingIDs     []string
	UnavailableIDs []string
}

// SyncStreams batches state reads and bounds chat pulls to four workers.
// Notification consumption and commit callbacks remain sequential. Epochs
// are captured before each network batch, not after it or just before its pull.
func (c *Client) SyncStreams(ctx context.Context, op string, req *pb.StateRequest, caps *pb.Capabilities, store BatchStreamStore, commit CommitStream) (*StreamResults, error) {
	if req == nil || caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 || store == nil || commit == nil || req.IncludeDetails || req.IncludeSelf && !req.Notifications {
		return nil, fmt.Errorf("sync v2: invalid stream batch")
	}
	var prefetched *preparedChatStates
	if req.Notifications && len(req.ConversationIDs) > int(caps.MaxBatchSize) {
		var err error
		prefetched, err = c.prefetchNotificationStates(ctx, op, req, caps, store)
		if err != nil {
			return nil, err
		}
	}
	result := new(StreamResults)
	processed, total := 0, len(req.ConversationIDs)
	if req.IncludeSelf {
		total++
	}
	progress, _ := ctx.Value(streamProgressKey{}).(func(int, int))
	stage, unit := "messages", "conversations"
	if req.Notifications {
		stage, unit = "notifications", "streams"
	}
	reportCount(ctx, stage, unit, 0, total)
	advance := func() {
		processed++
		reportCount(ctx, stage, unit, processed, total)
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
		var epochs map[string]uint64
		var response *pb.StateResponse
		var err error
		prepared, _ := ctx.Value(preparedChatStatesKey{}).(*preparedChatStates)
		if prepared != nil && !req.Notifications {
			epochs = prepared.epochs
			response = &pb.StateResponse{}
			for _, id := range parents {
				if state := prepared.states[id]; state != nil {
					response.States = append(response.States, state)
				}
				if prepared.unavailable[id] {
					response.UnavailableIDs = append(response.UnavailableIDs, id)
				}
			}
		} else {
			epochs, err = store.GetConversationSyncV2Epochs(ctx, ids)
			if err != nil {
				return err
			}
			reuse := prefetched != nil && !self
			if reuse {
				for _, id := range ids {
					if captured, ok := prefetched.epochs[id]; !ok || captured != epochs[id] {
						reuse = false
						break
					}
				}
			}
			if reuse {
				response = &pb.StateResponse{}
				for _, id := range ids {
					if state := prefetched.states[id]; state != nil {
						response.States = append(response.States, state)
					}
				}
				for _, parent := range parents {
					if prefetched.unavailable[parent] {
						response.UnavailableIDs = append(response.UnavailableIDs, parent)
					}
				}
			} else {
				response, err = c.StatesBatched(ctx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: parents, Notifications: req.Notifications, IncludeSelf: self}, caps)
				if err != nil {
					return err
				}
			}
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

		var unchanged map[string]bool
		if idleStore, ok := store.(unchangedStreamStore); ok {
			unchanged, err = idleStore.UnchangedConversationSyncV2Streams(ctx, epochs, response.States, !req.Notifications && ctx.Value(deferHistoricalChatHolesKey{}) == true)
			if err != nil {
				return err
			}
		}

		const (
			completed int8 = iota + 1
			pending
			denied
		)
		outcomes := make([]int8, len(ids))
		accepted := map[string]*model_struct.LocalConversationSyncV2Stream{}
		if req.Notifications {
			if batchStore, ok := store.(BatchStateStore); ok {
				statesToAccept := make([]*model_struct.LocalConversationSyncV2Stream, 0, len(ids))
				for _, id := range ids {
					if unavailable[id] || states[id] == nil || !states[id].Known {
						continue
					}
					query := queries[id]
					statesToAccept = append(statesToAccept, &model_struct.LocalConversationSyncV2Stream{ConversationID: id, ParentID: query.ConversationID, Notifications: true, Self: query.Self, StateToken: states[id].StateToken, MinSeq: states[id].MinSeq, PermissionMaxSeq: states[id].PermissionMaxSeq, Epoch: epochs[id]})
				}
				var acceptErr error
				accepted, acceptErr = batchStore.AcceptConversationSyncV2States(ctx, statesToAccept)
				if acceptErr != nil {
					return acceptErr
				}
			}
		}
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
			if f, ok := ctx.Value(chatPullKey{}).(*chatPull); ok && f.err == ErrStatePending {
				outcomes[index] = pending
				return nil
			}
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
			if complete, idle := unchanged[id]; idle {
				outcomes[index] = pending
				if complete {
					outcomes[index] = completed
				}
				return nil
			}
			var err error
			var stream *model_struct.LocalConversationSyncV2Stream
			if preaccepted := accepted[id]; preaccepted != nil {
				stream, err = c.syncAcceptedStreamState(ctx, op, queries[id], caps, store, serialCommit, preaccepted, states[id])
			} else {
				stream, err = c.syncStreamState(ctx, op, queries[id], caps, store, serialCommit, epoch, states[id])
			}
			if RecoveryFor(err) == Unavailable {
				outcomes[index] = denied
				return nil
			}
			if errors.Is(err, ErrStatePending) || (ctx.Value(chatPullKey{}) != nil && RecoveryFor(err) == RefreshState) {
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
		var publishMu sync.Mutex
		publish := func(i int) {
			publishMu.Lock()
			defer publishMu.Unlock()
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
		if !req.Notifications && chatBatchEnabled(caps) {
			if err := c.consumeChatsBatch(ctx, op, ids, queries, states, epochs, unchanged, caps, store, scan, publish); err != nil {
				return err
			}
		} else if req.Notifications && len(accepted) > 0 {
			if err := c.consumeNotificationsPrefetched(ctx, op, ids, queries, states, accepted, caps, store, scan, publish); err != nil {
				return err
			}
		} else if req.Notifications || len(ids) < 2 {
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
						publish(i)
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
