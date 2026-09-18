package syncv2

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
)

func notificationPageSize(q *pb.PullRequest, caps *pb.Capabilities) int {
	n := min(int(caps.MaxBatchSize), 500)
	if !q.Notifications && chatBatchEnabled(caps) {
		return chatPageSize(caps)
	}
	if q.Notifications && !q.Self && caps.MultiStreamMessagePull {
		n = min(n, int(caps.MaxMessagePullSeqsPerStream), int(caps.MaxMessagePullSeqs), 100)
	}
	return n
}

// Batch only the next bounded set of first pages. Notification handlers still
// run in directory order: an earlier event may revoke a later stream's epoch.
func (c *Client) consumeNotificationsBatch(ctx context.Context, op string, ids []string, queries map[string]*pb.PullRequest, states map[string]*pb.State, accepted map[string]*model_struct.LocalConversationSyncV2Stream, caps *pb.Capabilities, store BatchStreamStore, scan func(context.Context, int) error, publish func(int)) error {
	if caps.MaxMessagePullStreams <= 0 || caps.MaxMessagePullSeqsPerStream <= 0 || caps.MaxMessagePullSeqs <= 0 {
		return fmt.Errorf("sync v2: invalid multi-stream limits")
	}
	for start := 0; start < len(ids); {
		futures := map[int]*notificationPull{}
		var batch []*notificationPull
		total, end := 0, start
		for end < len(ids) {
			id := ids[end]
			stream, state, query := accepted[id], states[id], queries[id]
			if stream == nil || state == nil || query.Self || len(stream.PendingSeqs) > 0 {
				end++
				continue
			}
			upper := state.MaxSeq
			if state.PermissionMaxSeq > 0 {
				upper = min(upper, state.PermissionMaxSeq)
			}
			lower := max(int64(1), state.MinSeq, stream.ScannedSeq+1)
			if lower > upper {
				end++
				continue
			}
			q := proto.Clone(query).(*pb.PullRequest)
			q.StateToken = stream.StateToken
			last := lower + min(int64(notificationPageSize(q, caps)-1), upper-lower)
			count := int(last - lower + 1)
			if len(batch) >= min(int(caps.MaxMessagePullStreams), 25) || total+count > min(int(caps.MaxMessagePullSeqs), 500) {
				break
			}
			for n := lower; ; n++ {
				q.Seqs = append(q.Seqs, n)
				if n == last {
					break
				}
			}
			future := &notificationPull{request: q}
			futures[end] = future
			batch = append(batch, future)
			total += count
			end++
		}
		if len(batch) > 0 {
			c.fetchNotificationBatch(ctx, op, batch, caps)
		}
		var deferredError error
		for i := start; i < end; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			runCtx := ctx
			if future := futures[i]; future != nil {
				epochs, err := store.GetConversationSyncV2Epochs(ctx, []string{ids[i]})
				if err != nil {
					return err
				}
				if epochs[ids[i]] != accepted[ids[i]].Epoch {
					future.response = nil
					future.err = ErrStatePending
				}
				// Let successful siblings commit before reporting a permanent item error.
				// Pending IDs are persisted by the existing recovery scheduler and reread
				// authoritative state on their next attempt.
				if future.err != nil && RecoveryFor(future.err) != Unavailable {
					if !batchItemRetry(future.err) && deferredError == nil {
						deferredError = future.err
					}
					future.err = ErrStatePending
				}
				runCtx = context.WithValue(ctx, notificationPullKey{}, future)
			}
			if err := scan(runCtx, i); err != nil {
				return err
			}
			publish(i)
		}
		if deferredError != nil {
			return deferredError
		}
		start = end
	}
	return nil
}

func batchItemRetry(err error) bool {
	if errors.Is(err, ErrStatePending) {
		return true
	}
	if action := RecoveryFor(err); action == Retry || action == RefreshState {
		return true
	}
	var api *APIError
	return errors.As(err, &api) && ((api.ErrCode == 4 && api.ErrMsg == "SYNC_V2_ITEM_TIMEOUT") || (api.ErrCode == 13 && api.ErrMsg == "SYNC_V2_ITEM_INTERNAL"))
}

func batchRouteUnsupported(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == 404 {
		return true
	}
	var api *APIError
	return errors.As(err, &api) && api.ErrCode == 12
}

func (c *Client) fetchNotificationBatch(ctx context.Context, op string, futures []*notificationPull, caps *pb.Capabilities) {
	if c.batchPullUnsupported.Load() {
		for _, f := range futures {
			f.response, f.err = c.PullBatched(ctx, op, f.request, caps)
		}
		return
	}
	req := &pb.BatchPullRequest{UserID: futures[0].request.UserID}
	byID := map[string]*notificationPull{}
	for _, f := range futures {
		q := f.request
		identity, _ := json.Marshal(q)
		id := fmt.Sprintf("%x", sha256.Sum256(identity))
		req.Items = append(req.Items, &pb.BatchPullItem{RequestID: id, ConversationID: q.ConversationID, Seqs: q.Seqs, StateToken: q.StateToken, Notifications: true, Self: q.Self})
		byID[id] = f
	}
	resp := new(pb.BatchPullResponse)
	body, _ := json.Marshal(req)
	var err error
	if len(body) > min(int(caps.MaxRequestBytes), maxRequestBytes) {
		err = &APIError{ErrCode: 8, ErrMsg: "SYNC_V2_REQUEST_BYTES"}
	} else {
		_, err = c.invoke(ctx, op, "/msg/sync_messages_batch_v2", req, resp)
	}
	if batchRouteUnsupported(err) {
		c.batchPullUnsupported.Store(true)
		c.fetchNotificationBatch(ctx, op, futures, caps)
		return
	}
	if err != nil {
		if RecoveryFor(err) == ReduceBatch && c.splitNotificationBatch(ctx, op, futures, caps) {
			return
		}
		for _, f := range futures {
			f.err = err
		}
		return
	}
	// Validate the complete correlation envelope before accepting any payload.
	seen := map[string]bool{}
	for _, r := range resp.Results {
		if r == nil || byID[r.RequestID] == nil || seen[r.RequestID] {
			err = fmt.Errorf("sync v2: invalid batch correlation")
			break
		}
		f := byID[r.RequestID]
		if r.ConversationID != f.request.ConversationID || r.Self != f.request.Self {
			err = fmt.Errorf("sync v2: inconsistent batch identity")
			break
		}
		seen[r.RequestID] = true
	}
	if err == nil && len(seen) != len(futures) {
		err = fmt.Errorf("sync v2: incomplete batch results")
	}
	if err != nil {
		for _, f := range futures {
			f.err = err
		}
		return
	}
	RecordDiagnostic(ctx, "notification_batch_streams", len(futures), 0, false)
	for _, r := range resp.Results {
		f := byID[r.RequestID]
		if r.ErrorCode != 0 {
			f.err = &APIError{ErrCode: int(r.ErrorCode), ErrMsg: r.ErrorReason}
			if RecoveryFor(f.err) == ReduceBatch {
				c.splitNotificationBatch(ctx, op, []*notificationPull{f}, caps)
			}
			continue
		}
		result := &pb.PullResponse{State: r.State, Messages: r.Messages, UnresolvedSeqs: r.UnresolvedSeqs}
		id, idErr := pullStreamID(f.request)
		if idErr != nil || r.State == nil || r.State.ConversationID != id || r.State.StateToken != f.request.StateToken {
			f.err = fmt.Errorf("sync v2: inconsistent batch state")
			continue
		}
		if err := validatePullOutcomes(f.request.Seqs, result); err != nil {
			f.err = err
			continue
		}
		f.response = result
		RecordDiagnostic(ctx, "notification_batch_messages", len(r.Messages), 0, false)
	}
}

// Split only the failing request/item, never successful siblings. Request IDs
// are deterministic for each exact subrequest and remain stable on retries.
func (c *Client) splitNotificationBatch(ctx context.Context, op string, fs []*notificationPull, caps *pb.Capabilities) bool {
	if len(fs) > 1 {
		mid := len(fs) / 2
		c.fetchNotificationBatch(ctx, op, fs[:mid], caps)
		c.fetchNotificationBatch(ctx, op, fs[mid:], caps)
		return true
	}
	f := fs[0]
	if len(f.request.Seqs) < 2 {
		return false
	}
	left, right := proto.Clone(f.request).(*pb.PullRequest), proto.Clone(f.request).(*pb.PullRequest)
	mid := len(left.Seqs) / 2
	left.Seqs = left.Seqs[:mid]
	right.Seqs = right.Seqs[mid:]
	a, b := &notificationPull{request: left}, &notificationPull{request: right}
	c.fetchNotificationBatch(ctx, op, []*notificationPull{a}, caps)
	c.fetchNotificationBatch(ctx, op, []*notificationPull{b}, caps)
	f.err = a.err
	if f.err == nil {
		f.err = b.err
	}
	if f.err == nil {
		f.response = &pb.PullResponse{State: b.response.State, Messages: append(a.response.Messages, b.response.Messages...), UnresolvedSeqs: append(a.response.UnresolvedSeqs, b.response.UnresolvedSeqs...)}
	}
	return true
}
