package syncv2

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
)

type chatPullKey struct{}
type chatPull struct {
	request  *pb.PullRequest
	response *pb.PullResponse
	err      error
}
type responseBudgetKey struct{}

const (
	// These are local safety ceilings. The effective values remain the
	// negotiated server capabilities when they are lower.
	maxChatBatchStreams = 50
	maxChatBatchSeqs    = 200
)

func chatBatchEnabled(caps *pb.Capabilities) bool {
	return caps.MultiChatMessagePull && caps.ChatMessagePullVersion == 1
}
func validateChatLimits(caps *pb.Capabilities) error {
	if caps.MaxBatchSize <= 0 || caps.MaxChatMessagePullStreams <= 0 || caps.MaxChatMessagePullSeqsPerStream <= 0 || caps.MaxChatMessagePullSeqs <= 0 || caps.MaxChatMessageRequestBytes <= 0 || caps.MaxChatMessageResponseBytes <= 0 {
		return fmt.Errorf("sync v2: invalid chat batch limits")
	}
	return nil
}
func chatPageSize(caps *pb.Capabilities) int {
	return min(int(caps.MaxBatchSize), int(caps.MaxChatMessagePullSeqsPerStream), int(caps.MaxChatMessagePullSeqs), 100)
}
func chatItem(q *pb.PullRequest) *pb.ChatBatchPullItem {
	identity, _ := json.Marshal(q)
	return &pb.ChatBatchPullItem{RequestID: fmt.Sprintf("%x", sha256.Sum256(identity)), ConversationID: q.ConversationID, Seqs: q.Seqs, StateToken: q.StateToken}
}

// Estimate the JSON envelope by serializing only bounded individual items.
func chatRequestSize(user string, fs []*chatPull) int {
	u, _ := json.Marshal(user)
	n := len(`{"userID":,"items":[]}`) + len(u)
	for i, f := range fs {
		b, _ := json.Marshal(chatItem(f.request))
		n += len(b)
		if i > 0 {
			n++
		}
	}
	return n
}

// Capture at most one page per stream. Existing scan/commit logic retains its
// permission checks, durable cursors, holes, and recent-message window.
func (c *Client) consumeChatsBatch(ctx context.Context, op string, ids []string, queries map[string]*pb.PullRequest, states map[string]*pb.State, epochs map[string]uint64, unchanged map[string]bool, caps *pb.Capabilities, store BatchStreamStore, scan func(context.Context, int) error, publish func(int)) error {
	if err := validateChatLimits(caps); err != nil {
		return err
	}
	for start := 0; start < len(ids); {
		futures := map[int]*chatPull{}
		var batch []*chatPull
		total, end := 0, start
		for end < len(ids) {
			id := ids[end]
			state := states[id]
			_, idle := unchanged[id]
			if state == nil || !state.Known || idle {
				end++
				continue
			}
			stream, err := store.GetConversationSyncV2Stream(ctx, id)
			if err != nil {
				return err
			}
			if stream.Epoch != epochs[id] {
				futures[end] = &chatPull{err: ErrStatePending}
				end++
				continue
			}
			if len(stream.PendingSeqs) > 0 {
				end++
				continue
			}
			upper := state.MaxSeq
			if state.PermissionMaxSeq > 0 {
				upper = min(upper, state.PermissionMaxSeq)
			}
			lower := chatWindowStart(ctx, false, max(int64(1), state.MinSeq, stream.ScannedSeq+1), upper)
			if lower > upper {
				end++
				continue
			}
			q := proto.Clone(queries[id]).(*pb.PullRequest)
			q.StateToken = state.StateToken
			last := lower + min(int64(chatPageSize(caps)-1), upper-lower)
			for n := lower; ; n++ {
				q.Seqs = append(q.Seqs, n)
				if n == last {
					break
				}
			}
			f := &chatPull{request: q}
			candidate := append(append([]*chatPull(nil), batch...), f)
			if len(candidate) > min(int(caps.MaxChatMessagePullStreams), maxChatBatchStreams) || total+len(q.Seqs) > min(int(caps.MaxChatMessagePullSeqs), maxChatBatchSeqs) || chatRequestSize(q.UserID, candidate) > min(int(caps.MaxChatMessageRequestBytes), maxChatRequestBytes) {
				if len(batch) != 0 {
					break
				}
				// A single item may still be split by sequence count below.
			}
			batch = candidate
			futures[end] = f
			total += len(q.Seqs)
			end++
		}
		if len(batch) > 0 {
			if err := c.fetchChatBatch(ctx, op, batch, caps); err != nil {
				return err
			}
		}
		var deferred error
		for i := start; i < end; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			runCtx := ctx
			if f := futures[i]; f != nil {
				if f.err != nil && RecoveryFor(f.err) != Unavailable {
					if !batchItemRetry(f.err) && deferred == nil {
						deferred = f.err
					}
					f.err = ErrStatePending
				}
				runCtx = context.WithValue(ctx, chatPullKey{}, f)
			}
			if err := scan(runCtx, i); err != nil {
				return err
			}
			publish(i)
		}
		if deferred != nil {
			return deferred
		}
		start = end
	}
	return nil
}

func (c *Client) fetchChatBatch(ctx context.Context, op string, fs []*chatPull, caps *pb.Capabilities) error {
	req := &pb.ChatBatchPullRequest{UserID: fs[0].request.UserID}
	byID := map[string]*chatPull{}
	conversations := map[string]bool{}
	total := 0
	for _, f := range fs {
		q := f.request
		if q.Notifications || q.Self || strings.HasPrefix(q.ConversationID, "n_") || (!strings.HasPrefix(q.ConversationID, "si_") && !strings.HasPrefix(q.ConversationID, "sg_")) || len(q.ConversationID) > 512 || len(q.StateToken) != 64 || q.UserID != req.UserID || conversations[q.ConversationID] || len(q.Seqs) == 0 || len(q.Seqs) > chatPageSize(caps) {
			return fmt.Errorf("sync v2: invalid chat batch item")
		}
		seen := map[int64]bool{}
		for _, s := range q.Seqs {
			if s <= 0 || seen[s] {
				return fmt.Errorf("sync v2: invalid chat sequence")
			}
			seen[s] = true
		}
		conversations[q.ConversationID] = true
		it := chatItem(q)
		req.Items = append(req.Items, it)
		byID[it.RequestID] = f
		total += len(q.Seqs)
	}
	if len(fs) == 1 && len(fs[0].request.Seqs) > 1 && chatRequestSize(req.UserID, fs) > min(int(caps.MaxChatMessageRequestBytes), maxChatRequestBytes) {
		fs[0].err = c.splitChatPull(ctx, op, fs[0], caps)
		return nil
	}
	if len(fs) > min(int(caps.MaxChatMessagePullStreams), maxChatBatchStreams) || total > min(int(caps.MaxChatMessagePullSeqs), maxChatBatchSeqs) || chatRequestSize(req.UserID, fs) > min(int(caps.MaxChatMessageRequestBytes), maxChatRequestBytes) {
		return fmt.Errorf("sync v2: chat batch exceeds negotiated limits")
	}
	resp := new(pb.ChatBatchPullResponse)
	// The backend limits protobuf bytes. Bound JSON expansion and its envelope too.
	budget := min(int64(caps.MaxChatMessageResponseBytes)*6+4096, int64(maxResponseBytes))
	requestBudget := int64(min(int(caps.MaxChatMessageRequestBytes), maxChatRequestBytes))
	invokeCtx := context.WithValue(ctx, responseBudgetKey{}, budget)
	invokeCtx = context.WithValue(invokeCtx, requestBudgetKey{}, requestBudget)
	_, err := c.invoke(invokeCtx, op, "/msg/sync_chat_messages_batch_v2", req, resp)
	if err != nil {
		if chatBatchRetryable(err) {
			for _, f := range fs {
				f.err = err
			}
			return nil
		}
		return err
	}
	if proto.Size(resp) > int(caps.MaxChatMessageResponseBytes) {
		return fmt.Errorf("sync v2: chat response exceeds negotiated budget")
	}
	seen := map[string]bool{}
	for _, r := range resp.Results {
		if r == nil || byID[r.RequestID] == nil || seen[r.RequestID] || byID[r.RequestID].request.ConversationID != r.ConversationID {
			return fmt.Errorf("sync v2: invalid chat batch correlation")
		}
		seen[r.RequestID] = true
	}
	if len(seen) != len(fs) {
		return fmt.Errorf("sync v2: incomplete chat batch results")
	}
	RecordDiagnostic(ctx, "chat_batch_streams", len(fs), 0, false)
	for _, r := range resp.Results {
		f := byID[r.RequestID]
		if r.ErrorCode != 0 {
			f.err = &APIError{ErrCode: int(r.ErrorCode), ErrMsg: r.ErrorReason}
			if r.ErrorReason == "SYNC_V2_ITEM_RESPONSE_LIMIT" && len(f.request.Seqs) > 1 {
				f.err = c.splitChatPull(ctx, op, f, caps)
			}
			continue
		}
		result := &pb.PullResponse{State: r.State, Messages: r.Messages, UnresolvedSeqs: r.UnresolvedSeqs}
		if r.State == nil || r.State.ConversationID != f.request.ConversationID || r.State.StateToken != f.request.StateToken {
			f.err = fmt.Errorf("sync v2: inconsistent chat batch state")
			continue
		}
		if e := validatePullOutcomes(f.request.Seqs, result); e != nil {
			f.err = e
			continue
		}
		f.response = result
		RecordDiagnostic(ctx, "chat_batch_messages", len(r.Messages), 0, false)
		RecordDiagnostic(ctx, "fetched_chat", len(r.Messages), 0, false)
	}
	return nil
}

// A batch transport/HTTP failure must leave only this batch pending. It must
// not renegotiate capability or fan out into one request per conversation.
func chatBatchRetryable(err error) bool {
	if RecoveryFor(err) == Retry {
		return true
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode >= 500 && httpErr.StatusCode <= 599
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrCode >= 500 && apiErr.ErrCode <= 599
	}
	return false
}

// Keep a split inside the same item. No partial cursor is committed until all
// its outcomes have been validated; successful sibling items remain independent.
func (c *Client) splitChatPull(ctx context.Context, op string, f *chatPull, caps *pb.Capabilities) error {
	left, right := proto.Clone(f.request).(*pb.PullRequest), proto.Clone(f.request).(*pb.PullRequest)
	mid := len(left.Seqs) / 2
	left.Seqs = left.Seqs[:mid]
	right.Seqs = right.Seqs[mid:]
	a, b := &chatPull{request: left}, &chatPull{request: right}
	if err := c.fetchChatBatch(ctx, op, []*chatPull{a}, caps); err != nil {
		a.err = err
	}
	if err := c.fetchChatBatch(ctx, op, []*chatPull{b}, caps); err != nil {
		b.err = err
	}
	if err := errors.Join(a.err, b.err); err != nil {
		return err
	}
	f.response = &pb.PullResponse{State: b.response.State, Messages: append(a.response.Messages, b.response.Messages...), UnresolvedSeqs: append(a.response.UnresolvedSeqs, b.response.UnresolvedSeqs...)}
	return nil
}

func (c *Client) pullChatBatched(ctx context.Context, op string, q *pb.PullRequest, caps *pb.Capabilities) (*pb.PullResponse, error) {
	if err := validateChatLimits(caps); err != nil {
		return nil, err
	}
	q = proto.Clone(q).(*pb.PullRequest)
	unique := make([]int64, 0, len(q.Seqs))
	seen := make(map[int64]bool)
	for _, seq := range q.Seqs {
		if seq <= 0 {
			return nil, fmt.Errorf("sync v2: invalid chat sequence")
		}
		if !seen[seq] {
			unique = append(unique, seq)
			seen[seq] = true
		}
	}
	q.Seqs = unique
	out := new(pb.PullResponse)
	for start := 0; start < len(q.Seqs); start += chatPageSize(caps) {
		part := proto.Clone(q).(*pb.PullRequest)
		part.Seqs = q.Seqs[start:min(start+chatPageSize(caps), len(q.Seqs))]
		f := &chatPull{request: part}
		if err := c.fetchChatBatch(ctx, op, []*chatPull{f}, caps); err != nil {
			return nil, err
		}
		if f.err != nil {
			return nil, f.err
		}
		out.State = f.response.State
		out.Messages = append(out.Messages, f.response.Messages...)
		out.UnresolvedSeqs = append(out.UnresolvedSeqs, f.response.UnresolvedSeqs...)
	}
	return out, nil
}
