package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// SequencePage awaits the consumer's atomic message/bounds/progress commit.
// Reading arbitrary history must not advance the sequential scan cursor.
type SequencePage struct {
	Response *pb.PullResponse
	Progress *model_struct.LocalConversationSyncV2Stream
}

func (c *Client) ReadSequencePage(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities, store StreamStore) (*SequencePage, error) {
	return c.ReadSequencePageAndCommit(ctx, op, req, caps, store, nil)
}

// ReadSequencePageAndCommit retries the full read and atomic consumer commit.
// An invalidated page is discarded; every retry re-reads authoritative bounds.
func (c *Client) ReadSequencePageAndCommit(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities, store StreamStore, commit func(*SequencePage) error) (*SequencePage, error) {
	if req == nil || req.ConversationID == "" || req.Notifications || req.Self || store == nil {
		return nil, fmt.Errorf("sync v2: invalid history query")
	}
	for _, seq := range req.Seqs {
		if seq <= 0 {
			return nil, fmt.Errorf("sync v2: invalid history sequence")
		}
	}
	for attempt := 0; attempt < 3; attempt++ {
		page, err := c.readSequencePageAttempt(ctx, op, req, caps, store)
		if err == nil && commit != nil {
			err = commit(page)
		}
		if err == nil {
			return page, nil
		}
		if !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || attempt == 2 {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("sync v2: history retry exhausted")
}

func (c *Client) readSequencePageAttempt(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities, store StreamStore) (*SequencePage, error) {
	before, err := store.GetConversationSyncV2Stream(ctx, req.ConversationID)
	if err != nil {
		return nil, err
	}
	response, err := c.StatesBatched(ctx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: []string{req.ConversationID}}, caps)
	if err != nil {
		return nil, err
	}
	if len(response.UnavailableIDs) > 0 {
		return nil, &APIError{ErrCode: 7, ErrMsg: "SYNC_V2_CONVERSATION_UNAVAILABLE"}
	}
	if len(response.States) == 0 {
		return nil, ErrStatePending
	}
	if len(response.States) != 1 || response.States[0] == nil || response.States[0].ConversationID != req.ConversationID {
		return nil, fmt.Errorf("sync v2: inconsistent history state")
	}
	state := response.States[0]
	if !state.Known {
		return nil, ErrStatePending
	}
	if state.MinSeq < 0 || state.MaxSeq < 0 || state.PermissionMaxSeq < 0 || state.StateToken == "" {
		return nil, fmt.Errorf("sync v2: invalid history bounds")
	}
	progress, err := store.AcceptConversationSyncV2State(ctx, before.Epoch, &model_struct.LocalConversationSyncV2Stream{ConversationID: req.ConversationID, ParentID: req.ConversationID, StateToken: state.StateToken, MinSeq: state.MinSeq, PermissionMaxSeq: state.PermissionMaxSeq})
	if err != nil {
		return nil, err
	}
	upper := state.MaxSeq
	if state.PermissionMaxSeq > 0 {
		upper = min(upper, state.PermissionMaxSeq)
	}
	seqs := make([]int64, 0, len(req.Seqs))
	seen := make(map[int64]bool, len(req.Seqs))
	for _, seq := range req.Seqs {
		if seq >= max(int64(1), state.MinSeq) && seq <= upper && !seen[seq] {
			seqs = append(seqs, seq)
			seen[seq] = true
		}
	}
	result := &pb.PullResponse{State: state}
	if len(seqs) > 0 {
		result, err = c.PullBatched(ctx, op, &pb.PullRequest{UserID: req.UserID, ConversationID: req.ConversationID, StateToken: progress.StateToken, Seqs: seqs}, caps)
		if err != nil {
			return nil, err
		}
		if err = validatePullOutcomes(seqs, result); err != nil {
			return nil, err
		}
		if !result.State.Known {
			return nil, ErrStatePending
		}
	}
	progress.PendingSeqs = result.UnresolvedSeqs
	return &SequencePage{Response: result, Progress: progress}, nil
}
