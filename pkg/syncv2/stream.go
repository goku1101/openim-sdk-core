package syncv2

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

var ErrStatePending = errors.New("sync v2: stream state remains unconfirmed")

type StreamStore interface {
	GetConversationSyncV2Stream(context.Context, string) (*model_struct.LocalConversationSyncV2Stream, error)
	AcceptConversationSyncV2State(context.Context, uint64, *model_struct.LocalConversationSyncV2Stream) (*model_struct.LocalConversationSyncV2Stream, error)
}

// CommitStream must persist progress only after consuming the messages. Chat
// consumers commit rows and progress in one transaction; notifications acknowledge
// progress after their handlers succeed. A failed commit stops this scan.
type CommitStream func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error

func pullStreamID(req *pb.PullRequest) (string, error) {
	if req == nil || req.Self && (!req.Notifications || req.ConversationID != "") || !req.Self && req.ConversationID == "" {
		return "", fmt.Errorf("sync v2: invalid pull stream")
	}
	if req.Self {
		return "n_" + req.UserID + "_" + req.UserID, nil
	}
	if req.Notifications {
		separator := strings.IndexByte(req.ConversationID, '_')
		if separator < 0 {
			return "", fmt.Errorf("sync v2: invalid notification parent")
		}
		return "n" + req.ConversationID[separator:], nil
	}
	return req.ConversationID, nil
}

// SyncStream reads current permissions before pulling a bounded page at a time.
// Each pre-existing hole is retried once per call. Newly discovered holes remain
// durable for a later round instead of spinning or becoming deletion placeholders.
func (c *Client) SyncStream(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities, store StreamStore, commit CommitStream) (*model_struct.LocalConversationSyncV2Stream, error) {
	id, err := pullStreamID(req)
	if err != nil {
		return nil, err
	}
	if caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 || store == nil || commit == nil || req.Notifications && len(req.Seqs) != 0 {
		return nil, fmt.Errorf("sync v2: invalid stream scan")
	}
	for _, seq := range req.Seqs {
		if seq <= 0 {
			return nil, fmt.Errorf("sync v2: invalid pushed sequence")
		}
	}
	before, err := store.GetConversationSyncV2Stream(ctx, id)
	if err != nil {
		return nil, err
	}
	query := &pb.StateRequest{UserID: req.UserID, Notifications: req.Notifications, IncludeSelf: req.Self}
	if !req.Self {
		query.ConversationIDs = []string{req.ConversationID}
	}
	response, err := c.StatesBatched(ctx, op, query, caps)
	if err != nil {
		return nil, err
	}
	if len(response.UnavailableIDs) != 0 {
		return nil, &APIError{ErrCode: 7, ErrMsg: "SYNC_V2_CONVERSATION_UNAVAILABLE"}
	}
	if len(response.States) == 0 {
		return nil, ErrStatePending
	}
	if len(response.States) != 1 || response.States[0] == nil || response.States[0].ConversationID != id {
		return nil, fmt.Errorf("sync v2: inconsistent stream state")
	}
	return c.syncStreamState(ctx, op, req, caps, store, commit, before.Epoch, response.States[0])
}

// The caller must capture epoch before reading state from the network, including
// when fetching many stream states in one batch.
func (c *Client) syncStreamState(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities, store StreamStore, commit CommitStream, epoch uint64, state *pb.State) (*model_struct.LocalConversationSyncV2Stream, error) {
	id, err := pullStreamID(req)
	if err != nil {
		return nil, err
	}
	if state == nil || state.ConversationID != id {
		return nil, fmt.Errorf("sync v2: inconsistent stream state")
	}
	if !state.Known {
		return nil, ErrStatePending
	}
	if state.MinSeq < 0 || state.MaxSeq < 0 || state.PermissionMaxSeq < 0 || state.StateToken == "" {
		return nil, fmt.Errorf("sync v2: invalid stream bounds")
	}
	stream, err := store.AcceptConversationSyncV2State(ctx, epoch, &model_struct.LocalConversationSyncV2Stream{ConversationID: id, ParentID: req.ConversationID, Notifications: req.Notifications, Self: req.Self, StateToken: state.StateToken, MinSeq: state.MinSeq, PermissionMaxSeq: state.PermissionMaxSeq})
	if err != nil {
		return nil, err
	}
	upper := state.MaxSeq
	if state.PermissionMaxSeq > 0 {
		upper = min(upper, state.PermissionMaxSeq)
	}
	lower := max(int64(1), state.MinSeq)
	batchSize := min(int(caps.MaxBatchSize), 500)
	attempted := make(map[int64]bool)
	pull := func(seqs []int64, scanned int64) error {
		result := new(pb.PullResponse)
		if len(seqs) > 0 {
			result, err = c.PullBatched(ctx, op, &pb.PullRequest{UserID: req.UserID, ConversationID: req.ConversationID, Notifications: req.Notifications, Self: req.Self, StateToken: stream.StateToken, Seqs: seqs}, caps)
			if err != nil {
				return err
			}
			if err = validatePullOutcomes(seqs, result); err != nil {
				return err
			}
			for _, seq := range seqs {
				attempted[seq] = true
			}
		}
		progress := *stream
		progress.ScannedSeq = scanned
		progress.PendingSeqs = result.UnresolvedSeqs
		if err = commit(ctx, &progress, result.Messages); err != nil {
			return err
		}
		stream, err = store.GetConversationSyncV2Stream(ctx, id)
		if err != nil {
			return err
		}
		if stream.Epoch != progress.Epoch || stream.StateToken != progress.StateToken {
			return fmt.Errorf("sync v2: stream invalidated during consumption")
		}
		return nil
	}
	// A push may arrive before the state endpoint confirms its tail. Preserve
	// those allowed sequences for a later round instead of acknowledging loss.
	waiting := *stream
	waiting.PendingSeqs = append([]int64(nil), stream.PendingSeqs...)
	deferred := false
	for _, seq := range req.Seqs {
		if seq > upper && seq >= lower && (state.PermissionMaxSeq == 0 || seq <= state.PermissionMaxSeq) {
			waiting.PendingSeqs = append(waiting.PendingSeqs, seq)
			deferred = true
		}
	}
	if deferred {
		if err = commit(ctx, &waiting, nil); err != nil {
			return nil, err
		}
		stream, err = store.GetConversationSyncV2Stream(ctx, id)
		if err != nil {
			return nil, err
		}
		if stream.Epoch != waiting.Epoch || stream.StateToken != waiting.StateToken {
			return nil, ErrStatePending
		}
	}
	var pending []int64
	for _, seq := range stream.PendingSeqs {
		if seq >= lower && seq <= upper {
			pending = append(pending, seq)
		}
	}
	for start := 0; start < len(pending); start += batchSize {
		if err = pull(pending[start:min(start+batchSize, len(pending))], stream.ScannedSeq); err != nil {
			return nil, err
		}
	}
	// Explicit push sequences outside the recent window still require durable
	// delivery. Do not replay historical ranges merely to reach those sequences.
	hasNew := stream.ScannedSeq < upper
	nextStart := lower
	if hasNew {
		nextStart = chatWindowStart(ctx, req.Notifications, max(lower, stream.ScannedSeq+1), upper)
	}
	var required []int64
	seen := make(map[int64]bool, len(req.Seqs))
	for _, seq := range req.Seqs {
		if seq >= lower && seq <= upper && (!hasNew || seq < nextStart) && !attempted[seq] && !seen[seq] {
			required = append(required, seq)
			seen[seq] = true
		}
	}
	sort.Slice(required, func(i, j int) bool { return required[i] < required[j] })
	for start := 0; start < len(required); start += batchSize {
		if err = pull(required[start:min(start+batchSize, len(required))], stream.ScannedSeq); err != nil {
			return nil, err
		}
	}
	for stream.ScannedSeq < upper {
		start := max(lower, stream.ScannedSeq+1)
		start = chatWindowStart(ctx, req.Notifications, start, upper)
		if start > upper {
			break
		}
		end := start + min(int64(batchSize-1), upper-start)
		seqs := make([]int64, 0, int(end-start+1))
		for seq := start; ; seq++ {
			if !attempted[seq] {
				seqs = append(seqs, seq)
			}
			if seq == end {
				break
			}
		}
		if err = pull(seqs, end); err != nil {
			return nil, err
		}
	}
	return stream, nil
}
