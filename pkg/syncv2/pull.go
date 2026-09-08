package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
)

// PullBatched does not synthesize deletion placeholders for unresolved storage.
// Consumers must persist those pending sequences before advancing their cursor.
func (c *Client) PullBatched(ctx context.Context, op string, req *pb.PullRequest, caps *pb.Capabilities) (*pb.PullResponse, error) {
	if req == nil || caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 {
		return nil, fmt.Errorf("sync v2: invalid pull limits")
	}
	if req.Self && (!req.Notifications || req.ConversationID != "") || !req.Self && req.ConversationID == "" {
		return nil, fmt.Errorf("sync v2: invalid pull stream")
	}
	result := new(pb.PullResponse)
	streamID := req.ConversationID
	if req.Self {
		streamID = "n_" + req.UserID + "_" + req.UserID
	} else if req.Notifications {
		separator := strings.IndexByte(req.ConversationID, '_')
		if separator < 0 {
			return nil, fmt.Errorf("sync v2: invalid notification parent")
		}
		streamID = "n" + req.ConversationID[separator:]
	}
	batchSize := min(int(caps.MaxBatchSize), 500)
	for start := 0; start < len(req.Seqs); start += batchSize {
		batch := &pb.PullRequest{UserID: req.UserID, ConversationID: req.ConversationID, Notifications: req.Notifications, Self: req.Self, StateToken: req.StateToken, Seqs: req.Seqs[start:min(start+batchSize, len(req.Seqs))]}
		resp, err := c.pullSplit(ctx, op, batch, min(int(caps.MaxRequestBytes), maxRequestBytes))
		if err != nil {
			return nil, err
		}
		if resp.State == nil || resp.State.StateToken != req.StateToken || resp.State.ConversationID != streamID {
			return nil, fmt.Errorf("sync v2: inconsistent pull state")
		}
		result.State = resp.State
		result.Messages = append(result.Messages, resp.Messages...)
		result.UnresolvedSeqs = append(result.UnresolvedSeqs, resp.UnresolvedSeqs...)
	}
	return result, nil
}

func (c *Client) pullSplit(ctx context.Context, op string, req *pb.PullRequest, maxBytes int) (*pb.PullResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var resp *pb.PullResponse
	if len(body) > maxBytes {
		err = &APIError{ErrCode: 8, ErrMsg: "SYNC_V2_REQUEST_BYTES"}
	} else {
		resp, err = c.Pull(ctx, op, req)
	}
	if err == nil {
		return resp, nil
	}
	if RecoveryFor(err) != ReduceBatch || len(req.Seqs) < 2 {
		return nil, err
	}
	leftReq, rightReq := proto.Clone(req).(*pb.PullRequest), proto.Clone(req).(*pb.PullRequest)
	mid := len(req.Seqs) / 2
	leftReq.Seqs = req.Seqs[:mid]
	rightReq.Seqs = req.Seqs[mid:]
	left, err := c.pullSplit(ctx, op, leftReq, maxBytes)
	if err != nil {
		return nil, err
	}
	right, err := c.pullSplit(ctx, op, rightReq, maxBytes)
	if err != nil {
		return nil, err
	}
	if left.State == nil || right.State == nil || left.State.ConversationID != right.State.ConversationID || left.State.StateToken != req.StateToken || right.State.StateToken != req.StateToken {
		return nil, fmt.Errorf("sync v2: inconsistent split pull state")
	}
	left.State = right.State
	left.Messages = append(left.Messages, right.Messages...)
	left.UnresolvedSeqs = append(left.UnresolvedSeqs, right.UnresolvedSeqs...)
	return left, nil
}
