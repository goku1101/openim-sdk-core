package syncv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// StatesBatched keeps empty chat queries local and reads self notifications once,
// separately from parent IDs. Failed batches never become deletion evidence.
func (c *Client) StatesBatched(ctx context.Context, op string, req *pb.StateRequest, caps *pb.Capabilities) (*pb.StateResponse, error) {
	if caps == nil || caps.MaxBatchSize <= 0 || caps.MaxRequestBytes <= 0 {
		return nil, fmt.Errorf("sync v2: missing request limits")
	}
	if req.IncludeSelf && !req.Notifications || req.Notifications && req.IncludeDetails {
		return nil, fmt.Errorf("sync v2: invalid notification options")
	}
	out := new(pb.StateResponse)
	progress, _ := ctx.Value(stateProgressKey{}).(func(int, int))
	total := len(req.ConversationIDs)
	if req.IncludeSelf {
		total++
	}
	report := func(done int) {
		if progress != nil {
			progress(done, total)
		}
	}
	maxBatch := min(int(caps.MaxBatchSize), 500)
	maxBytes := min(int(caps.MaxRequestBytes), maxRequestBytes)
	for start := 0; start < len(req.ConversationIDs); start += maxBatch {
		batch := &pb.StateRequest{UserID: req.UserID, ConversationIDs: req.ConversationIDs[start:min(start+maxBatch, len(req.ConversationIDs))], Notifications: req.Notifications, IncludeDetails: req.IncludeDetails}
		result, err := c.statesSplit(ctx, op, batch, maxBytes)
		if err != nil {
			return nil, err
		}
		out.States = append(out.States, result.States...)
		out.UnavailableIDs = append(out.UnavailableIDs, result.UnavailableIDs...)
		report(min(start+maxBatch, len(req.ConversationIDs)))
	}
	if req.IncludeSelf {
		result, err := c.statesSplit(ctx, op, &pb.StateRequest{UserID: req.UserID, Notifications: true, IncludeSelf: true}, maxBytes)
		if err != nil {
			return nil, err
		}
		out.States = append(out.States, result.States...)
		out.UnavailableIDs = append(out.UnavailableIDs, result.UnavailableIDs...)
		report(total)
	}
	return out, nil
}

func (c *Client) statesSplit(ctx context.Context, op string, req *pb.StateRequest, maxBytes int) (*pb.StateResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var result *pb.StateResponse
	if len(body) > maxBytes {
		err = &APIError{ErrCode: 8, ErrMsg: "SYNC_V2_REQUEST_BYTES"}
	} else {
		result, err = c.States(ctx, op, req)
	}
	if err == nil {
		return result, nil
	}
	if RecoveryFor(err) != ReduceBatch || len(req.ConversationIDs) < 2 {
		return nil, err
	}
	mid := len(req.ConversationIDs) / 2
	left, err := c.statesSplit(ctx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: req.ConversationIDs[:mid], Notifications: req.Notifications, IncludeDetails: req.IncludeDetails}, maxBytes)
	if err != nil {
		return nil, err
	}
	right, err := c.statesSplit(ctx, op, &pb.StateRequest{UserID: req.UserID, ConversationIDs: req.ConversationIDs[mid:], Notifications: req.Notifications, IncludeDetails: req.IncludeDetails}, maxBytes)
	if err != nil {
		return nil, err
	}
	left.States = append(left.States, right.States...)
	left.UnavailableIDs = append(left.UnavailableIDs, right.UnavailableIDs...)
	return left, nil
}
