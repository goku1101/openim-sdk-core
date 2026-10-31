package interaction

import (
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
)

type syncRequestSender func(context.Context, proto.Message, int, proto.Message) error

// Build the response privately: a failed child must not expose a partial success.
func pullLastMessagesBounded(ctx context.Context, req *msg.GetLastMessageReq, resp *msg.GetLastMessageResp, send syncRequestSender) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result := new(msg.GetLastMessageResp)
	err := send(ctx, req, constant.PullConvLastMessage, result)
	if err == nil {
		proto.Merge(resp, result)
		return nil
	}
	var coded errs.CodeError
	if !errors.As(err, &coded) || coded.Code() != 413 || len(req.ConversationIDs) <= 1 {
		return err
	}
	combined := new(msg.GetLastMessageResp)
	middle := len(req.ConversationIDs) / 2
	for _, ids := range [][]string{req.ConversationIDs[:middle], req.ConversationIDs[middle:]} {
		child := proto.Clone(req).(*msg.GetLastMessageReq)
		child.ConversationIDs = append([]string(nil), ids...)
		if err := pullLastMessagesBounded(ctx, child, combined, send); err != nil {
			return err
		}
	}
	proto.Merge(resp, combined)
	return nil
}

// Legacy range replies cannot prove sequence holes. Keep each range unchanged
// while isolating conversations; never report a smaller Num as a full success.
func pullRangesIndividually(ctx context.Context, req *sdkws.PullMessageBySeqsReq, resp *sdkws.PullMessageBySeqsResp, send syncRequestSender) error {
	combined := new(sdkws.PullMessageBySeqsResp)
	for _, seqRange := range req.SeqRanges {
		if err := ctx.Err(); err != nil {
			return err
		}
		child := proto.Clone(req).(*sdkws.PullMessageBySeqsReq)
		child.SeqRanges = []*sdkws.SeqRange{proto.Clone(seqRange).(*sdkws.SeqRange)}
		result := new(sdkws.PullMessageBySeqsResp)
		if err := send(ctx, child, constant.PullMsgByRange, result); err != nil {
			return err
		}
		proto.Merge(combined, result)
	}
	proto.Merge(resp, combined)
	return nil
}
