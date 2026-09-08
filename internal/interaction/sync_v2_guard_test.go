package interaction

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2GuardRejectsLegacyRequestsBeforeEnqueue(t *testing.T) {
	failure := errors.New("V2 owns this request")
	connection := &LongConnMgr{send: make(chan Message, 1)}
	connection.SetSyncRequestGuard(func(_ context.Context, identifier int) error {
		switch identifier {
		case constant.GetNewestSeq, constant.GetConvMaxReadSeq, constant.PullMsgByRange, constant.PullMsgBySeqList, constant.PullConvLastMessage:
			return failure
		}
		return nil
	})
	for _, identifier := range []int{constant.GetNewestSeq, constant.GetConvMaxReadSeq, constant.PullMsgByRange, constant.PullMsgBySeqList, constant.PullConvLastMessage} {
		err := connection.SendReqWaitResp(context.Background(), &sdkws.GetMaxSeqReq{}, identifier, &sdkws.GetMaxSeqResp{})
		if !errors.Is(err, failure) {
			t.Fatalf("route %d bypassed V2 guard: %v", identifier, err)
		}
	}
	if len(connection.send) != 0 {
		t.Fatal("legacy request reached outbound channel")
	}
}
