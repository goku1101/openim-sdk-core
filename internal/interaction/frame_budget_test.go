package interaction

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"testing"
	"time"
)

func TestLastMessageSplitPreservesRequestAndStopsAtOne(t *testing.T) {
	req := &msg.GetLastMessageReq{UserID: "u", ConversationIDs: []string{"a", "b", "c"}}
	calls := 0
	send := func(ctx context.Context, m proto.Message, id int, out proto.Message) error {
		calls++
		q := m.(*msg.GetLastMessageReq)
		if len(q.ConversationIDs) > 1 || q.ConversationIDs[0] == "b" {
			return errs.NewCodeError(413, "too large")
		}
		return nil
	}
	err := pullLastMessagesBounded(context.Background(), req, &msg.GetLastMessageResp{}, send)
	if err == nil || calls > 6 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if len(req.ConversationIDs) != 3 {
		t.Fatal("mutated request")
	}
}
func TestLastMessageDoesNotSplitTransportFailure(t *testing.T) {
	calls := 0
	sentinel := errors.New("network")
	err := pullLastMessagesBounded(context.Background(), &msg.GetLastMessageReq{ConversationIDs: []string{"a", "b"}}, &msg.GetLastMessageResp{}, func(context.Context, proto.Message, int, proto.Message) error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("%v %d", err, calls)
	}
}
func TestNegotiatedFrameBudget(t *testing.T) {
	if maxMessageSize != 64<<20 {
		t.Fatalf("read budget=%d", maxMessageSize)
	}
}

func TestRangeIsolationDoesNotPublishPartialSuccess(t *testing.T) {
	req := &sdkws.PullMessageBySeqsReq{UserID: "u", SeqRanges: []*sdkws.SeqRange{{ConversationID: "a", Begin: 1, End: 20, Num: 10}, {ConversationID: "b", Begin: 2, End: 30, Num: 10}}}
	result := new(sdkws.PullMessageBySeqsResp)
	calls := 0
	err := pullRangesIndividually(context.Background(), req, result, func(_ context.Context, m proto.Message, _ int, out proto.Message) error {
		calls++
		q := m.(*sdkws.PullMessageBySeqsReq)
		if len(q.SeqRanges) != 1 || q.SeqRanges[0].Num != 10 {
			t.Fatal("range changed")
		}
		if q.SeqRanges[0].ConversationID == "b" {
			return errs.NewCodeError(413, "too large")
		}
		out.(*sdkws.PullMessageBySeqsResp).Msgs = map[string]*sdkws.PullMsgs{"a": {Msgs: []*sdkws.MsgData{{Seq: 1}}}}
		return nil
	})
	if err == nil || calls != 2 || len(result.Msgs) != 0 {
		t.Fatalf("partial success err=%v calls=%d result=%v", err, calls, result)
	}
}

func TestLegacy413DoesNotAdvanceCheckpoint(t *testing.T) {
	for _, reinstall := range []bool{false, true} {
		t.Run(fmt.Sprint(reinstall), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"}), time.Second)
			defer cancel()
			conn := &LongConnMgr{send: make(chan Message, 1)}
			syncer := &MsgSyncer{loginUserID: "u", longConnMgr: conn, syncedMaxSeqs: map[string]int64{"n_u_v": 4}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				select {
				case request := <-conn.send:
					request.Resp <- &GeneralWsResp{ErrCode: 413, ErrMsg: "too large"}
				case <-ctx.Done():
				}
			}()
			var err error
			if reinstall {
				err = syncer.syncAndTriggerReinstallMsgs(ctx, map[string][2]int64{"n_u_v": {5, 20}}, 10)
			} else {
				err = syncer.syncAndTriggerMsgs(ctx, map[string][2]int64{"n_u_v": {5, 20}}, 10)
			}
			<-done
			var coded errs.CodeError
			if !errors.As(err, &coded) || coded.Code() != 413 || syncer.syncedMaxSeqs["n_u_v"] != 4 {
				t.Fatalf("err=%v checkpoint=%v", err, syncer.syncedMaxSeqs)
			}
			if len(conn.send) != 0 {
				t.Fatal("oversize request retried")
			}
		})
	}
}
