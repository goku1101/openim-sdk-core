//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	sdk "github.com/openimsdk/openim-sdk-core/v3/pkg/sdk_params_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2HistoryEntryDoesNotInventDeletedMessagesOrFalseEnd(t *testing.T) {
	var allPending atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &pb.State{ConversationID: "c", Known: true, MinSeq: 2, MaxSeq: 3, StateToken: "t"}
		var data any
		switch r.URL.Path {
		case "/msg/sync_seqs_v2":
			data = &pb.StateResponse{States: []*pb.State{state}}
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.Seqs) == 0 || req.Seqs[0] < 2 || req.Seqs[len(req.Seqs)-1] > 3 {
				t.Errorf("out-of-bound history request: %v", req.Seqs)
			}
			result := &pb.PullResponse{State: state}
			for _, seq := range req.Seqs {
				if seq == 2 || allPending.Load() {
					result.UnresolvedSeqs = append(result.UnresolvedSeqs, seq)
				} else {
					result.Messages = append(result.Messages, &sdkws.MsgData{ClientMsgID: "m3", Seq: 3, SendTime: 3, ContentType: constant.Text, Content: []byte(`{"content":"three"}`)})
				}
			}
			data = result
		default:
			t.Errorf("unexpected legacy route: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), config))
	defer cancel()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", MinSeq: 1, MaxSeq: 3}); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	c.syncV2.mode.Store(syncV2Enabled)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	list := []*model_struct.LocalChatLog{{ClientMsgID: "m1", Seq: 1, SendTime: 1}}
	callback := &sdk.GetAdvancedHistoryMessageListCallback{}
	c.fetchAndMergeMissingMessages(ctx, "c", []int64{1, 2, 3}, false, 20, 0, &list, callback)
	if len(list) != 1 || list[0].Seq != 3 || callback.ErrCode == 0 {
		t.Fatalf("bad partial history: %+v %+v", list, callback)
	}
	c.checkEndBlock(ctx, "c", false, 0, 20, &list, callback)
	if callback.IsEnd {
		t.Fatal("unresolved sequence was reported as end of history")
	}
	if _, err = database.GetMessageBySeq(ctx, "c", 2); err == nil {
		t.Fatal("hole became a persisted deletion")
	}
	stream, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || stream.ScannedSeq != 0 || len(stream.PendingSeqs) != 1 {
		t.Fatalf("arbitrary history advanced sequential scan: %+v %v", stream, err)
	}
	empty := []*model_struct.LocalChatLog{}
	emptyCallback := &sdk.GetAdvancedHistoryMessageListCallback{}
	fetch, wanted := c.checkEndBlock(ctx, "c", false, 0, 20, &empty, emptyCallback)
	if !fetch || len(wanted) != 2 || wanted[0] != 2 || wanted[1] != 3 || emptyCallback.IsEnd {
		t.Fatalf("empty local window hid server history: fetch=%v seqs=%v callback=%+v", fetch, wanted, emptyCallback)
	}

	for _, tc := range []struct {
		name    string
		reverse bool
		cursor  int64
		count   int
		want    []int64
		end     bool
	}{
		{"newest", false, 0, 1, []int64{3}, false},
		{"older", false, 3, 1, []int64{2}, false},
		{"oldest reached", false, 2, 1, nil, true},
		{"oldest", true, 0, 1, []int64{2}, false},
		{"newer", true, 2, 1, []int64{3}, false},
		{"newest reached", true, 3, 1, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.messagePullForwardEndSeqMap.Delete("c", 0)
			c.messagePullReverseEndSeqMap.Delete("c", 0)
			if tc.reverse {
				c.messagePullReverseEndSeqMap.Store("c", 0, tc.cursor)
			} else {
				c.messagePullForwardEndSeqMap.Store("c", 0, tc.cursor)
			}
			cb := &sdk.GetAdvancedHistoryMessageListCallback{}
			fetch, seqs := c.checkEndBlock(ctx, "c", tc.reverse, 0, tc.count, &empty, cb)
			if fetch != (len(tc.want) > 0) || !reflect.DeepEqual(seqs, tc.want) || cb.IsEnd != tc.end {
				t.Fatalf("wrong page: fetch=%v seqs=%v callback=%+v", fetch, seqs, cb)
			}
		})
	}

	publicResult, err := c.getAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 20}, false)
	if err != nil || publicResult.ErrCode != 0 || publicResult.IsEnd || len(publicResult.MessageList) != 1 || publicResult.MessageList[0].Seq != 3 {
		t.Fatalf("public history discarded authorized partial messages: %+v %v", publicResult, err)
	}
	stream, err = database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || len(stream.PendingSeqs) != 1 || stream.PendingSeqs[0] != 2 {
		t.Fatal("partial delivery lost pending sequence")
	}

	if err = database.DeleteConversationAllMessages(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	allPending.Store(true)
	publicResult, err = c.getAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 20}, false)
	if err != nil || publicResult.ErrCode != 0 || publicResult.IsEnd || len(publicResult.MessageList) != 0 {
		t.Fatalf("authorized pending page should be empty without an error or false end: %+v %v", publicResult, err)
	}

	stream, err = database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || !reflect.DeepEqual(stream.PendingSeqs, []int64{2, 3}) {
		t.Fatalf("empty page lost durable holes: %+v %v", stream, err)
	}

	beforeClear, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.clearConversationAndDeleteAllMsg(ctx, "c", false, database.ClearConversation); err != nil {
		t.Fatal(err)
	}
	if err = database.CommitConversationSyncV2Messages(ctx, beforeClear.Epoch, beforeClear, []*model_struct.LocalChatLog{{ClientMsgID: "late-clear", Seq: 3}}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("clear allowed stale response to restore history: %v", err)
	}

}
