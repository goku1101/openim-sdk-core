//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSyncV2SelfReadDoesNotRequireDownloadedMessage(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	c := &Conversation{db: d, loginUserID: "u", syncV2: &conversationSyncV2Runtime{store: d}}
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "syncing"})
	tips := &sdkws.MarkAsReadTips{ConversationID: "si_u_v", MarkAsReadUserID: "u", HasReadSeq: 7, Seqs: []int64{7}}
	body, _ := json.Marshal(tips)
	content, _ := json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	msg := &sdkws.MsgData{Content: content}
	// A filtered-out conversation must not be recreated by a read notification.
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetConversation(ctx, tips.ConversationID); err == nil {
		t.Fatal("activated unloaded conversation")
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: tips.ConversationID, ConversationType: constant.SingleChatType, UnreadCount: 3}); err != nil {
		t.Fatal(err)
	}
	if err = d.BatchInsertMessageList(ctx, tips.ConversationID, []*model_struct.LocalChatLog{{ClientMsgID: "local", Seq: 6, SendID: "v"}}); err != nil {
		t.Fatal(err)
	}
	tips.Seqs = []int64{6, 7}
	body, _ = json.Marshal(tips)
	msg.Content, _ = json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	for i := 0; i < 2; i++ {
		if err = c.doReadDrawing(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	row, err := d.GetMessageBySeq(ctx, tips.ConversationID, 6)
	if err != nil || !row.IsRead {
		t.Fatalf("local message not read: %+v %v", row, err)
	}
	conv, err := d.GetConversation(ctx, tips.ConversationID)
	if err != nil || conv.UnreadCount != 3 {
		t.Fatalf("notification replaced authoritative unread: %+v %v", conv, err)
	}
	if _, err = d.GetMessageBySeq(ctx, tips.ConversationID, 7); err == nil {
		t.Fatal("invented missing message")
	}
	d.Close(ctx)
	if err = c.doReadDrawing(ctx, msg); err == nil {
		t.Fatal("database failure hidden")
	}
}

type syncV2ReadListener struct {
	open_im_sdk_callback.OnAdvancedMsgListener
	receipts int
}

func (l *syncV2ReadListener) OnRecvC2CReadReceipt(string) { l.receipts++ }
func TestSyncV2PeerReadBeforeLatestMessageProjection(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	l := &syncV2ReadListener{}
	c := &Conversation{db: d, loginUserID: "u", syncV2: &conversationSyncV2Runtime{store: d}, msgListener: func() open_im_sdk_callback.OnAdvancedMsgListener { return l }}
	c.syncV2.mode.Store(syncV2Enabled)
	tips := &sdkws.MarkAsReadTips{ConversationID: "si_u_v", MarkAsReadUserID: "v", Seqs: []int64{7}}
	body, _ := json.Marshal(tips)
	content, _ := json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	msg := &sdkws.MsgData{Content: content, SendTime: 123}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: tips.ConversationID, ConversationType: constant.SingleChatType}); err != nil {
		t.Fatal(err)
	}
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if l.receipts != 0 {
		t.Fatal("empty receipt emitted")
	}
	if err = d.BatchInsertMessageList(ctx, tips.ConversationID, []*model_struct.LocalChatLog{{ClientMsgID: "sent", Seq: 7, SendID: "u"}}); err != nil {
		t.Fatal(err)
	}
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	row, err := d.GetMessageBySeq(ctx, tips.ConversationID, 7)
	if err != nil || !row.IsRead || l.receipts != 1 {
		t.Fatalf("receipt not applied: %+v %v %d", row, err, l.receipts)
	}
	tips.Seqs = nil
	body, _ = json.Marshal(tips)
	msg.Content, _ = json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	tips.ConversationID = "si_u_unloaded"
	body, _ = json.Marshal(tips)
	msg.Content, _ = json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if l.receipts != 1 {
		t.Fatal("invented receipt for unloaded chat")
	}
}

func TestSyncV2LiveSelfReadRefreshesAuthoritativeUnreadWithoutHistory(t *testing.T) {
	states := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var result any
		switch r.URL.Path {
		case "/conversation/get_incremental_conversations":
			result = &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1}
		case "/msg/sync_seqs_v2":
			states++
			var q pb.StateRequest
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				t.Error(err)
			}
			if len(q.ConversationIDs) != 1 || q.ConversationIDs[0] != "si_u_v" {
				t.Errorf("activated unloaded receipt: %+v", q)
			}
			result = &pb.StateResponse{States: []*pb.State{{ConversationID: "si_u_v", Known: true, StateToken: "t", MinSeq: 20, MaxSeq: 30, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "si_u_v"}}}}
		default:
			t.Errorf("unexpected/history request: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": result})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx := ccontext.WithInfo(context.Background(), config)
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"si_u_v"}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "si_u_v", UnreadCount: 9}); err != nil {
		t.Fatal(err)
	}
	c := &Conversation{db: d, loginUserID: "u", syncV2: &conversationSyncV2Runtime{store: d, client: syncv2.NewClient(server.URL, "", server.Client()), caps: &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}}}
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "ready"})
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	if err = c.doConversationSyncV2SelfRead(ctx, &sdkws.MarkAsReadTips{ConversationID: "si_u_unloaded", MarkAsReadUserID: "u"}); err != nil {
		t.Fatal(err)
	}
	row, err := d.GetConversation(ctx, "si_u_v")
	if err != nil || row.UnreadCount != 2 || row.MinSeq != 20 || states != 1 {
		t.Fatalf("authoritative state not refreshed: %+v %v requests=%d", row, err, states)
	}
	if _, err = d.GetConversation(ctx, "si_u_unloaded"); err == nil {
		t.Fatal("read notification activated a chat")
	}
}
