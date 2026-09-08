//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/internal/user"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSyncV2PushRestoresNewChatWithoutNotifyingOldHistory(t *testing.T) {
	for _, upper := range []int64{3, 1000000} {
		t.Run(fmt.Sprint(upper), func(t *testing.T) { testSyncV2PushWindow(t, upper) })
	}
}

func testSyncV2PushWindow(t *testing.T, upper int64) {
	var capabilityRequests atomic.Int32
	var deniedUnknown atomic.Bool
	var deniedNotification atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &pb.State{ConversationID: "si_peer_u", Known: true, StateToken: "t", MinSeq: 1, MaxSeq: upper, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "si_peer_u", UserID: "peer", ConversationType: constant.SingleChatType}}
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			capabilityRequests.Add(1)
			w.WriteHeader(500)
			return
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.ConversationIDs) == 1 && req.ConversationIDs[0] == "si_denied_u" {
				if deniedUnknown.Load() {
					_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{{ConversationID: "si_denied_u", Known: false}}}})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{UnavailableIDs: []string{"si_denied_u"}}})
				return
			}
			if req.Notifications {
				if deniedNotification.Load() {
					_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{UnavailableIDs: req.ConversationIDs}})
					return
				}
				response := &pb.StateResponse{}
				if req.IncludeSelf {
					response.States = append(response.States, &pb.State{ConversationID: "n_u_u", Known: true, StateToken: "nt", MinSeq: 1, MaxSeq: 1})
				}
				for _, id := range req.ConversationIDs {
					if id != "sg_deferred" {
						t.Errorf("unexpected notification parent: %s", id)
					}
					response.States = append(response.States, &pb.State{ConversationID: "n_deferred", Known: true, StateToken: "nt", MinSeq: 1, MaxSeq: 1})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
				return
			}
			if len(req.ConversationIDs) != 1 || req.ConversationIDs[0] != "si_peer_u" {
				t.Errorf("push queried unrelated chat: %+v", req)
			}
			data = &pb.StateResponse{States: []*pb.State{state}}
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Notifications {
				id := "n_deferred"
				if req.Self {
					id = "n_u_u"
				}
				if req.StateToken != "nt" || !req.Self && req.ConversationID != "sg_deferred" {
					t.Errorf("wrong notification permission: %+v", req)
				}
				response := &pb.PullResponse{State: &pb.State{ConversationID: id, Known: true, StateToken: "nt", MinSeq: 1, MaxSeq: 1}, Messages: []*sdkws.MsgData{{ClientMsgID: id, Seq: 1, ContentType: constant.BusinessNotification, Content: []byte(fmt.Sprintf(`{"detail":%q}`, id))}}}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
				return
			}
			if req.ConversationID != "si_peer_u" || req.StateToken != "t" {
				t.Errorf("push bypassed authoritative state: %+v", req)
			}
			response := &pb.PullResponse{State: state}
			for _, seq := range req.Seqs {
				if seq != 3 && seq < max(int64(1), upper-9) {
					t.Errorf("push downloaded unrelated old history: %d", seq)
					w.WriteHeader(500)
					return
				}
				response.Messages = append(response.Messages, &sdkws.MsgData{ClientMsgID: fmt.Sprintf("m%d", seq), Seq: seq, SendID: "peer", RecvID: "u", SessionType: constant.SingleChatType, ContentType: constant.Text, SendTime: seq, Content: []byte(`{"content":"stored"}`)})
			}
			data = response
		default:
			t.Errorf("unexpected old route: %s", r.URL.Path)
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
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "snapshot", Generation: "g", SelectedIDs: []string{"sg_unrelated"}, NotificationIDs: []string{"si_peer_u", "sg_unrelated", "sg_deferred"}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_unrelated", ShowName: "untouched", LatestMsg: "keep"}); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertFriend(ctx, &model_struct.LocalFriend{OwnerUserID: "u", FriendUserID: "peer", Nickname: "Peer", Remark: "Remark", FaceURL: "face"}); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	u := user.NewUser(database, "u", queue)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, relation.NewFriend("u", database, u, queue), group.NewGroup("u", database, queue), u, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	c.syncV2.mode.Store(syncV2Enabled)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	listener := &syncV2PushConversationListener{syncV2Listener: &syncV2Listener{}}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })
	arrivals := 0
	c.msgListener = func() open_im_sdk_callback.OnAdvancedMsgListener {
		return &syncV2LiveListener{onMessage: func(value string) {
			var message sdk_struct.MsgStruct
			_ = json.Unmarshal([]byte(value), &message)
			if message.ClientMsgID != "m3" {
				t.Errorf("old history notified as new: %s", message.ClientMsgID)
			}
			arrivals++
		}}
	}
	c.batchMsgListener = func() open_im_sdk_callback.OnBatchMsgListener { return nil }
	push := &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"si_peer_u": {Msgs: []*sdkws.MsgData{{ClientMsgID: "m3", Seq: 3, SendID: "peer", ContentType: constant.Text}}}}}
	push.NotificationMsgs = map[string]*sdkws.PullMsgs{
		"n_deferred": {Msgs: []*sdkws.MsgData{{Seq: 1}, {Seq: 0, ContentType: constant.BusinessNotification, Content: []byte(`{"detail":"transient"}`)}}},
		"n_u_u":      {Msgs: []*sdkws.MsgData{{Seq: 1}}},
	}
	notifications := make(map[string]int)
	c.SetBusinessListener(func() open_im_sdk_callback.OnCustomBusinessListener {
		return &syncV2PushBusinessListener{receive: func(value string) { notifications[value]++ }}
	})
	for i := 0; i < 2; i++ {
		handled, err := c.ProcessConversationSyncV2Push(ctx, push)
		if !handled || err != nil {
			t.Fatalf("push not completed: %v %v", handled, err)
		}
	}
	if arrivals != 1 || listener.newChats != 1 || listener.changedChats != 1 {
		t.Fatalf("wrong push callbacks: messages=%d new=%d changed=%d", arrivals, listener.newChats, listener.changedChats)
	}
	got, err := database.GetConversation(ctx, "si_peer_u")
	if err != nil || got.UnreadCount != 2 || got.LatestMsgSendTime != upper || got.ShowName != "Remark" {
		t.Fatalf("push projection incomplete: %+v %v", got, err)
	}
	expected := []int64{3}
	for seq := max(int64(1), upper-9); seq <= upper; seq++ {
		expected = append(expected, seq)
	}
	for _, seq := range expected {
		if _, err = database.GetMessageBySeq(ctx, "si_peer_u", seq); err != nil {
			t.Fatalf("history not persisted: %d %v", seq, err)
		}
	}

	if notifications["n_deferred"] != 1 || notifications["n_u_u"] != 1 || notifications["transient"] != 2 {
		t.Fatalf("notification progress/transient delivery incorrect: %v", notifications)
	}
	if _, err = database.GetConversation(ctx, "sg_deferred"); err == nil {
		t.Fatal("notification activated deferred chat")
	}
	for _, id := range []string{"n_deferred", "n_u_u"} {
		stream, err := database.GetConversationSyncV2Stream(ctx, id)
		if err != nil || stream.ScannedSeq != 1 {
			t.Fatalf("notification not committed: %+v %v", stream, err)
		}
	}
	push.Msgs["si_denied_u"] = &sdkws.PullMsgs{Msgs: []*sdkws.MsgData{{ClientMsgID: "denied", Seq: 1, SendID: "denied", ContentType: constant.Text}}}
	deniedNotification.Store(true)
	push.NotificationMsgs = map[string]*sdkws.PullMsgs{"n_deferred": {Msgs: []*sdkws.MsgData{{Seq: 2}}}}
	handled, err := c.ProcessConversationSyncV2Push(ctx, push)
	if !handled || err != nil || listener.changedChats != 2 {
		t.Fatalf("unavailable chat prevented valid chat processing: handled=%v err=%v changes=%d", handled, err, listener.changedChats)
	}
	if _, err = database.GetConversation(ctx, "si_denied_u"); err == nil {
		t.Fatal("unavailable chat was activated")
	}
	deniedUnknown.Store(true)
	handled, err = c.ProcessConversationSyncV2Push(ctx, &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"si_denied_u": push.Msgs["si_denied_u"]}})
	if !handled || !errors.Is(err, syncv2.ErrStatePending) {
		t.Fatalf("unknown state stopped retry: %v %v", handled, err)
	}

	handled, err = c.ProcessConversationSyncV2Push(ctx, &sdkws.PushMessages{NotificationMsgs: map[string]*sdkws.PullMsgs{"n_unknown": {Msgs: []*sdkws.MsgData{{Seq: 1}}}}})
	if !handled || err == nil || capabilityRequests.Load() != 1 {
		t.Fatalf("unknown notification did not attempt bounded rebuild: %v %v requests=%d", handled, err, capabilityRequests.Load())
	}
	if c.syncV2.mode.Load() != syncV2Enabled {
		t.Fatal("failed rebuild downgraded to legacy")
	}
	other, err := database.GetConversation(ctx, "sg_unrelated")
	if err != nil || other.ShowName != "untouched" || other.LatestMsg != "keep" {
		t.Fatalf("activation rewrote unrelated display: %+v %v", other, err)
	}
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "ready", Progress: 100})
	previousChanges := listener.changedChats
	payload, _ := json.Marshal(&pb.PullRequest{UserID: "u", ConversationID: "si_peer_u"})
	if err = c.runConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{Kind: "reconcile", Payload: string(payload)}); err != nil {
		t.Fatal(err)
	}
	if listener.changedChats != previousChanges+1 || arrivals != 1 {
		t.Fatalf("background reconciliation failed to publish or replayed history: changes=%d arrivals=%d", listener.changedChats, arrivals)
	}

}

type syncV2PushConversationListener struct {
	*syncV2Listener
	newChats, changedChats int
}

func (l *syncV2PushConversationListener) OnNewConversation(string)     { l.newChats++ }
func (l *syncV2PushConversationListener) OnConversationChanged(string) { l.changedChats++ }

type syncV2PushBusinessListener struct{ receive func(string) }

func (l *syncV2PushBusinessListener) OnRecvCustomBusinessMessage(value string) { l.receive(value) }
