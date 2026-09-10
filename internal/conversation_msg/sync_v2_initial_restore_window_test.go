//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

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
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2InitialPagePreservesRestoreWindow(t *testing.T) {
	testInitialRestorePage(t, "ready")
}

func TestSyncV2InitialPageWaitsForConfirmedPermissions(t *testing.T) {
	for _, scenario := range []string{"notification-unknown", "chat-unavailable"} {
		t.Run(scenario, func(t *testing.T) { testInitialRestorePage(t, scenario) })
	}
}

func testInitialRestorePage(t *testing.T, scenario string) {
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"}))
	defer cancel()
	ctx = syncv2.WithChatWindow(ccontext.WithOperationID(ctx, "qa-restore-page"), 10)
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "messages"}
	states := &pb.StateResponse{}
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("si_u_%03d", i)
		cp.SelectedIDs = append(cp.SelectedIDs, id)
		if err := database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, ConversationType: constant.SingleChatType, UserID: "peer", MinSeq: 1, MaxSeq: 20}); err != nil {
			t.Fatal(err)
		}
		states.States = append(states.States, &pb.State{ConversationID: id, Known: true, MinSeq: 1, MaxSeq: 20, LastMessageTime: int64(i + 1), Conversation: &conversation.Conversation{ConversationID: id}})
	}
	if err := database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := database.InsertFriend(ctx, &model_struct.LocalFriend{OwnerUserID: "u", FriendUserID: "peer", Nickname: "Peer"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	requested := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := &pb.StateResponse{}
			if scenario == "chat-unavailable" && !req.Notifications {
				resp.UnavailableIDs = append(resp.UnavailableIDs, req.ConversationIDs...)
				data = resp
				break
			}
			for _, id := range req.ConversationIDs {
				state := &pb.State{ConversationID: id, Known: true, MinSeq: 1, MaxSeq: 20, StateToken: "token"}
				if req.Notifications {
					state.ConversationID = "n" + id[2:]
					state.MaxSeq = 0
					if scenario == "notification-unknown" {
						state.Known = false
					}
				}
				resp.States = append(resp.States, state)
			}
			if req.IncludeSelf {
				resp.States = append(resp.States, &pb.State{ConversationID: "n_u_u", Known: true, StateToken: "token"})
			}
			data = resp
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			requested[req.ConversationID] = true
			mu.Unlock()
			if req.Notifications || len(req.Seqs) != 10 || req.Seqs[0] != 11 || req.Seqs[9] != 20 {
				t.Errorf("unexpected preview pull: %+v", req)
			}
			response := &pb.PullResponse{State: &pb.State{ConversationID: req.ConversationID, Known: true, MinSeq: 1, MaxSeq: 20, StateToken: "token"}}
			for _, seq := range req.Seqs {
				response.Messages = append(response.Messages, &sdkws.MsgData{ClientMsgID: fmt.Sprintf("%s_%d", req.ConversationID, seq), Seq: seq, SendTime: 100, ContentType: constant.Text, Content: []byte(`{"content":"preview"}`)})
			}
			data = response
		default:
			t.Errorf("unexpected full-sync route: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	queue := make(chan common.Cmd2Value, 100)
	u := user.NewUser(database, "u", queue)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, relation.NewFriend("u", database, u, queue), group.NewGroup("u", database, queue), u, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.client = syncv2.NewClient(server.URL, "", server.Client())
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "syncing", Progress: 10})
	listener := &syncV2Listener{}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })

	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	round := &syncv2.Round{Checkpoint: cp, ChatStates: states, Capabilities: &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}}
	if err := c.prepareConversationSyncV2InitialPage(ctx, round); err != nil {
		t.Fatal(err)
	}
	if s := c.GetConversationSyncState(); s.InitialDataReady != (scenario == "ready") || s.Phase != "syncing" || s.Progress != 10 {
		t.Fatalf("premature full sync: %+v", s)
	}
	if cp, err := database.GetConversationSyncV2(ctx); err != nil || cp.Stage != "messages" {
		t.Fatalf("premature checkpoint: %+v %v", cp, err)
	}
	if listener.finishes != 0 {
		t.Fatal("preview emitted full sync finish")
	}
	if scenario != "ready" {
		if len(requested) != 0 {
			t.Fatalf("unconfirmed permissions pulled chats: %v", requested)
		}
		return
	}
	if len(requested) != 50 {
		t.Fatalf("pulled %d chats", len(requested))
	}
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("si_u_%03d", i)
		if requested[id] != (i >= 10) {
			t.Fatalf("wrong preview membership: %s", id)
		}
	}
	for i := 10; i < 60; i++ {
		id := fmt.Sprintf("si_u_%03d", i)
		for seq := int64(11); seq <= 20; seq++ {
			if _, err := database.GetMessageBySeq(ctx, id, seq); err != nil {
				t.Fatalf("restore window lost %s seq %d: %v", id, seq, err)
			}
		}
	}
	page, err := database.GetConversationListSplitDB(ctx, 0, 50)
	if err != nil || len(page) != 50 {
		t.Fatalf("preview not durably readable: %d %v", len(page), err)
	}
}
