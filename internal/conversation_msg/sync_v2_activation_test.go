//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/internal/user"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/conversation"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSyncV2ActivationPublishesOnlyRequestedConversation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("unexpected route: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		var req pb.StateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.ConversationIDs) != 1 || req.ConversationIDs[0] != "si_peer_u" || !req.IncludeDetails {
			t.Errorf("unexpected activation: %+v", req)
		}
		state := &pb.State{ConversationID: "si_peer_u", Known: true, StateToken: "t", MinSeq: 5, MaxSeq: 9, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "si_peer_u", UserID: "peer", ConversationType: constant.SingleChatType}}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{state}}})
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
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "snapshot", Generation: "g", SelectedIDs: []string{"unrelated"}, NotificationIDs: []string{"si_peer_u", "unrelated"}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "unrelated", ShowName: "untouched", LatestMsg: "keep"}); err != nil {
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
	got, err := c.GetOneConversation(ctx, constant.SingleChatType, "peer")
	if err != nil || got.ShowName != "Remark" || got.FaceURL != "face" || got.MinSeq != 5 || got.MaxSeq != 9 || got.UnreadCount != 2 {
		t.Fatalf("activation did not restore details/display: %+v %v", got, err)
	}
	other, err := database.GetConversation(ctx, "unrelated")
	if err != nil || other.ShowName != "untouched" || other.LatestMsg != "keep" {
		t.Fatalf("activation rewrote unrelated display: %+v %v", other, err)
	}
}
