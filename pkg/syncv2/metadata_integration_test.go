//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

func TestMetadataCatchupReadsEffectiveStateWithoutLoadingProfileOnlyChat(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	before := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"si_u_loaded"}}
	if err = database.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	before.Stage = "messages"
	if err = database.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("legacy or unexpected endpoint: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		var req pb.StateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if !req.IncludeDetails || req.Notifications {
			t.Error("missing authoritative chat details")
		}
		requested = append(requested, req.ConversationIDs...)
		states := &pb.StateResponse{}
		for _, id := range req.ConversationIDs {
			states.States = append(states.States, &pb.State{ConversationID: id, Known: true, MinSeq: 20, MaxSeq: 30, UnreadCount: 2, StateToken: "t", IsPinned: id == "si_u_pinned", Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: id, MinSeq: 1, MaxSeq: 100}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": states})
	}))
	defer server.Close()
	delta := &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1, Update: []*conversation.Conversation{{ConversationID: "si_u_profile"}, {ConversationID: "si_u_pinned", IsPinned: true}}}
	client := syncv2.NewClient(server.URL, "", server.Client())
	got, err := client.ApplyMetadataDelta(ctx, "op", before, delta, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 1 || !reflect.DeepEqual(requested, []string{"si_u_loaded", "si_u_pinned"}) {
		t.Fatalf("incorrect catchup: %+v %v", got, requested)
	}
	if _, err = database.GetConversation(ctx, "si_u_profile"); err == nil {
		t.Fatal("profile-only change created a display conversation")
	}
	loaded, err := database.GetConversation(ctx, "si_u_loaded")
	if err != nil || loaded.MinSeq != 20 || loaded.MaxSeq != 30 || loaded.UnreadCount != 2 {
		t.Fatalf("effective boundaries not applied: %+v %v", loaded, err)
	}
}
