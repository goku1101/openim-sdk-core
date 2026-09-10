//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	pbConversation "github.com/openimsdk/protocol/conversation"
)

func TestPrepareDetailsCommitsServerStateBeforeMessageStage(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "si_u_v", DraftText: "keep draft"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"maxRequestBytes":65536,"snapshotTTLSeconds":600}}`)
		case "/conversation/sync_snapshot":
			var req pb.SnapshotRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.SnapshotResponse{SnapshotID: "s", Scope: req.Scope, Mode: "full", PolicyVersion: syncv2.PolicyVersion, ConversationIDs: []string{"si_u_v"}, Total: 1, IncludeSelf: req.Scope == "notifications"}})
		case "/conversation/sync_snapshot_release":
			fmt.Fprint(w, `{"errCode":0}`)
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			response := new(pb.StateResponse)
			if !req.Notifications {
				response.States = []*pb.State{{ConversationID: "si_u_v", Known: true, MinSeq: 5, MaxSeq: 10, UnreadCount: 3, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "si_u_v", UserID: "v", ConversationType: 1}}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
		default:
			t.Error("unexpected endpoint")
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	round, err := syncv2.NewClient(server.URL, "", server.Client()).PrepareDetails(ctx, "op", "u", database)
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := database.GetConversation(ctx, "si_u_v")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if conversation.DraftText != "keep draft" || conversation.MinSeq != 5 || conversation.MaxSeq != 10 || conversation.UnreadCount != 3 || checkpoint.Stage != "messages" || round.Checkpoint.Stage != "messages" {
		t.Fatalf("stage advanced without data: %+v %+v", conversation, checkpoint)
	}
}
