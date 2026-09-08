//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestHistoryPageKeepsScanCursorAndUnresolvedSequences(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", DraftText: "draft"}); err != nil {
		t.Fatal(err)
	}
	if err = database.CommitConversationSyncV2Messages(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", MinSeq: 5, StateToken: "t", ScannedSeq: 100}, nil); err != nil {
		t.Fatal(err)
	}
	var pulled []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &pb.State{ConversationID: "c", Known: true, MinSeq: 5, MaxSeq: 100, StateToken: "t", UnreadCount: 2}
		var data any
		switch r.URL.Path {
		case "/msg/sync_seqs_v2":
			data = &pb.StateResponse{States: []*pb.State{state}}
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			pulled = append(pulled, req.Seqs...)
			result := &pb.PullResponse{State: state}
			for _, seq := range req.Seqs {
				if seq == 6 {
					result.UnresolvedSeqs = append(result.UnresolvedSeqs, seq)
				} else {
					result.Messages = append(result.Messages, &sdkws.MsgData{ClientMsgID: strconv.FormatInt(seq, 10), Seq: seq})
				}
			}
			data = result
		default:
			t.Errorf("unexpected route: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	page, err := client.ReadSequencePage(ctx, "op", &pb.PullRequest{UserID: "u", ConversationID: "c", Seqs: []int64{1, 5, 6, 9, 200, 5}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pulled, []int64{5, 6, 9}) {
		t.Fatalf("history exceeded visible bounds or duplicated work: %v", pulled)
	}
	rows := []*model_struct.LocalChatLog{}
	for _, message := range page.Response.Messages {
		rows = append(rows, &model_struct.LocalChatLog{ClientMsgID: message.ClientMsgID, Seq: message.Seq})
	}
	if err = database.CommitConversationSyncV2History(ctx, page.Progress.Epoch, page.Progress, rows, page.Response.State); err != nil {
		t.Fatal(err)
	}
	stream, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || stream.ScannedSeq != 100 || !reflect.DeepEqual(stream.PendingSeqs, []int64{6}) {
		t.Fatalf("history corrupted scan progress: %+v %v", stream, err)
	}
	if _, err = database.GetMessageBySeq(ctx, "c", 6); err == nil {
		t.Fatal("unresolved history became tombstone")
	}
	conversation, err := database.GetConversation(ctx, "c")
	if err != nil || conversation.MinSeq != 5 || conversation.MaxSeq != 100 || conversation.UnreadCount != 2 || conversation.DraftText != "draft" {
		t.Fatalf("history bounds not committed safely: %+v %v", conversation, err)
	}
}
