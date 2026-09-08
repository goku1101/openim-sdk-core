//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncStreamResumesHolesAndDoesNotAdvanceOnConsumerFailure(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	var pulls [][]int64
	hole := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &pb.State{ConversationID: "si_u_v", Known: true, MinSeq: 1, MaxSeq: 3, StateToken: "boundary"}
		var data any
		switch r.URL.Path {
		case "/msg/sync_seqs_v2":
			data = &pb.StateResponse{States: []*pb.State{state}}
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			pulls = append(pulls, req.Seqs)
			response := &pb.PullResponse{State: state}
			for _, seq := range req.Seqs {
				if seq == 2 && hole {
					response.UnresolvedSeqs = append(response.UnresolvedSeqs, seq)
				} else {
					response.Messages = append(response.Messages, &sdkws.MsgData{Seq: seq, ClientMsgID: string(rune('a' + seq))})
				}
			}
			data = response
		default:
			t.Errorf("unexpected route: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	request := &pb.PullRequest{UserID: "u", ConversationID: "si_u_v"}
	caps := &pb.Capabilities{MaxBatchSize: 2, MaxRequestBytes: 65536}
	fail := true
	commit := func(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
		if fail {
			return errors.New("consumer failed")
		}
		var rows []*model_struct.LocalChatLog
		for _, msg := range messages {
			rows = append(rows, &model_struct.LocalChatLog{ClientMsgID: msg.ClientMsgID, Seq: msg.Seq})
		}
		return database.CommitConversationSyncV2Messages(ctx, progress.Epoch, progress, rows)
	}
	if _, err = client.SyncStream(ctx, "op", request, caps, database, commit); err == nil {
		t.Fatal("consumer failure was hidden")
	}
	stream, err := database.GetConversationSyncV2Stream(ctx, "si_u_v")
	if err != nil || stream.ScannedSeq != 0 {
		t.Fatalf("failed delivery advanced cursor: %+v %v", stream, err)
	}
	fail = false
	stream, err = client.SyncStream(ctx, "op", request, caps, database, commit)
	if err != nil || stream.ScannedSeq != 3 || !reflect.DeepEqual(stream.PendingSeqs, []int64{2}) {
		t.Fatalf("hole lost: %+v %v", stream, err)
	}
	hole = false
	pulls = nil
	stream, err = client.SyncStream(ctx, "op", request, caps, database, commit)
	if err != nil || len(stream.PendingSeqs) != 0 || !reflect.DeepEqual(pulls, [][]int64{{2}}) {
		t.Fatalf("resume did not target the hole: %+v %v %v", stream, pulls, err)
	}
}
