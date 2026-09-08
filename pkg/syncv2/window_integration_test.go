//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestChatWindowBoundsTrafficWithoutDroppingPendingOrNotifications(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		window, upper          int64
		notifications, pending bool
	}{
		{"fresh", 1, 1000000, false, false},
		{"restore-with-hole", 10, 1000000, false, true},
		{"notifications", 1, 30, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := syncv2.WithChatWindow(context.Background(), tc.window)
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			id := "sg_c"
			if tc.notifications {
				id = "n_c"
			}
			if tc.pending {
				if err = database.CommitConversationSyncV2Messages(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: id, ParentID: id, StateToken: "t", MinSeq: 1, ScannedSeq: 10, PendingSeqs: []int64{5}}, nil); err != nil {
					t.Fatal(err)
				}
			}
			var got []int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				state := &pb.State{ConversationID: id, Known: true, StateToken: "t", MinSeq: 1, MaxSeq: tc.upper}
				var data any
				switch r.URL.Path {
				case "/msg/sync_seqs_v2":
					data = &pb.StateResponse{States: []*pb.State{state}}
				case "/msg/sync_messages_v2":
					var req pb.PullRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if !tc.notifications {
						for _, seq := range req.Seqs {
							if seq != 5 && seq < tc.upper-tc.window+1 {
								t.Errorf("unbounded history request: %v", req.Seqs)
								w.WriteHeader(500)
								return
							}
						}
					}
					got = append(got, req.Seqs...)
					response := &pb.PullResponse{State: state}
					for _, seq := range req.Seqs {
						response.Messages = append(response.Messages, &sdkws.MsgData{Seq: seq, ClientMsgID: fmt.Sprintf("m%d", seq)})
					}
					data = response
				default:
					t.Errorf("unexpected route: %s", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
			}))
			defer server.Close()
			client := syncv2.NewClient(server.URL, "", server.Client())
			stream, err := client.SyncStream(ctx, "op", &pb.PullRequest{UserID: "u", ConversationID: "sg_c", Notifications: tc.notifications}, &pb.Capabilities{MaxBatchSize: 4, MaxRequestBytes: 65536}, database, func(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
				var rows []*model_struct.LocalChatLog
				var seqs []int64
				for _, m := range messages {
					rows = append(rows, &model_struct.LocalChatLog{ClientMsgID: m.ClientMsgID, Seq: m.Seq})
					seqs = append(seqs, m.Seq)
				}
				if tc.notifications {
					return database.CommitConversationSyncV2Notifications(ctx, progress.Epoch, progress, seqs)
				}
				return database.CommitConversationSyncV2Messages(ctx, progress.Epoch, progress, rows)
			})
			if err != nil {
				t.Fatal(err)
			}
			var want []int64
			if tc.pending {
				want = append(want, 5)
			}
			lower := tc.upper - tc.window + 1
			if tc.notifications {
				lower = 1
			}
			for seq := lower; seq <= tc.upper; seq++ {
				want = append(want, seq)
			}
			if !reflect.DeepEqual(got, want) || stream.ScannedSeq != tc.upper || len(stream.PendingSeqs) != 0 {
				t.Fatalf("window/progress incorrect: requests=%v stream=%+v", got, stream)
			}
			if !tc.notifications {
				if _, err = database.GetMessageBySeq(ctx, id, tc.upper-100); err == nil {
					t.Fatal("unrequested history became a tombstone")
				}
			}
		})
	}
}
