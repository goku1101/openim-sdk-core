//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestNotificationRetryCompletesOnlyAfterConfirmedState(t *testing.T) {
	for _, self := range []bool{false, true} {
		for _, tail := range []int64{0, 1} {
			t.Run(fmt.Sprintf("self_%t_tail_%d", self, tail), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				database, err := db.NewDataBase(ctx, "alice", t.TempDir(), 0)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close(context.Background())
				store := &observedRetryStore{database, make(chan string, 8)}
				request := &pb.PullRequest{UserID: "alice", ConversationID: "si_alice_bob", Notifications: true}
				streamID := "n_alice_bob"
				if self {
					request.ConversationID = ""
					request.Self = true
					streamID = "n_alice_alice"
				}
				var known atomic.Bool
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					state := &pb.State{ConversationID: streamID, MinSeq: 1, StateToken: "state"}
					if known.Load() {
						state.Known = true
						state.MaxSeq = tail
					}
					var data any
					switch r.URL.Path {
					case "/msg/sync_seqs_v2":
						data = &pb.StateResponse{States: []*pb.State{state}}
					case "/msg/sync_messages_v2":
						if !known.Load() {
							t.Error("pulled unconfirmed notification")
						}
						data = &pb.PullResponse{State: state, Messages: []*sdkws.MsgData{{Seq: 1, ClientMsgID: "notification-one"}}}
					default:
						t.Errorf("unexpected route %s", r.URL.Path)
						w.WriteHeader(404)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
				}))
				defer server.Close()
				client := syncv2.NewClient(server.URL, "", server.Client())
				payload, _ := json.Marshal(request)
				taskID := "reconcile:" + streamID
				if err = database.PutConversationSyncV2Retry(ctx, &model_struct.LocalConversationSyncV2Retry{TaskID: taskID, Kind: "reconcile", Payload: string(payload)}); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					done <- syncv2.RunRetryTasks(ctx, store, make(chan struct{}), func(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
						var req pb.PullRequest
						if err := json.Unmarshal([]byte(job.Payload), &req); err != nil {
							return err
						}
						_, err := client.SyncStream(ctx, "retry-test", &req, &pb.Capabilities{MaxBatchSize: 100, MaxRequestBytes: 65536}, database, func(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
							var seqs []int64
							for _, m := range messages {
								seqs = append(seqs, m.Seq)
							}
							return database.CommitConversationSyncV2Notifications(ctx, progress.Epoch, progress, seqs)
						})
						return err
					})
				}()
				defer func() { cancel(); <-done }()
				awaitRetryEvent(t, store.events, "defer:"+taskID)
				job, err := database.NextConversationSyncV2Retry(ctx)
				if err != nil || job == nil || job.Attempts != 1 {
					t.Fatalf("unknown task lost: %+v %v", job, err)
				}
				known.Store(true)
				awaitRetryEvent(t, store.events, "ack:"+taskID)
				job, err = database.NextConversationSyncV2Retry(ctx)
				if err != nil || job != nil {
					t.Fatalf("confirmed task remained: %+v %v", job, err)
				}
				stream, err := database.GetConversationSyncV2Stream(ctx, streamID)
				if err != nil || !stream.Initialized || stream.ScannedSeq != tail || len(stream.PendingSeqs) != 0 {
					t.Fatalf("wrong committed state: %+v %v", stream, err)
				}
			})
		}
	}
}
