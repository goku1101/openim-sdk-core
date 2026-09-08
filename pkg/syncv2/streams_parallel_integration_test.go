package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestChatStreamsUseBoundedParallelPullsAndSerialCommits(t *testing.T) {
	for _, notifications := range []bool{false, true} {
		t.Run(fmt.Sprint(notifications), func(t *testing.T) {
			ctx := context.Background()
			d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			var active, peak, committing atomic.Int32
			state := func(id string) *pb.State {
				if notifications {
					id = "n" + id[2:]
				}
				return &pb.State{ConversationID: id, Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 1}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var result any
				if r.URL.Path == "/msg/sync_seqs_v2" {
					var q pb.StateRequest
					json.NewDecoder(r.Body).Decode(&q)
					resp := &pb.StateResponse{}
					for _, id := range q.ConversationIDs {
						resp.States = append(resp.States, state(id))
					}
					result = resp
				} else if r.URL.Path == "/msg/sync_messages_v2" {
					var q pb.PullRequest
					json.NewDecoder(r.Body).Decode(&q)
					n := active.Add(1)
					defer active.Add(-1)
					for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
					}
					select {
					case <-time.After(30 * time.Millisecond):
					case <-r.Context().Done():
						return
					}
					result = &pb.PullResponse{State: state(q.ConversationID), Messages: []*sdkws.MsgData{{ClientMsgID: q.ConversationID, Seq: 1}}}
				} else {
					t.Errorf("unexpected route: %s", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": result})
			}))
			defer server.Close()
			ids := []string{"si_1", "si_2", "si_3", "si_4", "si_5", "si_6", "si_7", "si_8"}
			commit := func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, msgs []*sdkws.MsgData) error {
				if committing.Add(1) != 1 {
					t.Error("concurrent commit callback")
				}
				defer committing.Add(-1)
				if notifications {
					return d.CommitConversationSyncV2Notifications(ctx, p.Epoch, p, []int64{1})
				}
				return d.CommitConversationSyncV2Messages(ctx, p.Epoch, p, []*model_struct.LocalChatLog{{ClientMsgID: msgs[0].ClientMsgID, Seq: 1}})
			}
			got, err := syncv2.NewClient(server.URL, "", server.Client()).SyncStreams(ctx, "op", &pb.StateRequest{UserID: "u", ConversationIDs: ids, Notifications: notifications}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, d, commit)
			if err != nil || got.Completed != 8 {
				t.Fatalf("incomplete: %+v %v", got, err)
			}
			if notifications && peak.Load() != 1 || !notifications && (peak.Load() < 2 || peak.Load() > 4) {
				t.Fatalf("incorrect concurrency: %d", peak.Load())
			}
			for _, id := range ids {
				if notifications {
					id = "n" + id[2:]
				}
				p, err := d.GetConversationSyncV2Stream(ctx, id)
				if err != nil || p.ScannedSeq != 1 {
					t.Fatalf("missing durable progress: %+v %v", p, err)
				}
			}
		})
	}
}

func TestChatStreamsCancellationWaitsForAllWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	entered := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/msg/sync_messages_v2" {
			io.Copy(io.Discard, r.Body)
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		var q pb.StateRequest
		json.NewDecoder(r.Body).Decode(&q)
		resp := &pb.StateResponse{}
		for _, id := range q.ConversationIDs {
			resp.States = append(resp.States, &pb.State{ConversationID: id, Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 1})
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
	}))
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		_, err := syncv2.NewClient(server.URL, "", server.Client()).SyncStreams(ctx, "op", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"si_1", "si_2", "si_3", "si_4", "si_5"}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, d, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
			t.Error("commit after cancelled HTTP")
			return nil
		})
		done <- err
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not stop")
	}
	p, err := d.GetConversationSyncV2Stream(context.Background(), "si_5")
	if err != nil || p.ScannedSeq != 0 {
		t.Fatalf("queued work advanced: %+v %v", p, err)
	}
}
