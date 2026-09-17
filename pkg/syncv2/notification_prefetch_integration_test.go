package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotificationPrefetchOrderAdmissionAndInvalidation(t *testing.T) {
	for _, invalidate := range []bool{false, true} {
		t.Run(fmt.Sprint(invalidate), func(t *testing.T) {
			ctx := context.Background()
			d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			var active, peak, pulls atomic.Int32
			state := func(id string) *pb.State {
				return &pb.State{ConversationID: "n" + id[2:], Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 1}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var out any
				switch r.URL.Path {
				case "/conversation/sync_capabilities":
					out = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, ClusterReadAdmission: true}
				case "/msg/sync_seqs_v2":
					var q pb.StateRequest
					json.NewDecoder(r.Body).Decode(&q)
					s := &pb.StateResponse{}
					for _, id := range q.ConversationIDs {
						s.States = append(s.States, state(id))
					}
					out = s
				case "/msg/sync_messages_v2":
					var q pb.PullRequest
					json.NewDecoder(r.Body).Decode(&q)
					n := active.Add(1)
					defer active.Add(-1)
					for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
					}
					pulls.Add(1)
					time.Sleep(15 * time.Millisecond)
					out = &pb.PullResponse{State: state(q.ConversationID), Messages: []*sdkws.MsgData{{ClientMsgID: q.ConversationID, Seq: 1}}}
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": out})
			}))
			defer server.Close()
			client := syncv2.NewClient(server.URL, "", server.Client())
			caps, err := client.Capabilities(ctx, "caps", &pb.UserRequest{UserID: "u"})
			if err != nil {
				t.Fatal(err)
			}
			var order []string
			_, err = client.SyncStreams(ctx, "scan", &pb.StateRequest{UserID: "u", Notifications: true, ConversationIDs: []string{"si_a", "si_b", "si_c", "si_d"}}, caps, d, func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, _ []*sdkws.MsgData) error {
				order = append(order, p.ConversationID)
				if invalidate && p.ConversationID == "n_a" {
					if err := d.InvalidateConversationSyncV2Stream(ctx, "n_b"); err != nil {
						return err
					}
				}
				time.Sleep(15 * time.Millisecond)
				return d.CommitConversationSyncV2Notifications(ctx, p.Epoch, p, []int64{1})
			})
			if invalidate {
				if !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || !reflect.DeepEqual(order, []string{"n_a"}) {
					t.Fatalf("stale callback consumed %v %v", order, err)
				}
			} else {
				if err != nil || !reflect.DeepEqual(order, []string{"n_a", "n_b", "n_c", "n_d"}) {
					t.Fatalf("order %v err %v", order, err)
				}
				if pulls.Load() != 4 {
					t.Fatal("duplicate network requests", pulls.Load())
				}
			}
			if peak.Load() != 2 {
				t.Fatal("account admission bypassed", peak.Load())
			}
		})
	}
}
