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

func TestNotificationPrefetchSkipsIdleSlots(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	var pulls atomic.Int32
	var starved atomic.Bool
	second := make(chan struct{})
	state := func(id string) *pb.State {
		maxSeq := int64(0)
		if id == "si_a" || id == "si_b" {
			maxSeq = 1
		}
		return &pb.State{ConversationID: "n" + id[2:], Known: true, StateToken: "t", MinSeq: 1, MaxSeq: maxSeq}
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
			if pulls.Add(1) == 2 {
				close(second)
			}
			if q.ConversationID == "si_a" {
				select {
				case <-second:
				case <-time.After(300 * time.Millisecond):
					starved.Store(true)
				}
			}
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
	result, err := client.SyncStreams(ctx, "sparse", &pb.StateRequest{UserID: "u", Notifications: true, ConversationIDs: []string{"si_a", "si_idle1", "si_idle2", "si_idle3", "si_idle4", "si_b"}}, caps, d, func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, msgs []*sdkws.MsgData) error {
		var seqs []int64
		for _, m := range msgs {
			seqs = append(seqs, m.Seq)
		}
		if len(msgs) > 0 {
			order = append(order, p.ConversationID)
		}
		return d.CommitConversationSyncV2Notifications(ctx, p.Epoch, p, seqs)
	})
	if err != nil {
		t.Fatal(err)
	}
	if starved.Load() {
		t.Error("idle streams occupied lookahead slots while a read slot was free")
	}
	if pulls.Load() != 2 || result.Completed != 6 || !reflect.DeepEqual(order, []string{"n_a", "n_b"}) {
		t.Fatalf("pulls=%d result=%+v order=%v", pulls.Load(), result, order)
	}
	_, err = client.SyncStreams(ctx, "repeat", &pb.StateRequest{UserID: "u", Notifications: true, ConversationIDs: []string{"si_a", "si_idle1", "si_idle2", "si_idle3", "si_idle4", "si_b"}}, caps, d, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
		t.Error("unchanged notifications were consumed again")
		return nil
	})
	if err != nil || pulls.Load() != 2 {
		t.Fatalf("unchanged streams re-pulled: pulls=%d err=%v", pulls.Load(), err)
	}
}
