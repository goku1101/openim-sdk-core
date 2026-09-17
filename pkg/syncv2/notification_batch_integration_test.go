package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestNotificationBatchIntegration(t *testing.T) {
	for _, scenario := range []string{"combined", "mixed", "stale", "fallback", "busy", "malformed", "split", "item_split", "long", "small_limits", "disabled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			caps := &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, MultiStreamMessagePull: true, MaxMessagePullStreams: 25, MaxMessagePullSeqsPerStream: 100, MaxMessagePullSeqs: 500, MaxMessagePullWorkers: 4}
			if scenario == "small_limits" {
				caps.MaxMessagePullStreams = 2
				caps.MaxMessagePullSeqsPerStream = 2
				caps.MaxMessagePullSeqs = 3
			}
			if scenario == "disabled" {
				caps.MultiStreamMessagePull = false
			}
			maxSeq := int64(1)
			if scenario == "long" || scenario == "item_split" {
				maxSeq = 201
			}
			state := func(id string) *pb.State {
				return &pb.State{ConversationID: "n" + id[2:], Known: true, StateToken: strings.Repeat("a", 64), MinSeq: 1, MaxSeq: maxSeq}
			}
			messages := func(id string, seqs []int64) []*sdkws.MsgData {
				out := make([]*sdkws.MsgData, 0, len(seqs))
				for _, seq := range seqs {
					out = append(out, &sdkws.MsgData{ClientMsgID: fmt.Sprintf("%s:%d", id, seq), Seq: seq})
				}
				return out
			}
			var batchCalls, singleCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var out any
				switch r.URL.Path {
				case "/conversation/sync_capabilities":
					out = caps
				case "/msg/sync_seqs_v2":
					var q pb.StateRequest
					json.NewDecoder(r.Body).Decode(&q)
					resp := &pb.StateResponse{}
					for _, id := range q.ConversationIDs {
						resp.States = append(resp.States, state(id))
					}
					out = resp
				case "/msg/sync_messages_v2":
					singleCalls.Add(1)
					var q pb.PullRequest
					json.NewDecoder(r.Body).Decode(&q)
					out = &pb.PullResponse{State: state(q.ConversationID), Messages: messages(q.ConversationID, q.Seqs)}
				case "/msg/sync_messages_batch_v2":
					batchCalls.Add(1)
					if scenario == "fallback" {
						w.WriteHeader(404)
						return
					}
					if scenario == "busy" {
						w.WriteHeader(503)
						return
					}
					var q pb.BatchPullRequest
					json.NewDecoder(r.Body).Decode(&q)
					total := 0
					seen := map[string]bool{}
					for _, it := range q.Items {
						total += len(it.Seqs)
						if seen[it.RequestID] || it.RequestID == "" || !it.Notifications || len(it.Seqs) > int(caps.MaxMessagePullSeqsPerStream) {
							t.Error("invalid batch item")
						}
						seen[it.RequestID] = true
					}
					if total > int(caps.MaxMessagePullSeqs) || len(q.Items) > int(caps.MaxMessagePullStreams) {
						t.Error("batch limit exceeded")
					}
					if scenario == "split" && len(q.Items) > 2 {
						json.NewEncoder(w).Encode(map[string]any{"errCode": 8, "errMsg": "SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH"})
						return
					}
					resp := &pb.BatchPullResponse{}
					for _, it := range q.Items {
						result := &pb.BatchPullResult{RequestID: it.RequestID, ConversationID: it.ConversationID, State: state(it.ConversationID), Messages: messages(it.ConversationID, it.Seqs)}
						if scenario == "mixed" {
							switch it.ConversationID {
							case "si_01":
								result.ErrorCode = 10
								result.ErrorReason = "SYNC_V2_STATE_CHANGED"
								result.Messages = nil
							case "si_02":
								result.ErrorCode = 7
								result.ErrorReason = "SYNC_V2_CONVERSATION_UNAVAILABLE"
								result.Messages = nil
							case "si_03":
								result.UnresolvedSeqs = it.Seqs
								result.Messages = nil
							}
						}
						if scenario == "item_split" && len(it.Seqs) > 50 {
							result.ErrorCode = 8
							result.ErrorReason = "SYNC_V2_READ_BYTES_REDUCE_BATCH"
							result.Messages = nil
						}
						// Intentionally reverse response order to reject positional correlation.
						resp.Results = append([]*pb.BatchPullResult{result}, resp.Results...)
					}
					if scenario == "malformed" {
						resp.Results = resp.Results[:len(resp.Results)-1]
					}
					out = resp
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": out})
			}))
			defer server.Close()
			client := syncv2.NewClient(server.URL, "", server.Client())
			negotiated, err := client.Capabilities(ctx, "caps", &pb.UserRequest{UserID: "u"})
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 25)
			for i := range ids {
				ids[i] = fmt.Sprintf("si_%02d", i)
			}
			committed := map[string]map[int64]bool{}
			result, err := client.SyncStreams(ctx, "batch", &pb.StateRequest{UserID: "u", Notifications: true, ConversationIDs: ids}, negotiated, d, func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, msgs []*sdkws.MsgData) error {
				if scenario == "stale" && p.ConversationID == "n_00" {
					if err := d.InvalidateConversationSyncV2Stream(ctx, "n_01"); err != nil {
						return err
					}
				}
				seqs := []int64{}
				for _, m := range msgs {
					if m.ClientMsgID != fmt.Sprintf("si%s:%d", p.ConversationID[1:], m.Seq) {
						t.Error("cross-stream message")
					}
					if committed[p.ConversationID] == nil {
						committed[p.ConversationID] = map[int64]bool{}
					}
					if committed[p.ConversationID][m.Seq] {
						t.Error("duplicate consumption")
					}
					committed[p.ConversationID][m.Seq] = true
					seqs = append(seqs, m.Seq)
				}
				return d.CommitConversationSyncV2Notifications(ctx, p.Epoch, p, seqs)
			})
			if scenario == "malformed" {
				if err == nil || len(committed) != 0 {
					t.Fatal("accepted malformed batch", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := 25
			switch scenario {
			case "mixed":
				expected = 22
				if result.Completed != 22 || len(result.PendingIDs) != 2 || len(result.UnavailableIDs) != 1 {
					t.Fatalf("mixed outcomes %+v", result)
				}
			case "stale":
				expected = 24
				if len(result.PendingIDs) != 1 || len(committed["n_01"]) != 0 {
					t.Fatal("consumed stale response")
				}
			case "busy":
				expected = 0
				if singleCalls.Load() != 0 || len(result.PendingIDs) != 25 {
					t.Fatal("busy downgraded or lost work")
				}
			}
			if len(committed) != expected {
				t.Fatalf("committed %d want %d", len(committed), expected)
			}
			if scenario == "combined" && (batchCalls.Load() != 1 || singleCalls.Load() != 0) {
				t.Fatalf("not combined: batch=%d single=%d", batchCalls.Load(), singleCalls.Load())
			}
			if scenario == "fallback" && (batchCalls.Load() != 1 || singleCalls.Load() != 25) {
				t.Fatalf("bad fallback: %d %d", batchCalls.Load(), singleCalls.Load())
			}
			if scenario == "disabled" && (batchCalls.Load() != 0 || singleCalls.Load() != 25) {
				t.Fatal("called unadvertised route")
			}
			if scenario == "long" || scenario == "item_split" {
				for _, seqs := range committed {
					if len(seqs) != 201 {
						t.Fatal("lost long-stream page", len(seqs))
					}
				}
			}
			if scenario == "combined" {
				_, err = client.SyncStreams(ctx, "warm", &pb.StateRequest{UserID: "u", Notifications: true, ConversationIDs: ids}, negotiated, d, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
					t.Error("idle stream consumed")
					return nil
				})
				if err != nil || batchCalls.Load() != 1 || singleCalls.Load() != 0 {
					t.Fatal("idle streams re-pulled", err)
				}
			}
		})
	}
}
