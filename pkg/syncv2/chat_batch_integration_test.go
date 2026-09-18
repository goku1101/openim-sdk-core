//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestChatBatchIntegration(t *testing.T) {
	for _, scenario := range []string{"combined", "mixed", "busy", "server_error", "unsupported", "malformed", "disabled", "version", "item_split", "pages", "bytes", "timeout", "internal", "unavailable", "holes", "epoch"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			caps := &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536, MultiChatMessagePull: true, ChatMessagePullVersion: 1, MaxChatMessagePullStreams: 3, MaxChatMessagePullSeqsPerStream: 2, MaxChatMessagePullSeqs: 4, MaxChatMessageRequestBytes: 2048, MaxChatMessageResponseBytes: 3 * 1024 * 1024}
			if scenario == "disabled" {
				caps.MultiChatMessagePull = false
			}
			if scenario == "version" {
				caps.ChatMessagePullVersion = 2
			}
			if scenario == "bytes" {
				caps.MaxChatMessageRequestBytes = 300
			}
			maxSeq := int64(1)
			if scenario == "pages" {
				maxSeq = 5
			}
			if scenario == "item_split" {
				maxSeq = 2
			}
			state := func(id string) *pb.State {
				return &pb.State{ConversationID: id, Known: true, MinSeq: 1, MaxSeq: maxSeq, StateToken: strings.Repeat("a", 64)}
			}
			msgs := func(id string, seqs []int64) []*sdkws.MsgData {
				var ms []*sdkws.MsgData
				for _, s := range seqs {
					ms = append(ms, &sdkws.MsgData{ClientMsgID: fmt.Sprintf("%s:%d", id, s), Seq: s})
				}
				return ms
			}
			var batches, singles atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var out any
				switch r.URL.Path {
				case "/msg/sync_seqs_v2":
					var q pb.StateRequest
					json.NewDecoder(r.Body).Decode(&q)
					v := &pb.StateResponse{}
					for _, id := range q.ConversationIDs {
						v.States = append(v.States, state(id))
					}
					out = v
				case "/msg/sync_messages_v2":
					singles.Add(1)
					var q pb.PullRequest
					json.NewDecoder(r.Body).Decode(&q)
					out = &pb.PullResponse{State: state(q.ConversationID), Messages: msgs(q.ConversationID, q.Seqs)}
				case "/msg/sync_chat_messages_batch_v2":
					batches.Add(1)
					if scenario == "busy" {
						w.WriteHeader(503)
						return
					}
					if scenario == "server_error" {
						json.NewEncoder(w).Encode(map[string]any{"errCode": 500, "errMsg": "ServerInternalError"})
						return
					}
					if scenario == "unsupported" {
						json.NewEncoder(w).Encode(map[string]any{"errCode": 12, "errMsg": "SYNC_V2_CHAT_BATCH_UNSUPPORTED"})
						return
					}
					var raw map[string]json.RawMessage
					json.NewDecoder(r.Body).Decode(&raw)
					body, _ := json.Marshal(raw)
					if len(body) > int(caps.MaxChatMessageRequestBytes) {
						t.Error("request bytes exceeded")
					}
					var q pb.ChatBatchPullRequest
					json.Unmarshal(body, &q)
					if len(q.Items) > 3 {
						t.Error("too many streams")
					}
					var items []map[string]any
					json.Unmarshal(raw["items"], &items)
					for _, it := range items {
						if _, ok := it["notifications"]; ok {
							t.Error("notification field sent")
						}
						if _, ok := it["self"]; ok {
							t.Error("self field sent")
						}
					}
					v := &pb.ChatBatchPullResponse{}
					total := 0
					seen := map[string]bool{}
					for _, it := range q.Items {
						if seen[it.ConversationID] || len(it.Seqs) > 2 || len(it.Seqs) == 0 {
							t.Error("invalid batch shape")
						}
						seen[it.ConversationID] = true
						total += len(it.Seqs)
						row := &pb.ChatBatchPullResult{RequestID: it.RequestID, ConversationID: it.ConversationID, State: state(it.ConversationID), Messages: msgs(it.ConversationID, it.Seqs)}
						if scenario == "mixed" && it.ConversationID == "sg_0" {
							row.State = nil
							row.Messages = nil
							row.ErrorCode = 10
							row.ErrorReason = "SYNC_V2_STATE_CHANGED"
						}
						if it.ConversationID == "sg_0" {
							switch scenario {
							case "timeout":
								row.Messages = nil
								row.ErrorCode = 4
								row.ErrorReason = "SYNC_V2_ITEM_TIMEOUT"
							case "internal":
								row.Messages = nil
								row.ErrorCode = 13
								row.ErrorReason = "SYNC_V2_ITEM_INTERNAL"
							case "unavailable":
								row.Messages = nil
								row.ErrorCode = 7
								row.ErrorReason = "SYNC_V2_CONVERSATION_UNAVAILABLE"
							case "holes":
								row.Messages = nil
								row.UnresolvedSeqs = it.Seqs
							case "epoch":
								if e := d.InvalidateConversationSyncV2Stream(ctx, it.ConversationID); e != nil {
									t.Error(e)
								}
							}
						}
						if scenario == "item_split" && len(it.Seqs) > 1 {
							row.State = nil
							row.Messages = nil
							row.ErrorCode = 8
							row.ErrorReason = "SYNC_V2_ITEM_RESPONSE_LIMIT"
						}
						v.Results = append([]*pb.ChatBatchPullResult{row}, v.Results...)
					}
					if total > 4 {
						t.Error("too many sequences")
					}
					if scenario == "malformed" {
						v.Results = v.Results[:0]
					}
					out = v
				default:
					t.Error("wrong route", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": out})
			}))
			defer server.Close()
			var ids []string
			for i := 0; i < 10; i++ {
				prefix := "sg"
				if i%2 == 1 {
					prefix = "si_u"
				}
				ids = append(ids, fmt.Sprintf("%s_%d", prefix, i))
			}
			var committed atomic.Int32
			result, err := syncv2.NewClient(server.URL, "", server.Client()).SyncStreams(ctx, "op", &pb.StateRequest{UserID: "u", ConversationIDs: ids}, caps, d, func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, ms []*sdkws.MsgData) error {
				rows := make([]*model_struct.LocalChatLog, 0, len(ms))
				for _, m := range ms {
					rows = append(rows, &model_struct.LocalChatLog{ClientMsgID: m.ClientMsgID, Seq: m.Seq})
				}
				e := d.CommitConversationSyncV2Messages(ctx, p.Epoch, p, rows)
				if e == nil {
					committed.Add(int32(len(ms)))
				}
				return e
			})
			if scenario == "unsupported" || scenario == "malformed" {
				if err == nil || singles.Load() != 0 || committed.Load() != 0 {
					t.Fatalf("unexpected fallback/commit err=%v singles=%d commits=%d", err, singles.Load(), committed.Load())
				}
				if scenario == "unsupported" && syncv2.RecoveryFor(err) != syncv2.Renegotiate {
					t.Fatal("must renegotiate")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "epoch" {
				if committed.Load() != 9 || len(result.PendingIDs) != 1 {
					t.Fatalf("stale write accepted commits=%d result=%+v", committed.Load(), result)
				}
				return
			}
			if scenario == "busy" || scenario == "server_error" {
				if singles.Load() != 0 || result.Completed != 0 || len(result.PendingIDs) != 10 {
					t.Fatalf("busy lost pending: %+v", result)
				}
				return
			}
			want := int32(10) * int32(maxSeq)
			if scenario == "mixed" || scenario == "timeout" || scenario == "internal" || scenario == "holes" {
				want = 9
				if len(result.PendingIDs) != 1 {
					t.Fatal("failed sibling not pending")
				}
			}
			if scenario == "unavailable" {
				want = 9
				if len(result.UnavailableIDs) != 1 {
					t.Fatal("unavailable lost")
				}
			}
			if committed.Load() != want {
				t.Fatalf("committed %d want %d", committed.Load(), want)
			}
			if scenario == "disabled" || scenario == "version" {
				if batches.Load() != 0 || singles.Load() != 10 {
					t.Fatal("capability ignored")
				}
			} else if singles.Load() != 0 {
				t.Fatal("chat batch fell through to single")
			}
			if scenario == "combined" && batches.Load() != 4 {
				t.Fatalf("batch calls=%d want4", batches.Load())
			}
		})
	}
}

func TestChatBatchLeavesForegroundReadAvailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	token := strings.Repeat("a", 64)
	caps := &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536, MultiChatMessagePull: true, ChatMessagePullVersion: 1, MaxChatMessagePullStreams: 3, MaxChatMessagePullSeqsPerStream: 2, MaxChatMessagePullSeqs: 4, MaxChatMessageRequestBytes: 2048, MaxChatMessageResponseBytes: 3 * 1024 * 1024, ClusterReadAdmission: true, ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxPageSize: 100, SnapshotTTLSeconds: 300}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var out any
		state := &pb.State{ConversationID: "sg_1", Known: true, MinSeq: 1, MaxSeq: 1, StateToken: token}
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			out = caps
		case "/msg/sync_chat_messages_batch_v2":
			var q pb.ChatBatchPullRequest
			json.NewDecoder(r.Body).Decode(&q)
			if len(q.Items) != 1 || len(q.Items[0].Seqs) != 1 {
				t.Error("sequences not deduplicated")
			}
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			out = &pb.ChatBatchPullResponse{Results: []*pb.ChatBatchPullResult{{RequestID: q.Items[0].RequestID, ConversationID: "sg_1", State: state, Messages: []*sdkws.MsgData{{Seq: 1, ClientMsgID: "m"}}}}}
		case "/msg/sync_messages_v2":
			out = &pb.PullResponse{State: state, Messages: []*sdkws.MsgData{{Seq: 1, ClientMsgID: "m"}}}
		default:
			t.Error("wrong route", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": out})
	}))
	defer server.Close()
	var releaseOnce sync.Once
	releaseBatch := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBatch()
	client := syncv2.NewClient(server.URL, "", server.Client())
	if _, err := client.Capabilities(ctx, "cap", &pb.UserRequest{UserID: "u"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.PullBatched(ctx, "bg", &pb.PullRequest{UserID: "u", ConversationID: "sg_1", StateToken: token, Seqs: []int64{1, 1}}, caps)
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("background did not enter")
	}
	foreground, err := client.PullBatched(syncv2.WithForegroundRead(ctx), "fg", &pb.PullRequest{UserID: "u", ConversationID: "sg_1", StateToken: token, Seqs: []int64{1}}, caps)
	if err != nil || foreground == nil || len(foreground.Messages) != 1 {
		t.Fatalf("foreground blocked by batch: %v", err)
	}
	releaseBatch()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
