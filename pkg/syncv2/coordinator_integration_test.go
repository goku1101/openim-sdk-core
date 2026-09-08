//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	conv "github.com/openimsdk/openim-sdk-core/v3/internal/conversation_msg"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestCoordinatorConsumesThroughRealConversationQueueBeforeReady(t *testing.T) {
	for _, name := range []string{"empty-chat-selection", "catchup", "consumer-failure", "late-notification-parent", "moving-notification-parents"} {
		t.Run(name, func(t *testing.T) {
			emptyChats := name == "empty-chat-selection"
			chatRequests, notificationRequests := 0, 0
			fail := name == "consumer-failure"
			movingParents := name == "moving-notification-parents"
			lateParent := name == "late-notification-parent"
			expectedReads := 2
			if lateParent || movingParents {
				expectedReads = 3
			}
			lateParentChecked := false
			ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"}))
			defer cancel()
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			queue := make(chan common.Cmd2Value, 100)
			consumer := conv.NewConversation(ctx, nil, database, queue, nil, nil, nil, nil, nil)
			queueStopped := make(chan struct{})
			go func() { defer close(queueStopped); common.DoListener(ctx, consumer) }()
			defer func() { cancel(); <-queueStopped }()
			var tail int64 = 1
			metadataReads := 0
			published := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var data any
				state := func(id string, chat bool) *pb.State {
					s := &pb.State{ConversationID: id, Known: true, MinSeq: 1, StateToken: "token"}
					if chat {
						s.MaxSeq = tail
						s.Conversation = &conversation.Conversation{OwnerUserID: "u", ConversationID: id, UserID: "v", ConversationType: 1}
					}
					return s
				}
				switch r.URL.Path {
				case "/conversation/sync_capabilities":
					data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600}
				case "/conversation/sync_snapshot":
					var req pb.SnapshotRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					response := &pb.SnapshotResponse{SnapshotID: "snapshot", Scope: req.Scope, Mode: "full", PolicyVersion: syncv2.PolicyVersion, ConversationIDs: []string{"si_u_v"}, Total: 1, IncludeSelf: req.Scope == "notifications", VersionID: "metadata", Version: 1}
					if emptyChats && req.Scope == "chat" {
						response.ConversationIDs = nil
						response.Total = 0
					}
					data = response
				case "/conversation/sync_snapshot_release":
					fmt.Fprint(w, `{"errCode":0}`)
					return
				case "/msg/sync_seqs_v2":
					var req pb.StateRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req.Notifications || req.IncludeSelf {
						notificationRequests++
					} else {
						chatRequests++
					}
					response := &pb.StateResponse{}
					for _, id := range req.ConversationIDs {
						if req.Notifications {
							if id == "sg_later" {
								lateParentChecked = true
							}
							id = "n" + id[2:]
						}
						response.States = append(response.States, state(id, !req.Notifications))
					}
					if req.IncludeSelf {
						response.States = append(response.States, state("n_u_u", false))
					}
					data = response
				case "/msg/sync_messages_v2":
					var req pb.PullRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req.Notifications || emptyChats {
						t.Error("empty stream pulled")
					}
					response := &pb.PullResponse{State: state(req.ConversationID, true)}
					for _, seq := range req.Seqs {
						response.Messages = append(response.Messages, &sdkws.MsgData{ClientMsgID: "m" + strconv.FormatInt(seq, 10), Seq: seq, ContentType: constant.Text, Content: []byte(`{"content":"hello"}`)})
					}
					data = response
				default:
					t.Errorf("unexpected/legacy route: %s", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
			}))
			defer server.Close()
			client := syncv2.NewClient(server.URL, "", server.Client())
			var progress []int
			result, err := client.Synchronize(ctx, "op", "u", database, syncv2.CoordinatorHooks{
				Progress: func(value int) {
					if value >= 100 || len(progress) > 0 && value <= progress[len(progress)-1] {
						t.Errorf("invalid progress: %v -> %d", progress, value)
					}
					progress = append(progress, value)
				},
				PrepareRelated: func(context.Context) error { return nil },
				ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
					metadataReads++
					tail = int64(metadataReads)
					delta := &conversation.GetIncrementalConversationResp{VersionID: "metadata", Version: 1}
					if lateParent && metadataReads == 2 {
						delta.Insert = []*conversation.Conversation{{ConversationID: "sg_later"}}
					}
					if movingParents {
						delta.Insert = []*conversation.Conversation{{ConversationID: "sg_later" + strconv.Itoa(metadataReads)}}
					}
					return delta, nil
				},
				Commit: func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, m []*sdkws.MsgData) error {
					if fail {
						return errors.New("consumer failed")
					}
					return consumer.ConsumeConversationSyncV2(ctx, p, m)
				},
				Publish: func(ctx context.Context, p *model_struct.LocalConversationSyncV2) error {
					published = true
					if lateParent && !lateParentChecked {
						return fmt.Errorf("new notification parent skipped before ready")
					}
					if emptyChats {
						if chatRequests != 0 || notificationRequests == 0 {
							return fmt.Errorf("empty round scope violated: chats=%d notifications=%d", chatRequests, notificationRequests)
						}
						return nil
					}
					if _, err := database.GetMessageBySeq(ctx, "si_u_v", int64(expectedReads)); err != nil {
						return fmt.Errorf("latest message missing before publish: %w", err)
					}
					return nil
				},
			})
			if len(progress) < 3 {
				t.Fatalf("missing intermediate progress: %v", progress)
			}
			if err == nil && progress[len(progress)-1] != 99 {
				t.Fatalf("successful round not projected: %v", progress)
			}
			if err != nil && progress[len(progress)-1] >= 99 {
				t.Fatalf("failed round reported complete: %v", progress)
			}
			checkpoint, readErr := database.GetConversationSyncV2(ctx)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if movingParents {
				if err == nil || syncv2.RecoveryFor(err) != syncv2.Retry || published || checkpoint.Stage == "ready" || metadataReads != 3 {
					t.Fatalf("catchup was unbounded or prematurely ready: %v %+v reads=%d", err, checkpoint, metadataReads)
				}
				return
			}
			if fail {
				if err == nil || published || checkpoint.Stage == "ready" {
					t.Fatalf("failure became ready: %v %+v", err, checkpoint)
				}
				return
			}
			if err != nil || !result.Enabled || !published || metadataReads != expectedReads || checkpoint.Stage != "ready" {
				t.Fatalf("round not ready: %v %+v published=%v reads=%d", err, result, published, metadataReads)
			}
		})
	}
}
