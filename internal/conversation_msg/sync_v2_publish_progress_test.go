//go:build !js

package conversation_msg

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/internal/user"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
)

type projectionQueueProbe struct{ *Conversation }

func (p *projectionQueueProbe) Work(cmd common.Cmd2Value) {
	if cmd.Cmd == "projection-test-event" {
		cmd.Value.(func())()
		return
	}
	p.Conversation.Work(cmd)
}

func TestSyncV2ProjectionProgressFollowsDurableBatches(t *testing.T) {
	for _, invalidateLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalidate-last-%t", invalidateLast), func(t *testing.T) {
			ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"}))
			defer cancel()
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot"}
			for i := 0; i < 301; i++ {
				id := fmt.Sprintf("qa_%d", i)
				cp.SelectedIDs = append(cp.SelectedIDs, id)
				if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, ConversationType: constant.SingleChatType, UserID: "peer", LatestMsg: "old-preview", LatestMsgSendTime: 77, DraftText: "keep-draft", DraftTextTime: 88, MinSeq: 7, MaxSeq: 6}); err != nil {
					t.Fatal(err)
				}
			}
			if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "messages"
			if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			if err = database.InsertFriend(ctx, &model_struct.LocalFriend{OwnerUserID: "u", FriendUserID: "peer", Nickname: "Peer"}); err != nil {
				t.Fatal(err)
			}
			queue := make(chan common.Cmd2Value, 10)
			u := user.NewUser(database, "u", queue)
			c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, relation.NewFriend("u", database, u, queue), group.NewGroup("u", database, queue), u, nil)
			c.ConfigureConversationSyncV2(ctx)
			stopped := make(chan struct{})
			go func() { defer close(stopped); common.DoListener(ctx, &projectionQueueProbe{c}) }()
			defer func() { cancel(); <-stopped }()
			var batches []int
			var eventProcessed atomic.Bool
			err = c.publishConversationSyncV2WithProgress(ctx, cp, func(done, total int) {
				batches = append(batches, done)
				if total != 301 {
					t.Errorf("total=%d", total)
				}
				row, readErr := database.GetConversation(ctx, cp.SelectedIDs[done-1])
				if readErr != nil || row == nil || row.LatestMsg != "" || row.DraftText != "keep-draft" {
					t.Errorf("reported before durable projection: %+v %v", row, readErr)
				}
				if done == 300 {
					queue <- common.Cmd2Value{Cmd: "projection-test-event", Ctx: ctx, Value: func() {
						if invalidateLast {
							if e := database.InvalidateConversationSyncV2Stream(ctx, cp.SelectedIDs[300]); e != nil {
								t.Error(e)
							}
						}
						eventProcessed.Store(true)
					}}
				}
				if done == 301 && !eventProcessed.Load() {
					t.Error("projection blocked queued event until all batches completed")
				}
			})
			expected := []int{300, 301}
			if invalidateLast {
				expected = []int{300}
				if err == nil {
					t.Error("invalidated projection succeeded")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !invalidateLast {
				visible, listErr := database.GetConversationListSplitDB(ctx, 0, 500)
				if listErr != nil || len(visible) != 301 {
					t.Fatalf("cleared conversations with local drafts disappeared: visible=%d err=%v", len(visible), listErr)
				}
				for _, row := range visible {
					if row.LatestMsg != "" || row.LatestMsgSendTime != 77 || row.DraftText != "keep-draft" || row.DraftTextTime != 88 {
						t.Fatalf("local draft entry changed during initial projection: %+v", row)
					}
				}
			}
			if !reflect.DeepEqual(batches, expected) {
				t.Fatalf("reported uncommitted/missing batches: %v, want %v", batches, expected)
			}
		})
	}
}

func TestSyncV2ProjectionPreservesProfileOnTransportFailure(t *testing.T) {
	for _, unauthorized := range []bool{false, true} {
		t.Run(fmt.Sprintf("unauthorized-%t", unauthorized), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/user/get_users_info" {
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				if unauthorized {
					w.Write([]byte(`{"errCode":1501,"errMsg":"token expired"}`))
					return
				}
				w.Header().Set("Content-Length", "200")
				w.Write([]byte(`{`))
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "profile-failure-test"))
			defer cancel()
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			row := &model_struct.LocalConversation{ConversationID: "si_peer_u", ConversationType: constant.SingleChatType, UserID: "peer", ShowName: "Saved name", FaceURL: "saved-avatar", DraftText: "keep"}
			if err := database.InsertConversation(ctx, row); err != nil {
				t.Fatal(err)
			}
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SelectedIDs: []string{row.ConversationID}}
			if err := database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "messages"
			if err := database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			queue := make(chan common.Cmd2Value, 10)
			u := user.NewUser(database, "u", queue)
			c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, relation.NewFriend("u", database, u, queue), group.NewGroup("u", database, queue), u, nil)
			c.ConfigureConversationSyncV2(ctx)
			stopped := make(chan struct{})
			go func() { defer close(stopped); common.DoListener(ctx, &projectionQueueProbe{c}) }()
			defer func() { cancel(); <-stopped }()
			completed := 0
			err = c.publishConversationSyncV2WithProgress(ctx, cp, func(done, total int) { completed = done })
			if unauthorized {
				if err == nil || completed != 0 {
					t.Fatalf("authorization failure was swallowed: err=%v progress=%d", err, completed)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			saved, err := database.GetConversation(ctx, row.ConversationID)
			if err != nil || completed != 1 || saved.ShowName != "Saved name" || saved.FaceURL != "saved-avatar" || saved.DraftText != "keep" {
				t.Fatalf("projection lost profile/progress: %+v completed=%d err=%v", saved, completed, err)
			}
		})
	}
}
