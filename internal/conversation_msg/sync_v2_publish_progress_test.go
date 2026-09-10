//go:build !js

package conversation_msg

import (
	"context"
	"fmt"
	"reflect"
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
)

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
			go func() { defer close(stopped); common.DoListener(ctx, c) }()
			defer func() { cancel(); <-stopped }()
			var batches []int
			err = c.publishConversationSyncV2WithProgress(ctx, cp, func(done, total int) {
				batches = append(batches, done)
				if total != 301 {
					t.Errorf("total=%d", total)
				}
				row, readErr := database.GetConversation(ctx, cp.SelectedIDs[done-1])
				if readErr != nil || row == nil || row.LatestMsg != "" || row.DraftText != "keep-draft" {
					t.Errorf("reported before durable projection: %+v %v", row, readErr)
				}
				if invalidateLast && done == 300 {
					if e := database.InvalidateConversationSyncV2Stream(ctx, cp.SelectedIDs[300]); e != nil {
						t.Error(e)
					}
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
