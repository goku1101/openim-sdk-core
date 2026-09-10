//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	protocolgroup "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
)

type obsoleteGroupListener struct {
	open_im_sdk_callback.OnGroupListener
	deleted, dismissed int
}

func (l *obsoleteGroupListener) OnJoinedGroupDeleted(string) { l.deleted++ }
func (l *obsoleteGroupListener) OnGroupDismissed(string)     { l.dismissed++ }

func TestSyncV2ObsoleteGroupNotificationConsumesBusinessStateBeforeProgress(t *testing.T) {
	for _, kind := range []int32{constant.GroupInfoSetNotification} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			var fail atomic.Bool
			fail.Store(true)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/group/get_incremental_group_members_batch" {
					json.NewEncoder(w).Encode(map[string]any{"errCode": 1002, "errMsg": "NoPermissionError"})
					return
				}
				if r.URL.Path != "/group/get_incremental_join_groups" || fail.Load() {
					w.WriteHeader(503)
					return
				}
				json.NewEncoder(w).Encode(struct {
					ErrCode int                                        `json:"errCode"`
					Data    *protocolgroup.GetIncrementalJoinGroupResp `json:"data"`
				}{Data: &protocolgroup.GetIncrementalJoinGroupResp{VersionID: "v", Version: 2, Delete: []string{"g"}}})
			}))
			defer server.Close()
			config := &ccontext.GlobalConfig{UserID: "u"}
			config.ApiAddr = server.URL
			ctx, cancel := context.WithCancel(ccontext.WithOperationID(ccontext.WithInfo(context.Background(), config), "terminal-consume"))
			defer cancel()
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			if err = database.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: "g"}); err != nil {
				t.Fatal(err)
			}
			if err = database.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: model_struct.LocalGroup{}.TableName(), EntityID: "u", VersionID: "v", Version: 1, UIDList: []string{"g"}}); err != nil {
				t.Fatal(err)
			}
			if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_g", DraftText: "keep", IsPinned: true}); err != nil {
				t.Fatal(err)
			}
			chat, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "sg_g", ParentID: "sg_g", StateToken: "chat", MinSeq: 1})
			if err != nil {
				t.Fatal(err)
			}
			notification, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_g", ParentID: "sg_g", Notifications: true, StateToken: "terminal", MinSeq: 7, PermissionMaxSeq: 7})
			if err != nil {
				t.Fatal(err)
			}
			queue := make(chan common.Cmd2Value, 10)
			g := group.NewGroup("u", database, queue)
			listener := &obsoleteGroupListener{}
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return listener })
			c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, g, nil, nil)
			c.ConfigureConversationSyncV2(ctx)
			c.syncV2.mode.Store(syncV2Enabled)
			stopped := make(chan struct{})
			go func() { defer close(stopped); common.DoListener(ctx, c) }()
			defer func() { cancel(); <-stopped }()
			var detail any
			detail = &sdkws.GroupInfoSetTips{Group: &sdkws.GroupInfo{GroupID: "g"}, GroupMemberVersionID: "changed", GroupMemberVersion: 10}

			data, _ := json.Marshal(detail)
			content, _ := json.Marshal(map[string]string{"detail": string(data)})
			message := &sdkws.MsgData{ClientMsgID: "terminal-7", Seq: 7, ContentType: kind, Content: content}
			progress := *notification
			progress.ScannedSeq = 7
			if err = c.ConsumeConversationSyncV2(ctx, &progress, []*sdkws.MsgData{message}); err == nil {
				t.Fatal("group refresh failure was hidden")
			}
			current, err := database.GetConversationSyncV2Stream(ctx, "n_g")
			if err != nil || current.ScannedSeq != notification.ScannedSeq || current.Epoch != notification.Epoch {
				t.Fatalf("failed handler advanced notification: %+v %v", current, err)
			}
			fail.Store(false)
			if err = c.ConsumeConversationSyncV2(ctx, &progress, []*sdkws.MsgData{message}); err != nil {
				t.Fatal(err)
			}
			current, err = database.GetConversationSyncV2Stream(ctx, "n_g")
			if err != nil || current.ScannedSeq != 7 || len(current.PendingSeqs) != 0 || current.StateToken != "terminal" {
				t.Fatalf("notification not durably consumed: %+v %v", current, err)
			}
			if err = database.CommitConversationSyncV2Messages(ctx, chat.Epoch, chat, []*model_struct.LocalChatLog{{ClientMsgID: "late", Seq: 1}}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
				t.Fatalf("confirmed departure retained old chat permission: %v", err)
			}
			groups, err := database.GetJoinedGroupListDB(ctx)
			if err != nil || len(groups) != 0 || listener.deleted != 1 {
				t.Fatalf("joined group not removed: %d callbacks=%d err=%v", len(groups), listener.deleted, err)
			}
			row, err := database.GetConversation(ctx, "sg_g")
			if err != nil || row.DraftText != "keep" || !row.IsPinned {
				t.Fatalf("terminal event erased local conversation: %+v %v", row, err)
			}
		})
	}
}
