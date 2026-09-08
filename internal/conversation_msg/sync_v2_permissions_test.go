//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSyncV2QuitInvalidatesOnlyChatAfterServerSuccess(t *testing.T) {
	var reject atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/group/quit_group" && !reject.Load() {
			fmt.Fprint(w, `{"errCode":0,"data":{}}`)
			return
		}
		w.WriteHeader(500)
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), config), "permission-test")
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	chat, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "sg_g", ParentID: "sg_g", StateToken: "chat", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	notification, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_g", ParentID: "sg_g", Notifications: true, StateToken: "notification", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_g", DraftText: "keep"}); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	g := group.NewGroup("u", database, queue)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, g, nil, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.mode.Store(syncV2Enabled)
	// The server accepts quitting, then the subsequent group refresh fails.
	// The old chat request must already be invalidated despite that failure.
	if err = g.QuitGroup(ctx, "g"); err == nil {
		t.Fatal("group refresh unexpectedly succeeded")
	}
	after, err := database.GetConversationSyncV2Stream(ctx, "sg_g")
	if err != nil || after.Epoch <= chat.Epoch || after.StateToken != "" {
		t.Fatalf("quit retained old permission: %+v %v", after, err)
	}
	if err = database.CommitConversationSyncV2Messages(ctx, chat.Epoch, chat, []*model_struct.LocalChatLog{{ClientMsgID: "late", Seq: 1}}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("late chat response survived quit: %v", err)
	}
	now, err := database.GetConversationSyncV2Stream(ctx, "n_g")
	if err != nil || now.Epoch != notification.Epoch || now.StateToken != notification.StateToken {
		t.Fatalf("quit invalidated terminal notification delivery: %+v %v", now, err)
	}
	row, err := database.GetConversation(ctx, "sg_g")
	if err != nil || row.DraftText != "keep" {
		t.Fatalf("quit erased local conversation: %+v %v", row, err)
	}
	for _, legacy := range []bool{false, true} {
		reject.Store(!legacy)
		if legacy {
			c.syncV2.mode.Store(syncV2Legacy)
		}
		_ = g.QuitGroup(ctx, "g")
		current, err := database.GetConversationSyncV2Stream(ctx, "sg_g")
		if err != nil || current.Epoch != after.Epoch {
			t.Fatalf("failed API/legacy request changed V2 epoch: %+v %v", current, err)
		}
	}
	c.syncV2.mode.Store(syncV2Enabled)
	for _, event := range []struct {
		kind   int32
		detail any
	}{
		{constant.MemberQuitNotification, &sdkws.MemberQuitTips{Group: &sdkws.GroupInfo{GroupID: "g"}, QuitUser: &sdkws.GroupMemberFullInfo{UserID: "other"}, GroupMemberVersion: 1, GroupMemberVersionID: "v"}},
		{constant.MemberKickedNotification, &sdkws.MemberKickedTips{Group: &sdkws.GroupInfo{GroupID: "g"}, KickedUserList: []*sdkws.GroupMemberFullInfo{{UserID: "other"}}, GroupMemberVersion: 1, GroupMemberVersionID: "v"}},
	} {
		detail, _ := json.Marshal(event.detail)
		content, _ := json.Marshal(map[string]string{"detail": string(detail)})
		_ = g.DoNotification(ctx, &sdkws.MsgData{ContentType: event.kind, Content: content})
		current, err := database.GetConversationSyncV2Stream(ctx, "sg_g")
		if err != nil || current.Epoch != after.Epoch {
			t.Fatalf("another member's departure invalidated my chat: %+v %v", current, err)
		}
	}

}
