//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

func TestSyncV2SelfReadDoesNotRequireDownloadedMessage(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	c := &Conversation{db: d, loginUserID: "u", syncV2: &conversationSyncV2Runtime{store: d}}
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "syncing"})
	tips := &sdkws.MarkAsReadTips{ConversationID: "si_u_v", MarkAsReadUserID: "u", HasReadSeq: 7, Seqs: []int64{7}}
	body, _ := json.Marshal(tips)
	content, _ := json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	msg := &sdkws.MsgData{Content: content}
	// A filtered-out conversation must not be recreated by a read notification.
	if err = c.doReadDrawing(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetConversation(ctx, tips.ConversationID); err == nil {
		t.Fatal("activated unloaded conversation")
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: tips.ConversationID, ConversationType: constant.SingleChatType, UnreadCount: 3}); err != nil {
		t.Fatal(err)
	}
	if err = d.BatchInsertMessageList(ctx, tips.ConversationID, []*model_struct.LocalChatLog{{ClientMsgID: "local", Seq: 6, SendID: "v"}}); err != nil {
		t.Fatal(err)
	}
	tips.Seqs = []int64{6, 7}
	body, _ = json.Marshal(tips)
	msg.Content, _ = json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	for i := 0; i < 2; i++ {
		if err = c.doReadDrawing(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	row, err := d.GetMessageBySeq(ctx, tips.ConversationID, 6)
	if err != nil || !row.IsRead {
		t.Fatalf("local message not read: %+v %v", row, err)
	}
	conv, err := d.GetConversation(ctx, tips.ConversationID)
	if err != nil || conv.UnreadCount != 3 {
		t.Fatalf("notification replaced authoritative unread: %+v %v", conv, err)
	}
	if _, err = d.GetMessageBySeq(ctx, tips.ConversationID, 7); err == nil {
		t.Fatal("invented missing message")
	}
	d.Close(ctx)
	if err = c.doReadDrawing(ctx, msg); err == nil {
		t.Fatal("database failure hidden")
	}
}
