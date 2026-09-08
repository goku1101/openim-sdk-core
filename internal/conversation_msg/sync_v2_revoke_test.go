//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

func TestSyncV2RevokeUnloadedMessageAcknowledgesNotification(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	c := &Conversation{db: d, syncV2: &conversationSyncV2Runtime{}}
	c.syncV2.mode.Store(syncV2Enabled)
	state, err := d.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_u_v", Notifications: true, StateToken: "t", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	state.ScannedSeq = 1
	tips := &sdkws.RevokeMsgTips{ConversationID: "si_u_v", ClientMsgID: "old", Seq: 7, RevokerUserID: "v", SesstionType: constant.SingleChatType, RevokeTime: 10}
	body, _ := json.Marshal(tips)
	content, _ := json.Marshal(&sdkws.NotificationElem{Detail: string(body)})
	err = c.consumeConversationSyncV2(ctx, state, []*sdkws.MsgData{{ClientMsgID: "notice", Seq: 1, ContentType: constant.RevokeNotification, Content: content}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.GetConversationSyncV2Stream(ctx, "n_u_v")
	if err != nil || got.ScannedSeq != 1 {
		t.Fatalf("notification not acknowledged: %+v %v", got, err)
	}
	if _, err = d.GetMessageBySeq(ctx, "si_u_v", 7); err == nil {
		t.Fatal("revoke invented original message")
	}
	c.syncV2.mode.Store(syncV2Legacy)
	if err = c.revokeMessage(ctx, tips); err == nil {
		t.Fatal("legacy behavior changed")
	}
	c.syncV2.mode.Store(syncV2Enabled)
	if err = d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.revokeMessage(ctx, tips); err == nil {
		t.Fatal("database failure hidden")
	}
}

type syncV2RevokeListener struct {
	open_im_sdk_callback.OnAdvancedMsgListener
	events []sdk_struct.MessageRevoked
}

func (l *syncV2RevokeListener) OnNewRecvMessageRevoked(value string) {
	var event sdk_struct.MessageRevoked
	_ = json.Unmarshal([]byte(value), &event)
	l.events = append(l.events, event)
}

func TestSyncV2RevokeUnloadedMessageUpdatesExistingQuotes(t *testing.T) {
	ctx := context.Background()
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.InsertFriend(ctx, &model_struct.LocalFriend{OwnerUserID: "u", FriendUserID: "v", Nickname: "Peer"}); err != nil {
		t.Fatal(err)
	}
	c := &Conversation{db: d, relation: relation.NewFriend("u", d, nil, nil), syncV2: &conversationSyncV2Runtime{}}
	c.syncV2.mode.Store(syncV2Enabled)
	listener := &syncV2RevokeListener{}
	c.msgListener = func() open_im_sdk_callback.OnAdvancedMsgListener { return listener }
	source := &sdk_struct.MsgStruct{ClientMsgID: "old", Seq: 7, SendID: "v", SendTime: 5, SenderNickname: "Peer", ContentType: constant.Text, SessionType: constant.SingleChatType}
	if err = d.BatchInsertMessageList(ctx, "si_u_v", nil); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"q1", "q2"} {
		if err = d.InsertMessage(ctx, "si_u_v", &model_struct.LocalChatLog{ClientMsgID: id, Seq: int64(i + 8), ContentType: constant.Quote, Content: utils.StructToJsonString(sdk_struct.QuoteElem{Text: "quote", QuoteMessage: source})}); err != nil {
			t.Fatal(err)
		}
	}
	tips := &sdkws.RevokeMsgTips{ConversationID: "si_u_v", ClientMsgID: "old", Seq: 7, RevokerUserID: "v", SesstionType: constant.SingleChatType, RevokeTime: 10}
	if err = c.revokeMessage(ctx, tips); err != nil {
		t.Fatal(err)
	}
	if len(listener.events) != 1 || listener.events[0].ClientMsgID != "old" || listener.events[0].SourceMessageSendID != "v" {
		t.Fatalf("incorrect revoke event: %+v", listener.events)
	}
	for _, seq := range []int64{8, 9} {
		row, err := d.GetMessageBySeq(ctx, "si_u_v", seq)
		if err != nil {
			t.Fatal(err)
		}
		var quote sdk_struct.QuoteElem
		if err = json.Unmarshal([]byte(row.Content), &quote); err != nil {
			t.Fatal(err)
		}
		if quote.QuoteMessage.ContentType != constant.RevokeNotification {
			t.Fatal("quoted content not revoked")
		}
	}
	if _, err = d.GetMessageBySeq(ctx, "si_u_v", 7); err == nil {
		t.Fatal("created missing original")
	}
	// A later V2 history result already carries the server's revoked content.
	state, err := d.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", StateToken: "t", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	state.ScannedSeq = 7
	body := utils.StructToJsonString(sdk_struct.NotificationElem{Detail: utils.StructToJsonString(listener.events[0])})
	if err = c.consumeConversationSyncV2(ctx, state, []*sdkws.MsgData{{ClientMsgID: "old", Seq: 7, ContentType: constant.RevokeNotification, Content: []byte(body)}}); err != nil {
		t.Fatal(err)
	}
	row, err := d.GetMessageBySeq(ctx, "si_u_v", 7)
	if err != nil || row.ContentType != constant.RevokeNotification || row.Content != body {
		t.Fatalf("history revived revoked content: %+v %v", row, err)
	}
}
