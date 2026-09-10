//go:build !js

package conversation_msg

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"testing"
)

type hiddenSendListener struct {
	syncV2Listener
	changes int
}

func (l *hiddenSendListener) OnConversationChanged(string) { l.changes++ }

func TestSyncV2HiddenConversationIgnoresLateSendConfirmation(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"})
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	id := "si_peer_u"
	lc := &model_struct.LocalConversation{ConversationID: id, ConversationType: constant.SingleChatType, UserID: "peer", LatestMsgSendTime: 80}
	if err = database.InsertConversation(ctx, lc); err != nil {
		t.Fatal(err)
	}
	message := &sdk_struct.MsgStruct{ClientMsgID: "sent-before-hide", SendID: "u", RecvID: "peer", SessionType: constant.SingleChatType, SendTime: 80, Status: constant.MsgStatusSending}
	if err = database.BatchInsertMessageList(ctx, id, []*model_struct.LocalChatLog{MsgStructToLocalChatLog(message)}); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
	if !c.ConfigureConversationSyncV2(ctx) {
		t.Fatal("configure failed")
	}
	c.syncV2.mode.Store(syncV2Enabled)
	listener := &hiddenSendListener{}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })
	if err = c.HideConversation(ctx, id); err != nil {
		t.Fatal(err)
	}
	c.updateMsgStatusAndTriggerConversation(ctx, message.ClientMsgID, "server", 90, constant.MsgStatusSendSuccess, message, lc, false)
	select {
	case cmd := <-queue:
		c.doUpdateConversation(cmd)
	default:
		t.Fatal("missing confirmation command")
	}
	row, err := database.GetConversation(ctx, id)
	if err != nil || row.LatestMsgSendTime != 0 || row.LatestMsg != "" {
		t.Fatalf("late send confirmation resurfaced hidden conversation: %+v %v", row, err)
	}
	if listener.changes != 0 {
		t.Fatal("hidden confirmation emitted visible callback")
	}
	saved, err := database.GetMessage(ctx, id, message.ClientMsgID)
	if err != nil || saved.Status != constant.MsgStatusSendSuccess || saved.SendTime != 90 {
		t.Fatalf("hidden confirmation lost message status: %+v %v", saved, err)
	}
	// Exercise both real send implementations up to an explicitly offline
	// transport boundary, without opening the hidden conversation first.
	offline := errors.New("offline transport fixture")
	c.LongConnMgr.SetSyncRequestGuard(func(context.Context, int) error { return offline })
	for _, withoutOss := range []bool{false, true} {
		if err = c.HideConversation(ctx, id); err != nil {
			t.Fatal(err)
		}
		clientID := "new-with-oss"
		if withoutOss {
			clientID = "new-without-oss"
		}
		fresh := &sdk_struct.MsgStruct{ClientMsgID: clientID, CreateTime: 100, Status: constant.MsgStatusSending, ContentType: constant.Text, TextElem: &sdk_struct.TextElem{Content: "new message"}}
		if withoutOss {
			_, err = c.sendMessageNotOss(ctx, fresh, "peer", "", nil, false)
		} else {
			_, err = c.sendMessage(ctx, fresh, "peer", "", nil, false)
		}
		if !errors.Is(err, offline) {
			t.Fatalf("unexpected send boundary: %v", err)
		}
		for len(queue) > 0 {
			c.doUpdateConversation(<-queue)
		}
		visible, err := database.GetConversation(ctx, id)
		visibility, visibilityErr := database.GetConversationSyncV2Visibility(ctx, id)
		if err != nil || visibilityErr != nil || visibility.Hidden || visible.LatestMsgSendTime != 100 {
			t.Fatalf("explicit send did not restore entrance: row=%+v visibility=%+v err=%v/%v", visible, visibility, err, visibilityErr)
		}
	}

	// A hide after the explicit send started also suppresses the already queued
	// local preview, not only the subsequent failure/success confirmation.
	c.LongConnMgr.SetSyncRequestGuard(func(context.Context, int) error {
		if err := c.HideConversation(ctx, id); err != nil {
			t.Error(err)
		}
		return offline
	})
	changes := listener.changes
	fresh := &sdk_struct.MsgStruct{ClientMsgID: "hide-during-send", CreateTime: 110, Status: constant.MsgStatusSending, ContentType: constant.Text, TextElem: &sdk_struct.TextElem{Content: "new message"}}
	if _, err = c.sendMessage(ctx, fresh, "peer", "", nil, false); !errors.Is(err, offline) {
		t.Fatal(err)
	}
	for len(queue) > 0 {
		c.doUpdateConversation(<-queue)
	}
	hidden, err := database.GetConversation(ctx, id)
	if err != nil || hidden.LatestMsgSendTime != 0 || listener.changes != changes {
		t.Fatalf("queued send preview survived hide: %+v callbacks=%d/%d err=%v", hidden, listener.changes, changes, err)
	}

}
