//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

type syncV2LiveListener struct {
	open_im_sdk_callback.OnAdvancedMsgListener
	onMessage func(string)
}

func (l *syncV2LiveListener) OnRecvNewMessage(value string) { l.onMessage(value) }

type syncV2LiveBatchListener struct {
	open_im_sdk_callback.OnBatchMsgListener
	onMessages func(string)
}

func (l *syncV2LiveBatchListener) OnRecvNewMessages(value string) { l.onMessages(value) }

func TestSyncV2LiveCallbackFollowsCommitAndDoesNotRepeat(t *testing.T) {
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"}))
	defer cancel()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", UnreadCount: 7}); err != nil {
		t.Fatal(err)
	}
	stream, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", StateToken: "t", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
	count := 0
	listener := &syncV2LiveListener{onMessage: func(value string) {
		var message sdk_struct.MsgStruct
		if err := json.Unmarshal([]byte(value), &message); err != nil {
			t.Error(err)
		}
		row, err := database.GetMessage(ctx, "c", message.ClientMsgID)
		if err != nil || row.Seq != 1 {
			t.Errorf("callback before durable message: %+v %v", row, err)
		}
		count++
	}}
	c.msgListener = func() open_im_sdk_callback.OnAdvancedMsgListener { return listener }
	c.batchMsgListener = func() open_im_sdk_callback.OnBatchMsgListener { return nil }
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	message := &sdkws.MsgData{ClientMsgID: "m", Seq: 1, ContentType: constant.Text, Content: []byte(`{"content":"hello"}`)}
	stream.ScannedSeq = 1
	if err = c.consumeConversationSyncV2Live(ctx, stream, []*sdkws.MsgData{message}); err != nil {
		t.Fatal(err)
	}
	if err = c.consumeConversationSyncV2Live(ctx, stream, []*sdkws.MsgData{message}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate receive callbacks: %d", count)
	}
	conversation, err := database.GetConversation(ctx, "c")
	if err != nil || conversation.UnreadCount != 7 {
		t.Fatalf("live callback incremented authoritative unread: %+v %v", conversation, err)
	}

	if err = database.HideConversationSyncV2(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if err = c.consumeConversationSyncV2Live(ctx, stream, []*sdkws.MsgData{message}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("hidden stream accepted stale queued messages: %v", err)
	}
	current, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	stream, err = database.AcceptConversationSyncV2State(ctx, current.Epoch, &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", StateToken: "fresh", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	stream.ScannedSeq = 1
	if err = c.consumeConversationSyncV2Live(ctx, stream, []*sdkws.MsgData{message}); err != nil {
		t.Fatal(err)
	}
	hidden, err := database.GetConversationSyncV2Visibility(ctx, "c")
	if err != nil || !hidden.Hidden {
		t.Fatalf("existing message replay revealed conversation: %+v %v", hidden, err)
	}
	batchCount := 0
	batchListener := &syncV2LiveBatchListener{onMessages: func(value string) {
		var rows []*sdk_struct.MsgStruct
		if err := json.Unmarshal([]byte(value), &rows); err != nil {
			t.Error(err)
		}
		if len(rows) != 1 || rows[0].ClientMsgID != "batch" {
			t.Errorf("deleted/suppressed row reached callback: %+v", rows)
		}
		batchCount++
	}}
	c.batchMsgListener = func() open_im_sdk_callback.OnBatchMsgListener { return batchListener }
	stream.ScannedSeq = 4
	batch := []*sdkws.MsgData{
		{ClientMsgID: "batch", Seq: 2, ContentType: constant.Text, Content: []byte(`{"content":"batch"}`)},
		{ClientMsgID: "deleted", Seq: 3, Status: constant.MsgStatusHasDeleted},
		{ClientMsgID: "suppressed", Seq: 4, Options: map[string]bool{constant.IsConversationUpdate: false}},
	}
	for i := 0; i < 2; i++ {
		if err = c.consumeConversationSyncV2Live(ctx, stream, batch); err != nil {
			t.Fatal(err)
		}
	}
	if batchCount != 1 || count != 1 {
		t.Fatalf("wrong callback channel/count: batch=%d single=%d", batchCount, count)
	}
	revealed, err := database.GetConversationSyncV2Visibility(ctx, "c")
	if err != nil || revealed.Hidden {
		t.Fatalf("new live message did not reveal conversation: %+v %v", revealed, err)
	}
	if err = database.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	message.ClientMsgID = "late"
	if err = c.consumeConversationSyncV2Live(ctx, stream, []*sdkws.MsgData{message}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("stale live delivery accepted: %v", err)
	}
	if count != 1 {
		t.Fatal("stale response emitted callback")
	}
}
