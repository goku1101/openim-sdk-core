//go:build !js

package conversation_msg

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/protocol/sdkws"
	"testing"
)

func TestSyncV2ConsumerAcknowledgesDurableMessages(t *testing.T) {
	reports := 0
	ctx := syncv2.WithStoredMessages(context.Background(), func(added int) {
		if added != 1 {
			t.Errorf("incorrect committed count: %d", added)
		}
		reports++
	})
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	state, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", StateToken: "t", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	state.ScannedSeq = 2
	state.PendingSeqs = []int64{2}
	queue := make(chan common.Cmd2Value, 1)
	conversation := &Conversation{db: database, conversationEventQueue: queue}
	done := make(chan error, 1)
	go func() {
		done <- conversation.ConsumeConversationSyncV2(ctx, state, []*sdkws.MsgData{{ClientMsgID: "m", Seq: 1, ContentType: constant.Text, Content: []byte(`{"content":"hello"}`)}})
	}()
	cmd := <-queue
	if _, err := database.GetMessageBySeq(ctx, state.ConversationID, 1); err == nil {
		t.Fatal("message written outside serial queue")
	}
	if reports != 0 {
		t.Fatal("reported before transaction")
	}
	conversation.Work(cmd)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if reports != 1 {
		t.Fatal("committed message not reported")
	}
	message, err := database.GetMessageBySeq(ctx, state.ConversationID, 1)
	if err != nil || message.Content != `{"content":"hello"}` || message.Status != constant.MsgStatusSendSuccess {
		t.Fatalf("bad persisted message: %+v %v", message, err)
	}
	progress, err := database.GetConversationSyncV2Stream(ctx, state.ConversationID)
	if err != nil || progress.ScannedSeq != 2 || len(progress.PendingSeqs) != 1 {
		t.Fatalf("progress not durable: %+v %v", progress, err)
	}
}

func TestSyncV2ConsumerDoesNotAcknowledgeFailedNotification(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	state, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_u_v", Notifications: true, StateToken: "t", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	state.ScannedSeq = 1
	conversation := &Conversation{db: database}
	err = conversation.consumeConversationSyncV2(ctx, state, []*sdkws.MsgData{{ClientMsgID: "notification", Seq: 1, ContentType: constant.ClearConversationNotification, Content: []byte("invalid")}})
	if err == nil {
		t.Fatal("notification failure hidden")
	}
	progress, err := database.GetConversationSyncV2Stream(ctx, state.ConversationID)
	if err != nil || progress.ScannedSeq != 0 {
		t.Fatalf("failed notification acknowledged: %+v %v", progress, err)
	}
}

func TestSyncV2QueuedMessagesCannotSurviveBoundaryInvalidation(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	state, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", StateToken: "old", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	state.ScannedSeq = 1
	queue := make(chan common.Cmd2Value, 1)
	conversation := &Conversation{db: database, conversationEventQueue: queue}
	done := make(chan error, 1)
	go func() {
		done <- conversation.ConsumeConversationSyncV2(ctx, state, []*sdkws.MsgData{{ClientMsgID: "late", Seq: 1}})
	}()
	cmd := <-queue
	if err := database.InvalidateConversationSyncV2Stream(ctx, state.ConversationID); err != nil {
		t.Fatal(err)
	}
	conversation.Work(cmd)
	if err := <-done; err == nil {
		t.Fatal("late queue item accepted after boundary invalidation")
	}
	if _, err := database.GetMessageBySeq(ctx, state.ConversationID, 1); err == nil {
		t.Fatal("late message resurrected")
	}
}
