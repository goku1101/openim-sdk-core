package db

import (
	"context"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
)

func TestGetLatestValidateServerMessage(t *testing.T) {
	ctx := context.Background()
	database, err := NewDataBase(ctx, "9007199254740993", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	const conversationID = "sg_test_history"
	if err := database.initChatLog(ctx, conversationID); err != nil {
		t.Fatal(err)
	}
	messages := []*model_struct.LocalChatLog{
		{ClientMsgID: "server-before", Seq: 1, SendTime: 100},
		{ClientMsgID: "local-only", Seq: 0, SendTime: 200},
		{ClientMsgID: "server-after", Seq: 3, SendTime: 300},
		{ClientMsgID: "server-last", Seq: 4, SendTime: 400},
	}
	if err := database.conn.Table(utils.GetTableName(conversationID)).Create(messages).Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		start   int64
		reverse bool
		want    string
	}{
		{"reverse skips local messages", 250, true, "server-before"},
		{"forward nearest server message", 250, false, "server-after"},
		{"reverse excludes boundary", 300, true, "server-before"},
		{"forward excludes boundary", 300, false, "server-last"},
		{"before first server message", 100, true, ""},
		{"after last server message", 400, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, err := database.GetLatestValidServerMessage(ctx, conversationID, tc.start, tc.reverse)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if message != nil {
					t.Fatalf("expected no message, got %s", message.ClientMsgID)
				}
				return
			}
			if message == nil || message.ClientMsgID != tc.want {
				t.Fatalf("message = %+v, want %s", message, tc.want)
			}
		})
	}
}

func TestCleanDuplicateInvalidMessages(t *testing.T) {
	ctx := context.Background()
	conn, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	d := &DataBase{
		conn:         conn,
		tableChecker: NewTableChecker(nil),
	}
	conversationID := "test_conversation"
	if err := d.initChatLog(ctx, conversationID); err != nil {
		t.Fatalf("init chat log: %v", err)
	}

	tableName := utils.GetTableName(conversationID)
	messages := []*model_struct.LocalChatLog{
		{ClientMsgID: "valid-50", Seq: 50, SendTime: 100, Status: constant.MsgStatusSendSuccess},
		{ClientMsgID: "deleted-50", Seq: 50, SendTime: 101, Status: constant.MsgStatusHasDeleted},
		{ClientMsgID: "zero-send-time", Seq: 51, SendTime: 0, Status: constant.MsgStatusHasDeleted},
		{ClientMsgID: "deleted-52-a", Seq: 52, SendTime: 200, Status: constant.MsgStatusHasDeleted},
		{ClientMsgID: "deleted-52-b", Seq: 52, SendTime: 201, Status: constant.MsgStatusHasDeleted},
		{ClientMsgID: "filtered-53", Seq: 53, SendTime: 300, Status: constant.MsgStatusFiltered},
		{ClientMsgID: "local-seq-zero", Seq: 0, SendTime: 0, Status: constant.MsgStatusSending},
	}
	if err := conn.Table(tableName).Create(messages).Error; err != nil {
		t.Fatalf("insert messages: %v", err)
	}

	if err := d.CleanDuplicateInvalidMessages(ctx, conversationID); err != nil {
		t.Fatalf("clean duplicate invalid messages: %v", err)
	}

	var got []*model_struct.LocalChatLog
	if err := conn.Table(tableName).Order("client_msg_id ASC").Find(&got).Error; err != nil {
		t.Fatalf("query messages: %v", err)
	}

	gotByID := make(map[string]*model_struct.LocalChatLog, len(got))
	for _, message := range got {
		gotByID[message.ClientMsgID] = message
	}

	wantIDs := []string{"valid-50", "deleted-52-a", "filtered-53", "local-seq-zero"}
	if len(gotByID) != len(wantIDs) {
		t.Fatalf("remaining message count = %d, want %d; messages = %#v", len(gotByID), len(wantIDs), gotByID)
	}
	for _, clientMsgID := range wantIDs {
		if gotByID[clientMsgID] == nil {
			t.Fatalf("remaining messages missing %s; messages = %#v", clientMsgID, gotByID)
		}
	}
	for _, clientMsgID := range []string{"deleted-50", "zero-send-time", "deleted-52-b"} {
		if gotByID[clientMsgID] != nil {
			t.Fatalf("message %s should have been removed", clientMsgID)
		}
	}
}
