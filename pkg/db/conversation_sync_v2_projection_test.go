//go:build !js

package db

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"gorm.io/gorm"
	"testing"
)

func TestSyncV2ProjectionRespectsVisibleBoundsAndPreservesDraft(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SelectedIDs: []string{"c"}}
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint.Stage = "messages"
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", MinSeq: 5, MaxSeq: 10, UnreadCount: 2, DraftText: "draft"}); err != nil {
		t.Fatal(err)
	}
	if err = d.BatchInsertMessageList(ctx, "c", []*model_struct.LocalChatLog{{ClientMsgID: "below", Seq: 2, SendTime: 999, Status: 2}, {ClientMsgID: "above", Seq: 11, SendTime: 999, Status: 2}, {ClientMsgID: "visible", Seq: 8, SendTime: 80, Status: 2}}); err != nil {
		t.Fatal(err)
	}
	messages, err := d.GetConversationSyncV2LatestMessage(ctx, "c", 5, 10)
	if err != nil || len(messages) != 1 || messages[0].ClientMsgID != "visible" {
		t.Fatalf("outside-bound message projected: %+v %v", messages, err)
	}
	projection := &model_struct.LocalConversation{ConversationID: "c", ShowName: "name", FaceURL: "face", LatestMsg: "visible-json", LatestMsgSendTime: 80}
	if err = d.ApplyConversationSyncV2Projection(ctx, checkpoint, map[string]uint64{"c": 0}, []*model_struct.LocalConversation{projection}); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetConversation(ctx, "c")
	if err != nil || got.DraftText != "draft" || got.UnreadCount != 2 || got.MinSeq != 5 || got.LatestMsg != "visible-json" {
		t.Fatalf("projection overwrote unrelated state: %+v %v", got, err)
	}
	writes := 0
	if err = d.conn.Callback().Update().Before("gorm:update").Register("test:projection-write", func(tx *gorm.DB) {
		if tx.Statement.Table == "local_conversations" {
			writes++
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err = d.ApplyConversationSyncV2Projection(ctx, checkpoint, map[string]uint64{"c": 0}, []*model_struct.LocalConversation{projection}); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("unchanged projection rewrote %d rows", writes)
	}
	if err = d.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	projection.LatestMsg = "stale"
	if err = d.ApplyConversationSyncV2Projection(ctx, checkpoint, map[string]uint64{"c": 0}, []*model_struct.LocalConversation{projection}); err == nil {
		t.Fatal("projection ignored invalidated stream")
	}
}

func TestBatchedLatestMessagesMatchVisibleSingleQueries(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	var conversations []*model_struct.LocalConversation
	expected := map[string]string{}
	for i := 0; i < 201; i++ {
		id := fmt.Sprintf("sg_%d", i)
		row := &model_struct.LocalConversation{ConversationID: id, MinSeq: 2, MaxSeq: 4}
		conversations = append(conversations, row)
		messages := []*model_struct.LocalChatLog{{ClientMsgID: "below", Seq: 1, SendTime: 999, Status: 2}, {ClientMsgID: "above", Seq: 5, SendTime: 999, Status: 2}, {ClientMsgID: "deleted", Seq: 4, SendTime: 999, Status: constant.MsgStatusHasDeleted}}
		if i%3 == 0 {
			messages = append(messages, &model_struct.LocalChatLog{ClientMsgID: "visible", Seq: 3, SendTime: 30, Status: 2})
		}
		if i%3 == 1 {
			messages = append(messages, &model_struct.LocalChatLog{ClientMsgID: "local", Seq: 0, SendTime: 40, Status: 1})
		}
		if err = d.BatchInsertMessageList(ctx, id, messages); err != nil {
			t.Fatal(err)
		}
		single, err := d.GetConversationSyncV2LatestMessage(ctx, id, 2, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(single) > 0 {
			expected[id] = single[0].ClientMsgID
		}
	}
	queries := 0
	d.conn.Callback().Row().Before("gorm:row").Register("test:projection-batches", func(*gorm.DB) { queries++ })
	rows, err := d.GetConversationSyncV2LatestMessages(ctx, conversations)
	if err != nil {
		t.Fatal(err)
	}
	if queries != 3 || len(rows) != len(expected) {
		t.Fatalf("not batched or missing rows: queries=%d rows=%d expected=%d", queries, len(rows), len(expected))
	}
	for id, want := range expected {
		if rows[id] == nil || rows[id].ClientMsgID != want {
			t.Fatal("batch changed visible message selection")
		}
	}
	// A later history commit must be visible on the next call, without a cache epoch shortcut.
	if err = d.BatchInsertMessageList(ctx, "sg_2", []*model_struct.LocalChatLog{{ClientMsgID: "late", Seq: 4, SendTime: 44, Status: 2}}); err != nil {
		t.Fatal(err)
	}
	rows, err = d.GetConversationSyncV2LatestMessages(ctx, conversations[2:3])
	if err != nil || rows["sg_2"] == nil || rows["sg_2"].ClientMsgID != "late" {
		t.Fatal("late history was hidden by projection reuse")
	}
}
