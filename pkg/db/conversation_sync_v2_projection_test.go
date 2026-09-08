//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
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
	if err = d.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	projection.LatestMsg = "stale"
	if err = d.ApplyConversationSyncV2Projection(ctx, checkpoint, map[string]uint64{"c": 0}, []*model_struct.LocalConversation{projection}); err == nil {
		t.Fatal("projection ignored invalidated stream")
	}
}
