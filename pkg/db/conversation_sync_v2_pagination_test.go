//go:build !js

package db

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"testing"
)

func TestSyncV2MetadataPagesBeforeMessageHistory(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "details"}
	response := &pb.StateResponse{}
	for i := 0; i < 120; i++ {
		id := fmt.Sprintf("si_%03d_u", i)
		cp.SelectedIDs = append(cp.SelectedIDs, id)
		response.States = append(response.States, &pb.State{ConversationID: id, Known: true, MinSeq: 1, MaxSeq: 2, LastMessageTime: int64(1000 + i), Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: id, ConversationType: 1, UserID: fmt.Sprintf("%03d", i)}})
	}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = d.ApplyConversationSyncV2Details(ctx, cp, response); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 50, 100} {
		rows, err := d.GetConversationListSplitDB(ctx, offset, 50)
		want := min(50, 120-offset)
		if err != nil || len(rows) != want {
			t.Fatalf("offset %d: got %d, want %d: %v", offset, len(rows), want, err)
		}
		for i, row := range rows {
			if row.LatestMsg != "" || row.LatestMsgSendTime != int64(1119-offset-i) {
				t.Fatalf("unexpected metadata projection: %+v", row)
			}
		}
	}
	persisted, err := d.GetConversationSyncV2(ctx)
	if err != nil || persisted.Stage != "messages" {
		t.Fatalf("metadata marked fully ready: %+v %v", persisted, err)
	}
}

func TestSyncV2MetadataPagingPreservesPreviewAndHiddenRows(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "details", SelectedIDs: []string{"visible", "hidden", "empty", "unknown"}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	for _, row := range []*model_struct.LocalConversation{{ConversationID: "visible", LatestMsg: "new local preview", LatestMsgSendTime: 9000}, {ConversationID: "hidden", LatestMsgSendTime: 1000}} {
		if err = d.InsertConversation(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.HideConversationSyncV2(ctx, "hidden"); err != nil {
		t.Fatal(err)
	}
	// Hiding changes the stream epoch; capture the current one for this metadata round.
	cp, err = d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cp.Stage = "details"
	cp.StateEpochs, err = d.GetConversationSyncV2Epochs(ctx, cp.SelectedIDs)
	if err != nil {
		t.Fatal(err)
	}
	response := &pb.StateResponse{}
	for _, id := range cp.SelectedIDs {
		state := &pb.State{ConversationID: id, Known: id != "unknown", MaxSeq: 2, LastMessageTime: 1000, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: id}}
		if id == "empty" {
			state.MaxSeq = 0
		}
		response.States = append(response.States, state)
	}
	if err = d.ApplyConversationSyncV2Details(ctx, cp, response); err != nil {
		t.Fatal(err)
	}
	rows, err := d.GetConversationListSplitDB(ctx, 0, 50)
	if err != nil || len(rows) != 1 || rows[0].ConversationID != "visible" || rows[0].LatestMsg != "new local preview" || rows[0].LatestMsgSendTime != 9000 {
		t.Fatalf("visibility or local preview overwritten: %+v %v", rows, err)
	}
}
