//go:build !js

package db

import (
	"context"
	"os"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

func TestUpgradeDatabaseWrittenByPreV2SDK(t *testing.T) {
	dir := os.Getenv("SYNCV2_LEGACY_DATABASE_DIR")
	if dir == "" {
		t.Skip("requires pinned pre-V2 writer fixture")
	}
	ctx := context.Background()
	database, err := NewDataBase(ctx, "9007199254740993", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			database.Close(ctx)
		}
	}()
	check := func() {
		t.Helper()
		for _, id := range []string{"sg_selected", "sg_deferred"} {
			row, err := database.GetConversation(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if row.DraftText != "old SDK draft" || row.DraftTextTime != 88 || row.IsPinned != (id == "sg_selected") || row.LatestMsg != "old SDK local preview" {
				t.Fatalf("old local fields lost: %+v", row)
			}
			message, err := database.GetMessage(ctx, id, "old-message")
			if err != nil || message.Seq != 7 || message.Content != "old SDK history" {
				t.Fatalf("old history lost: %+v %v", message, err)
			}
		}
	}
	check()
	if !database.conn.Migrator().HasTable(&model_struct.LocalConversationSyncV2{}) {
		t.Fatal("V2 table not added on real old database")
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "9007199254740993", Stage: "details", PolicyVersion: "conversation-sync-v2.2", SelectedIDs: model_struct.StringArray{"sg_selected"}, VersionID: "fixture", Version: 1}
	if err = database.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	response := &pb.StateResponse{States: []*pb.State{{ConversationID: "sg_selected", Known: true, MinSeq: 1, MaxSeq: 7, IsPinned: true, Conversation: &conversation.Conversation{OwnerUserID: "9007199254740993", ConversationID: "sg_selected", ConversationType: 3, GroupID: "selected"}}}}
	if err = database.ApplyConversationSyncV2Details(ctx, checkpoint, response); err != nil {
		t.Fatal(err)
	}
	check()
	if err = database.Close(ctx); err != nil {
		t.Fatal(err)
	}
	database = nil
	database, err = NewDataBase(ctx, "9007199254740993", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	check()
	fresh, err := NewDataBase(ctx, "9007199254740993", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close(ctx)
	empty, err := fresh.GetAllConversationListDB(ctx)
	if err != nil || len(empty) != 0 {
		t.Fatalf("fresh installation inherited old drafts: %v %v", empty, err)
	}
	freshCheckpoint := &model_struct.LocalConversationSyncV2{UserID: "9007199254740993", Stage: "details", PolicyVersion: "conversation-sync-v2.2", SelectedIDs: model_struct.StringArray{"sg_selected"}, VersionID: "fixture", Version: 1}
	if err = fresh.SaveConversationSyncV2(ctx, freshCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err = fresh.ApplyConversationSyncV2Details(ctx, freshCheckpoint, response); err != nil {
		t.Fatal(err)
	}
	row, err := fresh.GetConversation(ctx, "sg_selected")
	if err != nil || row.DraftText != "" || row.DraftTextTime != 0 || !row.IsPinned {
		t.Fatalf("fresh installation fabricated draft or lost server pin: %+v %v", row, err)
	}
	t.Log("real pre-V2 SQLite migrated, selected/deferred history and drafts persisted, selected pin retained across reopen; fresh same-account installation received server pin without fabricated draft")
}
