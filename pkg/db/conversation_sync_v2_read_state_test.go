//go:build !js

package db

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"gorm.io/gorm"
	"testing"
)

func TestSyncV2ReadStatePreservesSelectionAndRejectsStaleOrDeletedTargets(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", SelectedIDs: []string{"other"}, VersionID: "v", Version: 4}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "deferred", UnreadCount: 8, DraftText: "keep draft"}); err != nil {
		t.Fatal(err)
	}
	state := &pb.State{ConversationID: "deferred", Known: true, StateToken: "t", MinSeq: 5, MaxSeq: 10, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "deferred"}}
	if err = d.RefreshConversationSyncV2ReadState(ctx, cp, 0, state); err != nil {
		t.Fatal(err)
	}
	row, err := d.GetConversation(ctx, "deferred")
	if err != nil || row.UnreadCount != 2 || row.DraftText != "keep draft" {
		t.Fatalf("bad read projection: %+v %v", row, err)
	}
	cp, err = d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.SelectedIDs) != 1 || cp.SelectedIDs[0] != "other" || len(cp.NotificationIDs) != 0 || cp.Version != 4 || cp.Stage != "ready" {
		t.Fatalf("read promoted selection or advanced metadata: %+v", cp)
	}
	epochs, err := d.GetConversationSyncV2Epochs(ctx, []string{"deferred"})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.InvalidateConversationSyncV2Stream(ctx, "deferred"); err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshConversationSyncV2ReadState(ctx, cp, epochs["deferred"], state); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("accepted invalidated state: %v", err)
	}
	epochs, err = d.GetConversationSyncV2Epochs(ctx, []string{"deferred"})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.conn.Where("conversation_id = ?", "deferred").Delete(&model_struct.LocalConversation{}).Error; err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshConversationSyncV2ReadState(ctx, cp, epochs["deferred"], state); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetConversation(ctx, "deferred"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("read recreated deleted conversation: %v", err)
	}
}
