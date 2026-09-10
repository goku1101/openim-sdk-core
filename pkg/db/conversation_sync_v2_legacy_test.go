//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"testing"
)

func TestSyncV2DowngradeResetsOnlyConversationMetadataOnce(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.SaveConversationSyncV2(ctx, &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot"}); err != nil {
		t.Fatal(err)
	}
	table := model_struct.LocalConversation{}.TableName()
	for _, name := range []string{table, "friends"} {
		if err = d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: name, EntityID: "u", VersionID: "old", Version: 2}); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", DraftText: "draft"}); err != nil {
		t.Fatal(err)
	}
	stream, err := d.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", StateToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.PrepareConversationSyncV2Legacy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetVersionSync(ctx, table, "u"); err == nil {
		t.Fatal("filtered metadata cursor reused by legacy")
	}
	if _, err = d.GetVersionSync(ctx, "friends", "u"); err != nil {
		t.Fatal("unrelated version cursor reset")
	}
	got, err := d.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || got.Epoch != stream.Epoch+1 || got.StateToken != "" {
		t.Fatalf("V2 readers survived downgrade: %+v %v", got, err)
	}
	local, err := d.GetConversation(ctx, "c")
	if err != nil || local.DraftText != "draft" {
		t.Fatal("downgrade deleted local draft")
	}
	if err = d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: table, EntityID: "u", VersionID: "new", Version: 4}); err != nil {
		t.Fatal(err)
	}
	if err = d.PrepareConversationSyncV2Legacy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = d.GetVersionSync(ctx, table, "u"); err != nil {
		t.Fatal("repeated unsupported probe discarded new legacy progress")
	}
}

func TestSyncV2ReentryPreservesLegacyVisibility(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "old", Stage: "snapshot", SelectedIDs: []string{"hidden", "reopened"}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	for _, id := range cp.SelectedIDs {
		if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, LatestMsg: "saved", LatestMsgSendTime: 80}); err != nil {
			t.Fatal(err)
		}
		if err = d.HideConversationSyncV2(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.PrepareConversationSyncV2Legacy(ctx); err != nil {
		t.Fatal(err)
	}
	// The legacy message path restores an entry without knowing V2's marker.
	if err = d.UpdateColumnsConversation(ctx, "reopened", map[string]interface{}{"latest_msg": "legacy new message", "latest_msg_send_time": 90}); err != nil {
		t.Fatal(err)
	}
	cp.Generation = "new"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	for _, id := range cp.SelectedIDs {
		visibility, err := d.GetConversationSyncV2Visibility(ctx, id)
		if err != nil || visibility.Hidden != (id == "hidden") {
			t.Fatalf("legacy visibility lost for %s: %+v %v", id, visibility, err)
		}
	}
	// Starting another V2 snapshot must not undo a new local hide.
	if err = d.HideConversationSyncV2(ctx, "reopened"); err != nil {
		t.Fatal(err)
	}
	cp.Generation = "newer"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	visibility, err := d.GetConversationSyncV2Visibility(ctx, "reopened")
	if err != nil || !visibility.Hidden {
		t.Fatalf("V2 restart lost hidden intent: %+v %v", visibility, err)
	}
}
