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
