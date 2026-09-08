//go:build !js

package db

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/version"
)

func TestSyncV2CheckpointPersistsAndClearsZeroValues(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	userID := "9007199254740993"
	d, err := NewDataBase(ctx, userID, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: userID, PolicyVersion: "conversation-sync-v2.2", Stage: "details", SelectedIDs: model_struct.StringArray{"9007199254740995"}, NotificationIDs: model_struct.StringArray{"departed"}, IncludeSelf: true, VersionID: "v", Version: 10}
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = NewDataBase(ctx, userID, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	got, err := d.GetConversationSyncV2(ctx)
	if err != nil || got.UserID != userID || got.SelectedIDs[0] != "9007199254740995" || got.Version != 10 {
		t.Fatalf("checkpoint lost: %+v %v", got, err)
	}
	checkpoint.SelectedIDs = nil
	checkpoint.NotificationIDs = nil
	checkpoint.Version = 0
	checkpoint.VersionID = ""
	checkpoint.IncludeSelf = false
	checkpoint.Stage = "snapshot"
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	got, err = d.GetConversationSyncV2(ctx)
	if err != nil || len(got.SelectedIDs) != 0 || len(got.NotificationIDs) != 0 || got.IncludeSelf || got.Version != 0 || got.VersionID != "" {
		t.Fatalf("zero values not persisted: %+v %v", got, err)
	}
	checkpoint.UserID = "another-user"
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err == nil {
		t.Fatal("cross-user checkpoint accepted")
	}
}

func TestSyncV2TableMigratesWithoutSDKVersionChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	d, err := NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.conn.Migrator().DropTable(&model_struct.LocalConversationSyncV2{}); err != nil {
		t.Fatal(err)
	}
	if err = d.SetAppSDKVersion(ctx, &model_struct.LocalAppSDKVersion{Version: version.Version}); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	d, err = NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if !d.conn.Migrator().HasTable(&model_struct.LocalConversationSyncV2{}) {
		t.Fatal("existing installation did not migrate")
	}
	if err = d.SaveConversationSyncV2(ctx, &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "snapshot"}); err != nil {
		t.Fatal(err)
	}
}
