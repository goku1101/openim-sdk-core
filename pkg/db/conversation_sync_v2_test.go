//go:build !js

package db

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/version"
	pbConversation "github.com/openimsdk/protocol/conversation"
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

func TestSyncV2DetailsPreserveLocalDataAndUseEffectiveBounds(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	for _, id := range []string{"selected", "deferred"} {
		if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, DraftText: "local draft", DraftTextTime: 99, LatestMsg: "local history", LatestMsgSendTime: 88, ShowName: "local name", UnreadCount: 42, MinSeq: 1, MaxSeq: 100}); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "details", PolicyVersion: "conversation-sync-v2.2", SelectedIDs: model_struct.StringArray{"selected", "unknown", "unavailable"}, VersionID: "v", Version: 7}
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	resp := &pb.StateResponse{States: []*pb.State{
		{ConversationID: "selected", Known: true, MinSeq: 80, MaxSeq: 90, UnreadCount: 5, IsPinned: true, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "selected", MinSeq: 1, MaxSeq: 100, RecvMsgOpt: 2}},
		{ConversationID: "unknown", Known: false, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "unknown"}},
	}, UnavailableIDs: []string{"unavailable"}}
	if err = d.ApplyConversationSyncV2Details(ctx, checkpoint, resp); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetConversation(ctx, "selected")
	if err != nil {
		t.Fatal(err)
	}
	if got.DraftText != "local draft" || got.DraftTextTime != 99 || got.LatestMsg != "local history" || got.ShowName != "local name" || got.LatestMsgSendTime != 88 {
		t.Fatalf("local fields lost: %+v", got)
	}
	if got.MinSeq != 80 || got.MaxSeq != 90 || got.UnreadCount != 5 || !got.IsPinned || got.RecvMsgOpt != 2 {
		t.Fatalf("effective state not applied: %+v", got)
	}
	deferred, err := d.GetConversation(ctx, "deferred")
	if err != nil || deferred.UnreadCount != 42 {
		t.Fatalf("deferred local data changed: %v", err)
	}
	stored, err := d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Stage != "messages" || len(stored.PendingStateIDs) != 1 || stored.PendingStateIDs[0] != "unknown" || len(stored.UnavailableIDs) != 1 {
		t.Fatalf("pending state lost or premature finish: %+v", stored)
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

func TestSyncV2DetailsRejectStaleRound(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	old := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "details", SnapshotID: "old", SelectedIDs: model_struct.StringArray{"c"}}
	current := *old
	current.SnapshotID = "new"
	if err = d.SaveConversationSyncV2(ctx, &current); err != nil {
		t.Fatal(err)
	}
	resp := &pb.StateResponse{States: []*pb.State{{ConversationID: "c", Known: true, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "c"}}}}
	if err = d.ApplyConversationSyncV2Details(ctx, old, resp); err == nil {
		t.Fatal("stale details response accepted")
	}
	if _, err = d.GetConversation(ctx, "c"); err == nil {
		t.Fatal("stale response inserted conversation")
	}
	got, err := d.GetConversationSyncV2(ctx)
	if err != nil || got.SnapshotID != "new" || got.Stage != "details" {
		t.Fatalf("new checkpoint overwritten: %+v %v", got, err)
	}
}

func TestSyncV2DetailsFailureRollsBackDataAndStage(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "details", SnapshotID: "s", SelectedIDs: model_struct.StringArray{"valid", "invalid"}}
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	response := &pb.StateResponse{States: []*pb.State{
		{ConversationID: "valid", Known: true, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "valid"}},
		{ConversationID: "invalid", Known: true, Conversation: &pbConversation.Conversation{OwnerUserID: "other", ConversationID: "invalid"}},
	}}
	if err = d.ApplyConversationSyncV2Details(ctx, checkpoint, response); err == nil {
		t.Fatal("foreign owner accepted")
	}
	if _, err = d.GetConversation(ctx, "valid"); err == nil {
		t.Fatal("partial details transaction committed")
	}
	stored, err := d.GetConversationSyncV2(ctx)
	if err != nil || stored.Stage != "details" {
		t.Fatalf("stage advanced after rollback: %+v %v", stored, err)
	}
}

func TestSyncV2LateSnapshotCannotReplaceNewRound(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	old := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "snapshot", Generation: "old"}
	if err = d.SaveConversationSyncV2(ctx, old); err != nil {
		t.Fatal(err)
	}
	current := *old
	current.Generation = "new"
	if err = d.SaveConversationSyncV2(ctx, &current); err != nil {
		t.Fatal(err)
	}
	old.Stage = "details"
	old.SnapshotID = "late-response"
	if err = d.SaveConversationSyncV2(ctx, old); err == nil {
		t.Fatal("late snapshot replaced newer round")
	}
	stored, err := d.GetConversationSyncV2(ctx)
	if err != nil || stored.Generation != "new" || stored.Stage != "snapshot" {
		t.Fatalf("new round overwritten: %+v %v", stored, err)
	}
}

func TestSyncV2DetailsCannotRestoreStateAfterClear(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "details", SnapshotID: "s", SelectedIDs: model_struct.StringArray{"c"}}
	if err = d.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = d.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	resp := &pb.StateResponse{States: []*pb.State{{ConversationID: "c", Known: true, UnreadCount: 9, Conversation: &pbConversation.Conversation{OwnerUserID: "u", ConversationID: "c"}}}}
	if err = d.ApplyConversationSyncV2Details(ctx, checkpoint, resp); err == nil {
		t.Fatal("stale details restored after clear")
	}
}
