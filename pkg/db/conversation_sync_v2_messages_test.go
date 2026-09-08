//go:build !js

package db

import (
	"context"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func TestSyncV2MessageProgressRetainsHolesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	d, err := NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	progress := &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", ParentID: "si_u_v", StateToken: "token", MinSeq: 1, ScannedSeq: 3, PendingSeqs: []int64{2}}
	messages := []*model_struct.LocalChatLog{{ClientMsgID: "m1", Seq: 1, Status: 2}, {ClientMsgID: "m3", Seq: 3, Status: 2}}
	if err = d.CommitConversationSyncV2Messages(ctx, 0, progress, messages); err != nil {
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
	got, err := d.GetConversationSyncV2Stream(ctx, "si_u_v")
	if err != nil {
		t.Fatal(err)
	}
	if got.ScannedSeq != 3 || len(got.PendingSeqs) != 1 || got.PendingSeqs[0] != 2 {
		t.Fatalf("hole lost at restart: %+v", got)
	}
	if _, err = d.GetMessageBySeq(ctx, "si_u_v", 2); err == nil {
		t.Fatal("missing message was turned into a stored tombstone")
	}
	progress.PendingSeqs = nil
	progress.ScannedSeq = 2
	if err = d.CommitConversationSyncV2Messages(ctx, 0, progress, []*model_struct.LocalChatLog{{ClientMsgID: "m2", Seq: 2, Status: 2}}); err != nil {
		t.Fatal(err)
	}
	got, err = d.GetConversationSyncV2Stream(ctx, "si_u_v")
	if err != nil || len(got.PendingSeqs) != 0 || got.ScannedSeq != 3 {
		t.Fatalf("retry failed to resolve hole: %+v %v", got, err)
	}
}

func TestSyncV2InvalidationRejectsInFlightMessages(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	before, err := d.GetConversationSyncV2Stream(ctx, "si_u_v")
	if err != nil {
		t.Fatal(err)
	}
	if err = d.InvalidateConversationSyncV2Stream(ctx, "si_u_v"); err != nil {
		t.Fatal(err)
	}
	progress := &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", ParentID: "si_u_v", MinSeq: 1, ScannedSeq: 1}
	if err = d.CommitConversationSyncV2Messages(ctx, before.Epoch, progress, []*model_struct.LocalChatLog{{ClientMsgID: "stale", Seq: 1}}); err == nil {
		t.Fatal("in-flight data resurrected after invalidation")
	}
	if _, err = d.GetMessage(ctx, "si_u_v", "stale"); err == nil {
		t.Fatal("stale message reached database")
	}
	current, err := d.GetConversationSyncV2Stream(ctx, "si_u_v")
	if err != nil || current.Epoch != 1 || current.ScannedSeq != 0 {
		t.Fatalf("invalidated progress overwritten: %+v %v", current, err)
	}
}

func TestSyncV2MessageCommitUpdatesLocalSendWithoutLosingLocalFields(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.BatchInsertMessageList(ctx, "si_u_v", []*model_struct.LocalChatLog{{ClientMsgID: "same", Seq: 0, Status: 1, IsRead: true, LocalEx: "local"}}); err != nil {
		t.Fatal(err)
	}
	progress := &model_struct.LocalConversationSyncV2Stream{ConversationID: "si_u_v", ParentID: "si_u_v", MinSeq: 1, ScannedSeq: 10}
	if err = d.CommitConversationSyncV2Messages(ctx, 0, progress, []*model_struct.LocalChatLog{{ClientMsgID: "same", ServerMsgID: "server", Seq: 10, Status: 2, SendTime: 123}}); err != nil {
		t.Fatal(err)
	}
	message, err := d.GetMessage(ctx, "si_u_v", "same")
	if err != nil {
		t.Fatal(err)
	}
	if message.Seq != 10 || message.ServerMsgID != "server" || message.Status != 2 || !message.IsRead || message.LocalEx != "local" {
		t.Fatalf("local send not reconciled safely: %+v", message)
	}
}

func TestSyncV2NewBoundaryInvalidatesOlderNetworkResponse(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	old := &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", StateToken: "old", MinSeq: 1, ScannedSeq: 1}
	if err = d.CommitConversationSyncV2Messages(ctx, 0, old, nil); err != nil {
		t.Fatal(err)
	}
	next := *old
	next.StateToken = "new"
	next.MinSeq = 10
	accepted, err := d.AcceptConversationSyncV2State(ctx, 0, &next)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Epoch != 1 {
		t.Fatalf("boundary change did not invalidate reads: %+v", accepted)
	}
	if err = d.CommitConversationSyncV2Messages(ctx, 0, old, []*model_struct.LocalChatLog{{ClientMsgID: "late", Seq: 1}}); err == nil {
		t.Fatal("old response accepted after newer boundary")
	}
	if _, err = d.AcceptConversationSyncV2State(ctx, 0, old); err == nil {
		t.Fatal("late state query rolled boundary back")
	}
}

func TestSyncV2NotificationProgressKeepsLegacyCursorBeforeHole(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	progress := &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_g", ParentID: "sg_g", Notifications: true, StateToken: "t", MinSeq: 1, ScannedSeq: 3, PendingSeqs: []int64{2}}
	if err = d.CommitConversationSyncV2Notifications(ctx, 0, progress, []int64{1, 3}); err != nil {
		t.Fatal(err)
	}
	legacy, err := d.GetNotificationAllSeqs(ctx)
	if err != nil || len(legacy) != 1 || legacy[0].Seq != 1 {
		t.Fatalf("legacy cursor skips unresolved notification: %+v %v", legacy, err)
	}
	progress.ScannedSeq = 2
	progress.PendingSeqs = nil
	if err = d.CommitConversationSyncV2Notifications(ctx, 0, progress, []int64{2}); err != nil {
		t.Fatal(err)
	}
	legacy, err = d.GetNotificationAllSeqs(ctx)
	if err != nil || legacy[0].Seq != 3 {
		t.Fatalf("resolved notification cursor did not advance: %+v %v", legacy, err)
	}
}

func TestSyncV2ProgressWriteFailureRollsBackMessages(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	if err = d.conn.Exec(`CREATE TRIGGER fail_v2_progress BEFORE INSERT ON local_conversation_sync_v2_streams BEGIN SELECT RAISE(ABORT, 'test progress write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	progress := &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", MinSeq: 1, ScannedSeq: 1}
	if err = d.CommitConversationSyncV2Messages(ctx, 0, progress, []*model_struct.LocalChatLog{{ClientMsgID: "m", Seq: 1}}); err == nil {
		t.Fatal("injected database failure hidden")
	}
	if _, err = d.GetMessage(ctx, "c", "m"); err == nil {
		t.Fatal("message committed without its progress")
	}
}
