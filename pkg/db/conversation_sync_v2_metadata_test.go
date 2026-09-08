//go:build !js

package db

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"testing"
)

func TestSyncV2MetadataCommitsDeletionDetailsAndVersionTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			ctx := context.Background()
			d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			before := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SnapshotID: "s", VersionID: "v", Version: 1, SelectedIDs: []string{"removed", "loaded"}}
			if err = d.SaveConversationSyncV2(ctx, before); err != nil {
				t.Fatal(err)
			}
			before.Stage = "messages"
			if err = d.SaveConversationSyncV2(ctx, before); err != nil {
				t.Fatal(err)
			}
			for _, id := range before.SelectedIDs {
				if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, DraftText: "draft"}); err != nil {
					t.Fatal(err)
				}
			}
			if err = d.BatchInsertMessageList(ctx, "removed", []*model_struct.LocalChatLog{{ClientMsgID: "history", Seq: 1}}); err != nil {
				t.Fatal(err)
			}
			next := *before
			next.Version = 2
			next.SelectedIDs = []string{"loaded", "pinned"}
			next.StateEpochs = map[string]uint64{"loaded": 0, "pinned": 0, "removed": 0}
			states := &pb.StateResponse{}
			for _, id := range next.SelectedIDs {
				states.States = append(states.States, &pb.State{ConversationID: id, Known: true, MinSeq: 5, MaxSeq: 10, UnreadCount: 2, IsPinned: id == "pinned", Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: id}})
			}
			if fail {
				if err = d.conn.Exec("CREATE TRIGGER reject_metadata BEFORE UPDATE ON local_conversation_sync_v2 BEGIN SELECT RAISE(ABORT, 'checkpoint rejected'); END").Error; err != nil {
					t.Fatal(err)
				}
			}
			err = d.ApplyConversationSyncV2Metadata(ctx, before, &next, states, []string{"removed"})
			checkpoint, readErr := d.GetConversationSyncV2(ctx)
			if readErr != nil {
				t.Fatal(readErr)
			}
			removed, removedErr := d.GetConversation(ctx, "removed")
			loaded, loadedErr := d.GetConversation(ctx, "loaded")
			if loadedErr != nil || loaded.DraftText != "draft" {
				t.Fatalf("draft lost: %+v %v", loaded, loadedErr)
			}
			if fail {
				if err == nil || checkpoint.Version != 1 || removedErr != nil || removed == nil || loaded.MinSeq != 0 {
					t.Fatalf("partial transaction: %v %+v %+v", err, checkpoint, loaded)
				}
			} else {
				if err != nil || checkpoint.Version != 2 || removedErr == nil || loaded.MinSeq != 5 {
					t.Fatalf("commit failed: %v %+v %+v", err, checkpoint, loaded)
				}
				stream, err := d.GetConversationSyncV2Stream(ctx, "removed")
				if err != nil || stream.Epoch != 1 {
					t.Fatalf("deletion failed to invalidate old requests: %+v %v", stream, err)
				}
			}
			if _, err = d.GetMessage(ctx, "removed", "history"); err != nil {
				t.Fatal("metadata delete erased message history")
			}
		})
	}
}

func TestSyncV2MetadataRejectsLateResponseWithoutServerVersionChange(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	before := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SnapshotID: "s", VersionID: "v", Version: 1, SelectedIDs: []string{"c"}}
	if err = d.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	before.Stage = "messages"
	if err = d.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	response := func(min int64) *pb.StateResponse {
		return &pb.StateResponse{States: []*pb.State{{ConversationID: "c", Known: true, MinSeq: min, MaxSeq: 50, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "c"}}}}
	}
	next := *before
	if err = d.ApplyConversationSyncV2Metadata(ctx, before, &next, response(20), nil); err != nil {
		t.Fatal(err)
	}
	if err = d.ApplyConversationSyncV2Metadata(ctx, before, &next, response(5), nil); err == nil {
		t.Fatal("late read rolled back bounds without a metadata version change")
	}
	got, err := d.GetConversation(ctx, "c")
	if err != nil || got.MinSeq != 20 {
		t.Fatalf("new boundary lost: %+v %v", got, err)
	}
}
