//go:build !js

package db

import (
	"context"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func TestSyncV2HiddenConversationSurvivesBackgroundProjection(t *testing.T) {
	for _, captureBeforeHide := range []bool{true, false} {
		name := "projection_started_after_hide"
		if captureBeforeHide {
			name = "projection_in_flight_before_hide"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			d, err := NewDataBase(ctx, "u", directory, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close(ctx) }()
			id := "sg_9007199254740995777"
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SelectedIDs: []string{id}}
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "messages"
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, LatestMsg: "existing", LatestMsgSendTime: 80, MinSeq: 1, MaxSeq: 2}); err != nil {
				t.Fatal(err)
			}
			if err = d.BatchInsertMessageList(ctx, id, []*model_struct.LocalChatLog{{ClientMsgID: "kept", Seq: 2, SendTime: 80, Status: 2}}); err != nil {
				t.Fatal(err)
			}
			epochs, err := d.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.HideConversationSyncV2(ctx, id); err != nil {
				t.Fatal(err)
			}
			if !captureBeforeHide {
				epochs, err = d.GetConversationSyncV2Epochs(ctx, []string{id})
				if err != nil {
					t.Fatal(err)
				}
			}
			projectionErr := d.ApplyConversationSyncV2Projection(ctx, cp, epochs, []*model_struct.LocalConversation{{ConversationID: id, LatestMsg: "existing", LatestMsgSendTime: 80}})
			row, err := d.GetConversation(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if row.LatestMsgSendTime != 0 || row.LatestMsg != "" {
				t.Fatalf("background projection resurfaced hidden conversation: timestamp=%d preview=%q projectionError=%v", row.LatestMsgSendTime, row.LatestMsg, projectionErr)
			}
			visibility, err := d.GetConversationSyncV2Visibility(ctx, id)
			if err != nil || !visibility.Hidden {
				t.Fatalf("hidden state: %+v %v", visibility, err)
			}
			if err = d.Close(ctx); err != nil {
				t.Fatal(err)
			}
			d, err = NewDataBase(ctx, "u", directory, 0)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := d.GetConversationSyncV2Visibility(ctx, id)
			if err != nil || *persisted != *visibility {
				t.Fatalf("hide not durable: %+v %v", persisted, err)
			}
			if err = d.HideConversationSyncV2(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err = d.RevealConversationSyncV2(ctx, id, visibility.Revision); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
				t.Fatalf("stale reveal accepted: %v", err)
			}
			current, err := d.GetConversationSyncV2Visibility(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if err = d.RevealConversationSyncV2(ctx, id, current.Revision); err != nil {
				t.Fatal(err)
			}
			epochs, err = d.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.ApplyConversationSyncV2Projection(ctx, cp, epochs, []*model_struct.LocalConversation{{ConversationID: id, LatestMsg: "existing", LatestMsgSendTime: 80}}); err != nil {
				t.Fatal(err)
			}
			restored, err := d.GetConversation(ctx, id)
			if err != nil || restored.LatestMsgSendTime != 80 {
				t.Fatalf("current reveal failed: %+v %v", restored, err)
			}
			if err = d.HideConversationSyncV2(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err = d.SetConversationDraftDB(ctx, id, "local draft"); err != nil {
				t.Fatal(err)
			}
			epochs, err = d.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.ApplyConversationSyncV2Projection(ctx, cp, epochs, []*model_struct.LocalConversation{{ConversationID: id, LatestMsg: "existing", LatestMsgSendTime: 80}}); err != nil {
				t.Fatal(err)
			}
			drafted, err := d.GetConversation(ctx, id)
			if err != nil || drafted.DraftText != "local draft" || drafted.LatestMsgSendTime == 0 {
				t.Fatalf("draft entrance lost: %+v %v", drafted, err)
			}
			messages, err := d.GetMessagesByClientMsgIDs(ctx, id, []string{"kept"})
			if err != nil || len(messages) != 1 {
				t.Fatalf("hide removed history: %v %v", messages, err)
			}

		})
	}
}

func TestSyncV2HiddenConversationMetadataPinTransition(t *testing.T) {
	for _, alreadyPinned := range []bool{false, true} {
		name := "new_remote_pin_reveals"
		if alreadyPinned {
			name = "repeated_pin_preserves_hide"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			id := "sg_9007199254740995777"
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{id}}
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "messages"
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, IsPinned: alreadyPinned, LatestMsg: "saved", LatestMsgSendTime: 80}); err != nil {
				t.Fatal(err)
			}
			if err = d.HideConversationSyncV2(ctx, id); err != nil {
				t.Fatal(err)
			}
			before, err := d.GetConversationSyncV2(ctx)
			if err != nil {
				t.Fatal(err)
			}
			next := *before
			next.Version++
			next.StateEpochs, err = d.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			response := &pb.StateResponse{States: []*pb.State{{ConversationID: id, Known: true, IsPinned: true, MinSeq: 1, MaxSeq: 2, Conversation: &conversation.Conversation{ConversationID: id, OwnerUserID: "u", IsPinned: true}}}}
			if err = d.ApplyConversationSyncV2Metadata(ctx, before, &next, response, nil); err != nil {
				t.Fatal(err)
			}
			current, err := d.GetConversationSyncV2(ctx)
			if err != nil {
				t.Fatal(err)
			}
			epochs, err := d.GetConversationSyncV2Epochs(ctx, []string{id})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.ApplyConversationSyncV2Projection(ctx, current, epochs, []*model_struct.LocalConversation{{ConversationID: id, LatestMsg: "saved", LatestMsgSendTime: 80}}); err != nil {
				t.Fatal(err)
			}
			visibility, err := d.GetConversationSyncV2Visibility(ctx, id)
			if err != nil || visibility.Hidden != alreadyPinned {
				t.Fatalf("unexpected pin visibility: %+v %v", visibility, err)
			}
			row, err := d.GetConversation(ctx, id)
			if err != nil || !row.IsPinned || (row.LatestMsgSendTime == 0) != alreadyPinned {
				t.Fatalf("unexpected pin projection: %+v %v", row, err)
			}
		})
	}
}

func TestSyncV2HideRejectsInflightPinnedMetadata(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	id := "sg_target"
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{id}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, LatestMsg: "saved", LatestMsgSendTime: 80}); err != nil {
		t.Fatal(err)
	}
	before, err := d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := *before
	next.Version++
	next.StateEpochs, err = d.GetConversationSyncV2Epochs(ctx, []string{id})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.HideConversationSyncV2(ctx, id); err != nil {
		t.Fatal(err)
	}
	response := &pb.StateResponse{States: []*pb.State{{ConversationID: id, Known: true, IsPinned: true, MinSeq: 1, MaxSeq: 2, Conversation: &conversation.Conversation{ConversationID: id, OwnerUserID: "u", IsPinned: true}}}}
	if err = d.ApplyConversationSyncV2Metadata(ctx, before, &next, response, nil); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("inflight pinned metadata survived hide: %v", err)
	}
	visibility, err := d.GetConversationSyncV2Visibility(ctx, id)
	if err != nil || !visibility.Hidden {
		t.Fatalf("stale pin cleared hidden marker: %+v %v", visibility, err)
	}
}
