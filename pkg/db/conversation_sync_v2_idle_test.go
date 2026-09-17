//go:build !js

package db

import (
	"context"
	"testing"

	"gorm.io/gorm"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

func TestUnchangedStreamsRequireConfirmedBoundsAndNoPendingWork(t *testing.T) {
	for _, name := range []string{"unchanged", "new-tail", "pending", "invalidated-epoch", "changed-token", "changed-min", "changed-permission", "unknown", "uninitialized", "invalid-bounds"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			row := &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", Epoch: 2, Initialized: true, StateToken: "t", MinSeq: 1, ScannedSeq: 5}
			state := &pb.State{ConversationID: "c", Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 5}
			epoch := uint64(2)
			switch name {
			case "new-tail":
				state.MaxSeq = 6
			case "pending":
				row.PendingSeqs = []int64{3}
			case "invalidated-epoch":
				epoch = 1
			case "changed-token":
				state.StateToken = "new"
			case "changed-min":
				state.MinSeq = 3
			case "changed-permission":
				state.PermissionMaxSeq = 4
			case "unknown":
				state.Known = false
			case "uninitialized":
				row.Initialized = false
			case "invalid-bounds":
				state.MaxSeq = -1
			}
			if err := d.conn.Create(row).Error; err != nil {
				t.Fatal(err)
			}
			result, err := d.UnchangedConversationSyncV2Streams(ctx, map[string]uint64{"c": epoch}, []*pb.State{state}, false)
			if err != nil || result["c"] != (name == "unchanged") {
				t.Fatalf("unsafe idle classification: %v %v", result, err)
			}
			current, err := d.GetConversationSyncV2Stream(ctx, "c")
			if err != nil || current.Epoch != 2 {
				t.Fatal("read-only confirmation modified epoch")
			}
		})
	}
}

func TestMetadataRefreshPreservesUnchangedPermission(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-permission", true: "changed-permission"}[changed], func(t *testing.T) {
			ctx := context.Background()
			d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"c"}}
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "ready"
			if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			row := &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", Epoch: 2, Initialized: true, StateToken: "t", MinSeq: 1, ScannedSeq: 5}
			if err = d.conn.Create(row).Error; err != nil {
				t.Fatal(err)
			}
			state := &pb.State{ConversationID: "c", Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 6, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "c"}}
			if changed {
				state.MinSeq = 3
				state.StateToken = "new"
			}
			next := *cp
			next.Stage = "messages"
			next.StateEpochs = map[string]uint64{"c": 2}
			if err = d.ApplyConversationSyncV2Metadata(ctx, cp, &next, &pb.StateResponse{States: []*pb.State{state}}, nil); err != nil {
				t.Fatal(err)
			}
			current, err := d.GetConversationSyncV2Stream(ctx, "c")
			if err != nil {
				t.Fatal(err)
			}
			if !changed && (current.Epoch != 2 || current.StateToken != "t") {
				t.Fatal("unchanged permission was invalidated")
			}
			if changed && (current.Epoch != 3 || current.StateToken != "") {
				t.Fatal("new permission did not retire in-flight history")
			}
			detail, err := d.GetConversation(ctx, "c")
			if err != nil || detail.MaxSeq != 6 || detail.UnreadCount != 2 {
				t.Fatal("preserving permission skipped metadata refresh")
			}
		})
	}
}

// A second identical server response must not rewrite cached conversations.
// Keep permission validation and changed unread/bounds updates in the same path.
func TestIdleMetadataOnlyWritesChangedConversations(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"c"}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	response := &pb.StateResponse{States: []*pb.State{{ConversationID: "c", Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 5, UnreadCount: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "c"}}}}
	apply := func() {
		t.Helper()
		current, e := d.GetConversationSyncV2(ctx)
		if e != nil {
			t.Fatal(e)
		}
		next := *current
		next.Stage = "messages"
		next.StateEpochs, e = d.GetConversationSyncV2Epochs(ctx, []string{"c"})
		if e != nil {
			t.Fatal(e)
		}
		if e = d.ApplyConversationSyncV2Metadata(ctx, current, &next, response, nil); e != nil {
			t.Fatal(e)
		}
	}
	apply()
	writes := 0
	count := func(tx *gorm.DB) {
		if tx.Statement.Table == "local_conversations" {
			writes++
		}
	}
	if err = d.conn.Callback().Create().Before("gorm:create").Register("test:idle-create", count); err != nil {
		t.Fatal(err)
	}
	if err = d.conn.Callback().Update().Before("gorm:update").Register("test:idle-update", count); err != nil {
		t.Fatal(err)
	}
	apply()
	if writes != 0 {
		t.Fatalf("unchanged response rewrote %d conversation rows", writes)
	}
	response.States[0].UnreadCount = 0
	apply()
	row, err := d.GetConversation(ctx, "c")
	if err != nil || row.UnreadCount != 0 || writes != 1 {
		t.Fatalf("read-only change was lost: writes=%d err=%v", writes, err)
	}
	response.States[0].MinSeq = 4
	response.States[0].StateToken = "new"
	apply()
	row, err = d.GetConversation(ctx, "c")
	if err != nil || row.MinSeq != 4 || writes != 2 {
		t.Fatalf("permission change was lost: writes=%d err=%v", writes, err)
	}
}

func TestCachedSyncV2DetailsExcludePartialAndUnselectedRows(t *testing.T) {
	ctx := context.Background()
	d, err := NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SelectedIDs: []string{"loaded", "pending", "unavailable"}, PendingStateIDs: []string{"pending"}, UnavailableIDs: []string{"unavailable"}}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"loaded", "pending", "unavailable", "local-only"} {
		if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, Ex: "cached"}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := d.GetConversationSyncV2CachedDetails(ctx, []string{"loaded", "pending", "unavailable", "local-only", "missing"})
	if err != nil || len(rows) != 1 || rows["loaded"] == nil || rows["loaded"].Ex != "cached" {
		t.Fatalf("partial metadata reused: count=%d err=%v", len(rows), err)
	}
}
