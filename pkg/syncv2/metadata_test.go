package syncv2

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	conversation "github.com/openimsdk/protocol/conversation"
	"reflect"
	"testing"
)

func TestMetadataDeltaActivatesOnlyPinnedAndPreservesLoadedChats(t *testing.T) {
	before := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "messages", VersionID: "v", Version: 3, SelectedIDs: []string{"loaded", "removed"}, NotificationIDs: []string{"loaded", "removed"}}
	delta := &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 4, Delete: []string{"removed"}, Update: []*conversation.Conversation{{ConversationID: "loaded"}, {ConversationID: "profile-only"}, {ConversationID: "pinned", IsPinned: true}}}
	plan, err := PlanMetadataDelta(before, delta)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.Next.SelectedIDs, model_struct.StringArray{"loaded", "pinned"}) || !reflect.DeepEqual(plan.DeletedIDs, []string{"removed"}) {
		t.Fatalf("wrong selection: %+v", plan)
	}
	if !reflect.DeepEqual(plan.Next.NotificationIDs, model_struct.StringArray{"loaded", "removed", "profile-only", "pinned"}) {
		t.Fatalf("deferred notifications lost: %+v", plan.Next)
	}
	if before.Version != 3 || len(before.SelectedIDs) != 2 {
		t.Fatal("mutated committed checkpoint before durable apply")
	}
}
func TestMetadataDeltaRequiresV2SnapshotOnFullOrChangedVersion(t *testing.T) {
	before := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "messages", VersionID: "v", Version: 3}
	for _, delta := range []*conversation.GetIncrementalConversationResp{{Full: true}, {VersionID: "other", Version: 4}, {VersionID: "v", Version: 2}} {
		_, err := PlanMetadataDelta(before, delta)
		if err == nil || RecoveryFor(err) != RebuildSnapshot {
			t.Fatalf("must rebuild V2 snapshot: %v", err)
		}
	}
}
