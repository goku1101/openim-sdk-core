package syncv2

import (
	"encoding/json"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"testing"
)

func TestPendingRetryTasksKeepOnlyUnfinishedAuthorizedSelection(t *testing.T) {
	cp := &model_struct.LocalConversationSyncV2{UserID: "9007199254740993", SelectedIDs: []string{"si_pending", "si_done", "si_unavailable"}, NotificationIDs: []string{"sg_notice"}, IncludeSelf: true}
	result := &SyncResult{Checkpoint: cp, Chats: &StreamResults{PendingIDs: []string{"si_pending", "si_pending"}, UnavailableIDs: []string{"si_unavailable"}}, Notifications: &StreamResults{PendingIDs: []string{"n_notice", "n_9007199254740993_9007199254740993"}}}
	jobs, err := PendingRetryTasks(result, 123)
	if err != nil || len(jobs) != 3 {
		t.Fatalf("bad retry plan: %v %v", jobs, err)
	}
	for _, job := range jobs {
		var req pb.PullRequest
		if err = json.Unmarshal([]byte(job.Payload), &req); err != nil {
			t.Fatal(err)
		}
		if req.UserID != cp.UserID || job.DueAt != 123 || job.Kind != "reconcile" {
			t.Fatal("retry changed identity/deadline")
		}
		if job.TaskID == "reconcile:n_notice" && (req.ConversationID != "sg_notice" || !req.Notifications) {
			t.Fatal("notification parent lost")
		}
		if req.ConversationID == "si_done" || req.ConversationID == "si_unavailable" {
			t.Fatal("completed/unavailable stream retried")
		}
	}
	result.Chats.PendingIDs = []string{"si_outside"}
	if _, err = PendingRetryTasks(result, 123); err == nil {
		t.Fatal("unregistered pending stream accepted")
	}
}
