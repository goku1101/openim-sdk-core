package syncv2

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"testing"
)

func TestPriorityPageReportsDirectoryTotals(t *testing.T) {
	task := &model_struct.ConversationSyncV2Task{NotificationsDone: true, ChatProcessed: 500, ChatTotal: 2000}
	var detail DetailProgress
	percentage := 0
	ctx := WithDetailProgress(context.Background(), func(d DetailProgress) { detail = d })
	page := priorityPageContext(ctx, task, "chat", CoordinatorHooks{Progress: func(n int) { percentage = n }})
	reportCount(page, "messages", "conversations", 1, 500)
	if detail.Completed != 501 || detail.Total == nil || *detail.Total != 2000 {
		t.Fatalf("page leaked into directory counts: %+v", detail)
	}
	if percentage != 58 {
		t.Fatalf("progress=%d", percentage)
	}
	task.ChatProcessed = 1500
	if PriorityProgress(task) != 76 {
		t.Fatal("resume did not restore durable progress")
	}
}
