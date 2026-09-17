package syncv2

import (
	"context"
	"sync"
	"testing"
)

func TestStoredProgressDeduplicatesRetriesAndResetsRound(t *testing.T) {
	count := 0
	ctx := WithStoredMessages(context.Background(), func(added int) { count += added })
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); ReportStoredMessages(ctx, "c", []string{"a", "b", "a"}) }()
	}
	workers.Wait()
	ReportStoredMessages(ctx, "other", []string{"a"})
	if count != 3 {
		t.Fatalf("retries counted: %d", count)
	}
	next := WithStoredMessages(context.Background(), func(added int) { count = added })
	ReportStoredMessages(next, "c", []string{"a"})
	if count != 1 {
		t.Fatalf("round not isolated: %d", count)
	}
}
func TestDetailProgressDoesNotInventTotalOrReportCancelledWork(t *testing.T) {
	calls := 0
	ctx := WithDetailProgress(context.Background(), func(d DetailProgress) {
		calls++
		if d.Total != nil {
			t.Fatal("invented total")
		}
	})
	ReportDetailProgress(ctx, "changes", "", 0, nil)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	ReportDetailProgress(cancelled, "messages", "conversations", 1, nil)
	if calls != 1 {
		t.Fatalf("cancelled progress published: %d", calls)
	}
}
