package syncv2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestDiagnosticsMixedNotificationResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"errCode":0,"data":{"messages":[{"contentType":2001},{"contentType":1501}]}}`))
	}))
	defer server.Close()
	ctx, d := WithDiagnostics(context.Background())
	client := NewClient(server.URL, "", server.Client())
	_, err := client.invokeMeasured(ctx, "diagnostics-test", "/msg/sync_messages_v2", &pb.PullRequest{Notifications: true}, &pb.PullResponse{})
	if err != nil {
		t.Fatal(err)
	}
	got := d.Snapshot()
	if got["http_notification_pull"].Calls != 1 || got["http_notification_pull"].Failures != 0 || got["fetched_business_notification"].Items != 1 || got["fetched_system_notification"].Items != 1 || got["fetched_chat"].Items != 0 {
		t.Fatalf("incorrect classification: %+v", got)
	}
}

func TestDiagnosticsFailureDoesNotCountResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	ctx, d := WithDiagnostics(context.Background())
	_, err := NewClient(server.URL, "", server.Client()).invokeMeasured(ctx, "test", "/msg/sync_messages_v2", &pb.PullRequest{Notifications: true}, &pb.PullResponse{})
	if err == nil || d.Snapshot()["http_notification_pull"].Failures != 1 || d.Snapshot()["fetched_business_notification"].Calls != 0 {
		t.Fatal("failure miscounted")
	}
}

func TestDiagnosticsConcurrentSubMillisecondAccounting(t *testing.T) {
	ctx, d := WithDiagnostics(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RecordDiagnostic(ctx, "consume_business_notification", 1, 100*time.Microsecond, false)
			_ = d.Snapshot()
		}()
	}
	wg.Wait()
	got := d.Snapshot()["consume_business_notification"]
	if got.Calls != 100 || got.Items != 100 || got.DurationMS != 10 {
		t.Fatalf("lost counters: %+v", got)
	}
	snapshot := d.Snapshot()
	delete(snapshot, "consume_business_notification")
	if len(d.Snapshot()) != 1 {
		t.Fatal("snapshot aliases counters")
	}
}
