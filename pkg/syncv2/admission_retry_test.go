package syncv2

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionRetryMatchesProtocolAndHonorsRetryAfter(t *testing.T) {
	for _, reason := range []string{"SYNC_V2_READ_BUSY", "SYNC_V2_SCAN_BUSY", "SYNC_V2_BUILD_BUSY"} {
		e := &APIError{ErrCode: 8, ErrMsg: reason}
		if RecoveryFor(e) != Retry || !isAdmissionRetry(e) {
			t.Fatal(reason)
		}
		if isAdmissionRetry(&APIError{ErrCode: 500, ErrMsg: reason}) {
			t.Fatal("wrong code accepted")
		}
	}
	if !isAdmissionRetry(&APIError{ErrCode: 14, ErrMsg: "SYNC_V2_ADMISSION_UNAVAILABLE"}) {
		t.Fatal("admission unavailable")
	}
	if isAdmissionRetry(&APIError{ErrCode: 500, ErrMsg: "ServerInternalError", ErrDlt: "SYNC_V2_READ_BUSY"}) {
		t.Fatal("diagnostic detail changed recovery")
	}
	now := time.Now().UTC().Truncate(time.Second)
	if parseRetryAfter("17", now) != 17*time.Second || parseRetryAfter(now.Add(time.Minute).Format(http.TimeFormat), now) != time.Minute || parseRetryAfter("bad", now) != 0 {
		t.Fatal("Retry-After parsing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(waitAdmissionRetry(ctx, &HTTPError{StatusCode: 429, RetryAfter: time.Hour}, 0), context.Canceled) {
		t.Fatal("backoff ignored cancellation")
	}
}

func TestNegotiatedAdmissionSerializesRequestsAndRetriesSameRead(t *testing.T) {
	var active, peak, calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/conversation/sync_capabilities" {
			fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":true,"clusterReadAdmission":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"snapshotTTLSeconds":600,"maxRequestBytes":65536}}`)
			return
		}
		if r.URL.Path != "/msg/sync_messages_v2" {
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `{"errCode":8,"errMsg":"SYNC_V2_READ_BUSY"}`)
			return
		}
		fmt.Fprint(w, `{"errCode":0,"data":{}}`)
	}))
	defer server.Close()
	client := NewClient(server.URL, "", server.Client())
	if _, err := client.Capabilities(context.Background(), "op", &pb.UserRequest{}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.Pull(context.Background(), "op", &pb.PullRequest{ConversationID: "unchanged", Seqs: []int64{1}}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if peak.Load() > 2 || calls.Load() != 5 {
		t.Fatalf("peak=%d calls=%d", peak.Load(), calls.Load())
	}
	if err := client.requestSlot.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Pull(ctx, "op", &pb.PullRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	client.requestSlot.release()
	if calls.Load() != 5 {
		t.Fatal("canceled waiter reached network")
	}
}

func TestGatewayFailureRetriesSameRead(t *testing.T) {
	for _, code := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/msg/sync_messages_v2" || r.Header.Get("operationID") != "gateway-read" {
					t.Errorf("read changed: %s", r.URL.Path)
				}
				calls++
				if calls == 1 {
					w.WriteHeader(code)
					return
				}
				fmt.Fprint(w, `{"errCode":0,"data":{}}`)
			}))
			defer server.Close()
			client := NewClient(server.URL, "", server.Client())
			client.clusterAdmission.Store(true)
			if RecoveryFor(&HTTPError{StatusCode: code}) != Retry {
				t.Errorf("gateway error is terminal")
			}
			if _, err := client.Pull(context.Background(), "gateway-read", &pb.PullRequest{ConversationID: "unchanged", Seqs: []int64{1}}); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestAdmissionBackoffDoesNotBlockAnotherRead(t *testing.T) {
	backoffStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("operationID") == "background" {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			close(backoffStarted)
			return
		}
		fmt.Fprint(w, `{"errCode":0,"data":{}}`)
	}))
	defer server.Close()
	client := NewClient(server.URL, "", server.Client())
	client.clusterAdmission.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Pull(ctx, "background", &pb.PullRequest{ConversationID: "background", Seqs: []int64{1}})
		done <- err
	}()
	select {
	case <-backoffStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("background request did not reach server")
	}
	foreground, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := client.Pull(foreground, "foreground", &pb.PullRequest{ConversationID: "opened", Seqs: []int64{1}}); err != nil {
		t.Errorf("another read was blocked by background backoff: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("background retry ignored cancellation: %v", err)
	}
}
