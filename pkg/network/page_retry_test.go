package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestPageReadRetriesBeforeInsert(t *testing.T) {
	calls, inserts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req relation.GetPaginationFriendsReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req.Pagination.PageNumber != 1 {
			t.Errorf("page advanced: %d", req.Pagination.PageNumber)
		}
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			w.Header().Set("Content-Length", "200")
			w.Write([]byte(`{"errCode":0,`))
			return
		}
		w.Write([]byte(`{"errCode":0,"data":{"items":["friend"]}}`))
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), pageTestConfig(server.URL)), "page-retry-test")
	type response struct {
		Items []string `json:"items"`
	}
	err := FetchAndInsertPagedData(ctx, "/friend/get_friend_list", &relation.GetPaginationFriendsReq{Pagination: &sdkws.RequestPagination{ShowNumber: 100}}, func(r *response) []string { return r.Items }, func(_ context.Context, items []string) error { inserts += len(items); return nil }, func(context.Context, string) error { t.Error("unexpected single insert"); return nil }, 0)
	if err != nil || calls != 2 || inserts != 1 {
		t.Fatalf("err=%v calls=%d inserts=%d", err, calls, inserts)
	}
}

func TestPageReadDoesNotRetryBusinessFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write([]byte(`{"errCode":3,"errMsg":"invalid"}`))
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), pageTestConfig(server.URL)), "business-test")
	_, err := fetchPageWithRetry[any](ctx, "/friend/get_friend_list", nil)
	if err == nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestPageReadCancellationStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Length", "200")
		w.Write([]byte(`{`))
	}))
	defer server.Close()
	ctx = ccontext.WithOperationID(ccontext.WithInfo(ctx, pageTestConfig(server.URL)), "cancel-test")
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := fetchPageWithRetry[any](ctx, "/friend/get_friend_list", nil)
	if !errors.Is(err, context.Canceled) || calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func pageTestConfig(addr string) *ccontext.GlobalConfig {
	var conf ccontext.GlobalConfig
	conf.ApiAddr = addr
	return &conf
}

func TestPageReadRetryLimit(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Length", "200")
		w.Write([]byte(`{`))
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), pageTestConfig(server.URL)), "limit-test")
	_, err := fetchPageWithRetry[any](ctx, "/friend/get_friend_list", nil)
	if err == nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestPageReadRecognizesBodyTimeout(t *testing.T) {
	err := &transportError{cause: context.DeadlineExceeded}
	if !retryablePageRead(context.Background(), err) {
		t.Fatal("body timeout must retry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if retryablePageRead(ctx, err) {
		t.Fatal("session cancellation must not retry")
	}
	if retryablePageRead(context.Background(), errors.New("context deadline exceeded")) {
		t.Fatal("must not classify error text")
	}
}
