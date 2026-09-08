package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestStatesBatchedSeparatesSelfAndSkipsEmpty(t *testing.T) {
	var requests []*pb.StateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Error("wrong states route")
		}
		req := new(pb.StateRequest)
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			t.Error(err)
		}
		requests = append(requests, req)
		fmt.Fprint(w, `{"errCode":0,"data":{}}`)
	}))
	defer server.Close()
	client := NewClient(server.URL, "", server.Client())
	caps := &pb.Capabilities{MaxBatchSize: 2, MaxRequestBytes: 65536}
	if _, err := client.StatesBatched(context.Background(), "op", &pb.StateRequest{UserID: "u"}, caps); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 0 {
		t.Fatal("empty IDs sent to server")
	}
	_, err := client.StatesBatched(context.Background(), "op", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"a", "b", "c"}, Notifications: true, IncludeSelf: true}, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 3 || len(requests[0].ConversationIDs) != 2 || len(requests[1].ConversationIDs) != 1 || len(requests[2].ConversationIDs) != 0 || !requests[2].IncludeSelf {
		t.Fatalf("incorrect batches: %v", requests)
	}
	for _, req := range requests[:2] {
		if req.IncludeSelf || !req.Notifications || req.IncludeDetails {
			t.Fatal("notification flags changed")
		}
	}
}

func TestStatesBatchedSplitsOnlyExplicitSizeErrors(t *testing.T) {
	for _, reason := range []string{"SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH", "SYNC_V2_READ_CONCURRENCY"} {
		t.Run(reason, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req pb.StateRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if len(req.ConversationIDs) > 1 {
					fmt.Fprintf(w, `{"errCode":8,"errMsg":%q}`, reason)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{UnavailableIDs: req.ConversationIDs}})
			}))
			defer server.Close()
			result, err := NewClient(server.URL, "", server.Client()).StatesBatched(context.Background(), "op", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"a", "b"}, IncludeDetails: true}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536})
			if reason == "SYNC_V2_READ_CONCURRENCY" {
				if err == nil || calls != 1 || result != nil {
					t.Fatal("concurrency error split or hidden")
				}
				return
			}
			if err != nil || calls != 3 || len(result.UnavailableIDs) != 2 {
				t.Fatalf("failed size split: %v calls=%d", err, calls)
			}
		})
	}
}
