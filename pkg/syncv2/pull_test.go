package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

func TestPullBatchedPreservesUnresolvedSequences(t *testing.T) {
	var requests [][]int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_messages_v2" {
			t.Error("unexpected route")
		}
		var req pb.PullRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		requests = append(requests, req.Seqs)
		if req.ConversationID != "sg_parent" || !req.Notifications || req.Self {
			t.Error("notification parent replaced by stream ID")
		}
		resp := &pb.PullResponse{State: &pb.State{ConversationID: "n_parent", StateToken: "token", Known: true, MinSeq: 1, MaxSeq: 5}}
		for _, seq := range req.Seqs {
			if seq == 3 {
				resp.UnresolvedSeqs = append(resp.UnresolvedSeqs, seq)
			} else {
				resp.Messages = append(resp.Messages, &sdkws.MsgData{Seq: seq, ClientMsgID: fmt.Sprint("m", seq)})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
	}))
	defer server.Close()
	resp, err := NewClient(server.URL, "", server.Client()).PullBatched(context.Background(), "op", &pb.PullRequest{UserID: "u", ConversationID: "sg_parent", Notifications: true, StateToken: "token", Seqs: []int64{1, 2, 3, 4, 5}}, &pb.Capabilities{MaxBatchSize: 2, MaxRequestBytes: 65536})
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 3 || len(resp.Messages) != 4 || len(resp.UnresolvedSeqs) != 1 || resp.UnresolvedSeqs[0] != 3 {
		t.Fatalf("unresolved hole discarded: %+v requests=%v", resp, requests)
	}
}

func TestPullBatchedRejectsWrongStreamAndStaleBoundary(t *testing.T) {
	for _, wrongStream := range []bool{true, false} {
		t.Run(fmt.Sprint(wrongStream), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				state := &pb.State{ConversationID: "sg_parent", StateToken: "token"}
				if wrongStream {
					state.ConversationID = "sg_other"
				} else if calls == 2 {
					state.StateToken = "changed"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.PullResponse{State: state}})
			}))
			defer server.Close()
			resp, err := NewClient(server.URL, "", server.Client()).PullBatched(context.Background(), "op", &pb.PullRequest{UserID: "u", ConversationID: "sg_parent", StateToken: "token", Seqs: []int64{1, 2}}, &pb.Capabilities{MaxBatchSize: 1, MaxRequestBytes: 65536})
			if err == nil || resp != nil {
				t.Fatal("wrong stream or partial stale response accepted")
			}
		})
	}
}

func TestPullBatchedShrinksOnlyCapacityFailures(t *testing.T) {
	for _, reason := range []string{"SYNC_V2_RESPONSE_LIMIT_REDUCE_BATCH", "SYNC_V2_READ_CONCURRENCY"} {
		t.Run(reason, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req pb.PullRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if len(req.Seqs) > 1 {
					fmt.Fprintf(w, `{"errCode":8,"errMsg":%q}`, reason)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.PullResponse{State: &pb.State{ConversationID: "sg_c", StateToken: "t", Known: true, MinSeq: 1, MaxSeq: 2}, UnresolvedSeqs: req.Seqs}})
			}))
			defer server.Close()
			result, err := NewClient(server.URL, "", server.Client()).PullBatched(context.Background(), "op", &pb.PullRequest{UserID: "u", ConversationID: "sg_c", StateToken: "t", Seqs: []int64{1, 2}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536})
			if reason == "SYNC_V2_READ_CONCURRENCY" {
				if err == nil || result != nil || calls != 1 {
					t.Fatal("concurrency failure split or hidden")
				}
				return
			}
			if err != nil || calls != 3 || len(result.UnresolvedSeqs) != 2 {
				t.Fatalf("capacity split failed: %+v %v calls=%d", result, err, calls)
			}
		})
	}
}
