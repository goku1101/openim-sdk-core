package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"testing"
)

type batchStreamMemory struct {
	epochs   map[string]uint64
	accepted int
}

func (s *batchStreamMemory) GetConversationSyncV2Epochs(_ context.Context, ids []string) (map[string]uint64, error) {
	out := map[string]uint64{}
	for _, id := range ids {
		out[id] = s.epochs[id]
	}
	return out, nil
}
func (s *batchStreamMemory) GetConversationSyncV2Stream(_ context.Context, id string) (*model_struct.LocalConversationSyncV2Stream, error) {
	return &model_struct.LocalConversationSyncV2Stream{ConversationID: id, Epoch: s.epochs[id]}, nil
}
func (s *batchStreamMemory) AcceptConversationSyncV2State(_ context.Context, epoch uint64, state *model_struct.LocalConversationSyncV2Stream) (*model_struct.LocalConversationSyncV2Stream, error) {
	if s.epochs[state.ConversationID] != epoch {
		return nil, fmt.Errorf("stale epoch")
	}
	s.accepted++
	return state, nil
}

func TestSyncStreamsBatchesFiftyThousandNotifications(t *testing.T) {
	store := &batchStreamMemory{epochs: map[string]uint64{}}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("empty streams must not pull: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		requests++
		var req pb.StateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if len(req.ConversationIDs) > 500 || !req.Notifications {
			t.Errorf("invalid request: %+v", req)
		}
		resp := &pb.StateResponse{}
		for _, id := range req.ConversationIDs {
			resp.States = append(resp.States, &pb.State{ConversationID: "n" + id[2:], Known: true, StateToken: "t"})
		}
		if req.IncludeSelf {
			if len(req.ConversationIDs) != 0 {
				t.Error("self mixed with parents")
			}
			resp.States = append(resp.States, &pb.State{ConversationID: "n_u_u", Known: true, StateToken: "t"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
	}))
	defer server.Close()
	ids := make([]string, 50000)
	for i := range ids {
		ids[i] = fmt.Sprintf("sg_%d", i)
	}
	client := NewClient(server.URL, "", server.Client())
	progressCalls := 0
	ctx := withStreamProgress(context.Background(), func(done, total int) {
		progressCalls++
		if done != progressCalls || total != 50001 || store.accepted != done {
			t.Errorf("progress before consumption: done=%d total=%d accepted=%d", done, total, store.accepted)
		}
	})
	result, err := client.SyncStreams(ctx, "op", &pb.StateRequest{UserID: "u", ConversationIDs: ids, Notifications: true, IncludeSelf: true}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, store, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
		t.Fatal("no messages to commit")
		return nil
	})
	if err != nil || requests != 101 || store.accepted != 50001 || result.Completed != 50001 || progressCalls != 50001 {
		t.Fatalf("batching lost: result=%+v requests=%d accepted=%d err=%v", result, requests, store.accepted, err)
	}
}

func TestSyncStreamsRejectsBatchInvalidatedDuringStateQuery(t *testing.T) {
	store := &batchStreamMemory{epochs: map[string]uint64{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		store.epochs["si_u_v"]++
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{{ConversationID: "si_u_v", Known: true, StateToken: "t"}}}})
	}))
	defer server.Close()
	client := NewClient(server.URL, "", server.Client())
	_, err := client.SyncStreams(context.Background(), "op", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"si_u_v"}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, store, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error { return nil })
	if err == nil || store.accepted != 0 {
		t.Fatal("stale batch state accepted")
	}
}

func TestSyncStreamsContinuesAfterPermissionIsLostDuringPull(t *testing.T) {
	store := &batchStreamMemory{epochs: map[string]uint64{}}
	pulls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/msg/sync_messages_v2" {
			pulls++
			fmt.Fprint(w, `{"errCode":7,"errMsg":"SYNC_V2_CONVERSATION_UNAVAILABLE"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{{ConversationID: "si_denied", Known: true, StateToken: "t", MinSeq: 1, MaxSeq: 1}, {ConversationID: "si_empty", Known: true, StateToken: "t", MinSeq: 1}}}})
	}))
	defer server.Close()
	result, err := NewClient(server.URL, "", server.Client()).SyncStreams(context.Background(), "op", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"si_denied", "si_empty"}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, store, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
		t.Fatal("denied content was committed")
		return nil
	})
	if err != nil || pulls != 1 || result.Completed != 1 || len(result.PendingIDs) != 0 || len(result.UnavailableIDs) != 1 || result.UnavailableIDs[0] != "si_denied" {
		t.Fatalf("terminal read blocked remaining stream: %+v %v", result, err)
	}
}
