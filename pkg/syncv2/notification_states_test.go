package syncv2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/sdkws"
)

type notificationInvalidationStore struct{ batchStreamMemory }

func (s *notificationInvalidationStore) AcceptConversationSyncV2State(ctx context.Context, epoch uint64, state *model_struct.LocalConversationSyncV2Stream) (*model_struct.LocalConversationSyncV2Stream, error) {
	result, err := s.batchStreamMemory.AcceptConversationSyncV2State(ctx, epoch, state)
	if state.ConversationID == "n_a" {
		s.epochs["n_b"]++
	}
	return result, err
}

func TestNotificationPrefetchOverlapsReadsButRechecksInvalidations(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req pb.StateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		id := req.ConversationIDs[0]
		mu.Lock()
		calls[id]++
		total := calls["sg_a"] + calls["sg_b"]
		if total == 2 {
			close(gate)
		}
		mu.Unlock()
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{{ConversationID: "n_" + id[3:], Known: true, StateToken: "t"}}}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := &notificationInvalidationStore{batchStreamMemory: batchStreamMemory{epochs: map[string]uint64{}}}
	result, err := NewClient(server.URL, "", server.Client()).SyncStreams(ctx, "prefetch", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"sg_a", "sg_b"}, Notifications: true}, &pb.Capabilities{MaxBatchSize: 1, MaxRequestBytes: 65536}, store, func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error { return nil })
	if err != nil || result.Completed != 2 {
		t.Fatalf("prefetch failed: %v %v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["sg_a"] != 1 || calls["sg_b"] != 2 {
		t.Fatalf("invalidated prefetched state was reused: %v", calls)
	}
}

type serialEpochStore struct {
	batchStreamMemory
	active atomic.Int32
	peak   atomic.Int32
}

func (s *serialEpochStore) GetConversationSyncV2Epochs(ctx context.Context, ids []string) (map[string]uint64, error) {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); active > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, active) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	return s.batchStreamMemory.GetConversationSyncV2Epochs(ctx, ids)
}
func TestNotificationPrefetchKeepsSQLiteReadsSerial(t *testing.T) {
	store := &serialEpochStore{batchStreamMemory: batchStreamMemory{epochs: map[string]uint64{}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{}})
	}))
	defer server.Close()
	_, err := NewClient(server.URL, "", server.Client()).prefetchNotificationStates(context.Background(), "sqlite", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"sg_a", "sg_b", "sg_c", "sg_d"}, Notifications: true}, &pb.Capabilities{MaxBatchSize: 1, MaxRequestBytes: 65536}, store)
	if err != nil {
		t.Fatal(err)
	}
	if store.peak.Load() != 1 {
		t.Fatalf("notification prefetch opened concurrent SQLite reads: %d", store.peak.Load())
	}
}
