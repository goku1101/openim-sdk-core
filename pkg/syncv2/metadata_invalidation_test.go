package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type invalidatedMetadataStore struct {
	metadataTimingStore
	epoch    uint64
	attempts int
	mode     string
}

func (s *invalidatedMetadataStore) GetConversationSyncV2Epochs(context.Context, []string) (map[string]uint64, error) {
	return map[string]uint64{"changed": s.epoch}, nil
}
func (s *invalidatedMetadataStore) ApplyConversationSyncV2Metadata(ctx context.Context, before, next *model_struct.LocalConversationSyncV2, response *pb.StateResponse, deleted []string) error {
	s.attempts++
	if s.attempts == 1 || s.mode == "continuous" {
		s.epoch++
		if s.mode == "superseded" {
			copy := *s.current
			copy.Revision++
			s.current = &copy
		}
		return sdkerrs.ErrSyncV2StateChanged
	}
	if next.StateEpochs["changed"] != s.epoch {
		return sdkerrs.ErrSyncV2StateChanged
	}
	return s.metadataTimingStore.ApplyConversationSyncV2Metadata(ctx, before, next, response, deleted)
}
func TestMetadataInvalidationRefreshesOnlyChangedConversation(t *testing.T) {
	for _, mode := range []string{"once", "superseded", "continuous"} {
		t.Run(mode, func(t *testing.T) {
			before := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "messages", VersionID: "v", Version: 1, SelectedIDs: []string{"stable", "changed"}}
			store := &invalidatedMetadataStore{metadataTimingStore: metadataTimingStore{current: before}, mode: mode}
			var requests [][]string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/msg/sync_seqs_v2" {
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				var req pb.StateRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				requests = append(requests, req.ConversationIDs)
				if !req.IncludeDetails {
					t.Error("missing authoritative details")
				}
				resp := &pb.StateResponse{}
				for _, id := range req.ConversationIDs {
					resp.States = append(resp.States, &pb.State{ConversationID: id, Known: true, MinSeq: int64(len(requests))})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
			}))
			defer server.Close()
			_, err := NewClient(server.URL, "", server.Client()).ApplyMetadataDelta(context.Background(), "op", before, &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 2}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, store)
			if mode != "once" {
				if !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || store.commits != 0 {
					t.Fatalf("stale state committed: %v", err)
				}
				expected := 1
				if mode == "continuous" {
					expected = 3
				}
				if len(requests) != expected {
					t.Fatalf("unbounded/unexpected retries: %d", len(requests))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(requests, [][]string{{"stable", "changed"}, {"changed"}}) {
				t.Fatalf("unrelated details fetched again: %v", requests)
			}
			if store.commits != 1 || len(store.response.States) != 2 || store.response.States[0].MinSeq != 1 || store.response.States[1].MinSeq != 2 {
				t.Fatalf("fresh boundary or stable result lost: %+v", store.response)
			}
		})
	}
}
