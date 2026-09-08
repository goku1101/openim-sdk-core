package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type checkpointMemory struct {
	stages []string
	saved  *model_struct.LocalConversationSyncV2
}

func (s *checkpointMemory) SaveConversationSyncV2(_ context.Context, v *model_struct.LocalConversationSyncV2) error {
	copy := *v
	s.saved = &copy
	s.stages = append(s.stages, v.Stage)
	return nil
}

func TestPrepareRoundPreservesLoadedChatsAndDoesNotFinishEarly(t *testing.T) {
	store := new(checkpointMemory)
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"maxRequestBytes":65536,"snapshotTTLSeconds":600}}`)
		case "/conversation/sync_snapshot":
			var req pb.SnapshotRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := &pb.SnapshotResponse{SnapshotID: "s", Scope: req.Scope, PolicyVersion: PolicyVersion, Mode: "filtered", VersionID: "v", Version: 4}
			if req.Scope == "chat" {
				resp.Total = 1
				resp.ConversationIDs = []string{"selected"}
			} else {
				resp.IncludeSelf = true
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
		case "/conversation/sync_snapshot_release":
			fmt.Fprint(w, `{"errCode":0}`)
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if !req.Notifications {
				requested = append(requested, req.ConversationIDs...)
				if !req.IncludeDetails {
					t.Error("details not requested")
				}
			}
			fmt.Fprint(w, `{"errCode":0,"data":{}}`)
		default:
			t.Errorf("legacy or unexpected route: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	round, err := NewClient(server.URL, "", server.Client()).PrepareRound(context.Background(), "op", "u", []string{"loaded", "selected"}, store)
	if err != nil {
		t.Fatal(err)
	}
	if !round.Enabled || len(requested) != 2 || requested[0] != "selected" || requested[1] != "loaded" {
		t.Fatalf("local chats discarded: %v", requested)
	}
	if len(store.stages) != 2 || store.stages[0] != "snapshot" || store.stages[1] != "details" || store.saved.Version != 4 {
		t.Fatalf("incorrect checkpoints: %+v", store)
	}
}

func TestPrepareRoundDoesNotFallbackOrOverwriteOnProbeFailure(t *testing.T) {
	for _, response := range []string{`{"errCode":14,"errMsg":"SYNC_V2_REDIS_UNAVAILABLE"}`, `{"errCode":0,"data":{}}`, `{"errCode":0,"data":{"conversationSyncV2":false}}`} {
		t.Run(response, func(t *testing.T) {
			calls := 0
			store := new(checkpointMemory)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; fmt.Fprint(w, response) }))
			defer server.Close()
			round, err := NewClient(server.URL, "", server.Client()).PrepareRound(context.Background(), "op", "u", nil, store)
			if response == `{"errCode":0,"data":{"conversationSyncV2":false}}` {
				if err != nil || round.Enabled {
					t.Fatal("explicit legacy negotiation rejected")
				}
			} else if err == nil || round != nil {
				t.Fatal("ambiguous failure became legacy mode")
			}
			if calls != 1 || len(store.stages) != 0 {
				t.Fatal("probe failure changed persistent state")
			}
		})
	}
}
