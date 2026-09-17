package syncv2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

type metadataCacheFixture struct{ MetadataStore }

func (metadataCacheFixture) GetConversationSyncV2CachedDetails(context.Context, []string) (map[string]*conversation.Conversation, error) {
	return map[string]*conversation.Conversation{"cached": {OwnerUserID: "u", ConversationID: "cached", Ex: "cached"}, "changed": {OwnerUserID: "u", ConversationID: "changed", Ex: "old"}}, nil
}
func TestRecoveryDetailsAreDeltaButStateIsAlwaysFresh(t *testing.T) {
	modes := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req pb.StateRequest
		json.NewDecoder(r.Body).Decode(&req)
		response := &pb.StateResponse{}
		for _, id := range req.ConversationIDs {
			modes[id] = req.IncludeDetails
			state := &pb.State{ConversationID: id, Known: true, StateToken: "fresh-permission", MinSeq: 5, MaxSeq: 8, UnreadCount: 0}
			if req.IncludeDetails {
				state.Conversation = &conversation.Conversation{OwnerUserID: "u", ConversationID: id, Ex: "server"}
			}
			response.States = append(response.States, state)
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
	}))
	defer server.Close()
	ctx := context.WithValue(context.Background(), preparedChatStatesKey{}, &preparedChatStates{})
	result, err := NewClient(server.URL, "", server.Client()).metadataStatesForDelta(ctx, "delta", "u", []string{"cached", "changed", "missing"}, &conversation.GetIncrementalConversationResp{Update: []*conversation.Conversation{{ConversationID: "changed"}}}, &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, metadataCacheFixture{})
	if err != nil {
		t.Fatal(err)
	}
	if len(modes) != 3 || modes["cached"] || !modes["changed"] || !modes["missing"] {
		t.Fatalf("incorrect detail requests: %v", modes)
	}
	for _, state := range result.States {
		if state.MinSeq != 5 || state.MaxSeq != 8 || state.UnreadCount != 0 || state.StateToken != "fresh-permission" {
			t.Fatal("cached metadata masked new state")
		}
		want := "server"
		if state.ConversationID == "cached" {
			want = "cached"
		}
		if state.Conversation.Ex != want {
			t.Fatal("changed metadata was not refreshed")
		}
	}
}
