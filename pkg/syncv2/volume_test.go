package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// Mirrors the pinned backend fixture's 50,000 parents and 150 selected chats.
// This verifies request scope/counts, not device startup latency or traffic.
func TestPrepareRoundLargeAccountRequestScope(t *testing.T) {
	chatIDs, notificationIDs, selfReads, snapshotCreates, releases := 0, 0, 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"maxRequestBytes":65536,"snapshotTTLSeconds":600}}`)
		case "/conversation/sync_snapshot":
			var req pb.SnapshotRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.SnapshotID == "" {
				snapshotCreates++
			}
			total := 150
			if req.Scope == "notifications" {
				total = 50000
			}
			start := 0
			if req.Cursor != "" {
				var err error
				start, err = strconv.Atoi(req.Cursor)
				if err != nil {
					t.Error(err)
				}
			} // test fixture cursor, not a business identifier
			end := min(start+int(req.PageSize), total)
			resp := &pb.SnapshotResponse{SnapshotID: "s", Scope: req.Scope, PolicyVersion: PolicyVersion, Mode: "filtered", VersionID: "v", Total: int64(total), HasMore: end < total, IncludeSelf: req.Scope == "notifications" && start == 0}
			for i := start; i < end; i++ {
				resp.ConversationIDs = append(resp.ConversationIDs, fmt.Sprintf("sg_fixture_%d", i))
			}
			if resp.HasMore {
				resp.Cursor = strconv.Itoa(end)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
		case "/conversation/sync_snapshot_release":
			releases++
			fmt.Fprint(w, `{"errCode":0}`)
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.ConversationIDs) > 500 {
				t.Fatal("batch limit exceeded")
			}
			if req.Notifications {
				notificationIDs += len(req.ConversationIDs)
				if req.IncludeDetails {
					t.Error("notification fetched chat details")
				}
			} else {
				chatIDs += len(req.ConversationIDs)
				if len(req.ConversationIDs) == 0 {
					t.Error("unbounded chat request")
				}
			}
			if req.IncludeSelf {
				selfReads++
				if len(req.ConversationIDs) != 0 {
					t.Error("self mixed with parents")
				}
			}
			fmt.Fprint(w, `{"errCode":0,"data":{}}`)
		default:
			t.Errorf("unexpected legacy route: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	round, err := NewClient(server.URL, "", server.Client()).PrepareRound(context.Background(), "op", "u", nil, new(checkpointMemory))
	if err != nil {
		t.Fatal(err)
	}
	if len(round.Selection.ChatIDs) != 150 || chatIDs != 150 || notificationIDs != 50000 || selfReads != 1 || snapshotCreates != 1 || releases != 1 {
		t.Fatalf("scope mismatch: chats=%d notifications=%d self=%d creates=%d releases=%d", chatIDs, notificationIDs, selfReads, snapshotCreates, releases)
	}
}
