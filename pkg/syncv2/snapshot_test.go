package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestReadSnapshotSharesSnapshotAcrossScopes(t *testing.T) {
	var scopes, cursors []string
	released := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/conversation/sync_snapshot_release" {
			released++
			fmt.Fprint(w, `{"errCode":0}`)
			return
		}
		var req pb.SnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		scopes = append(scopes, req.Scope)
		cursors = append(cursors, req.Cursor)
		if (len(scopes) == 1 && req.SnapshotID != "") || (len(scopes) > 1 && req.SnapshotID != "s") {
			t.Error("snapshot rebuilt between scopes")
		}
		resp := &pb.SnapshotResponse{SnapshotID: "s", PolicyVersion: PolicyVersion, VersionID: "v", Version: 9, Mode: "filtered", Scope: req.Scope, ExpiresAt: 100}
		if req.Scope == "chat" {
			resp.Total = 2
			if req.Cursor == "" {
				resp.ConversationIDs = []string{"9007199254740993"}
				resp.HasMore = true
				resp.Cursor = "c"
			} else {
				resp.ConversationIDs = []string{"chat2"}
			}
		} else {
			resp.Total = 1
			resp.ConversationIDs = []string{"departed"}
			resp.IncludeSelf = true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
	}))
	defer server.Close()
	result, err := NewClient(server.URL, "", server.Client()).ReadSnapshot(context.Background(), "op", "u", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scopes, []string{"chat", "chat", "notifications"}) || !reflect.DeepEqual(cursors, []string{"", "c", ""}) {
		t.Fatalf("wrong traversal: %v %v", scopes, cursors)
	}
	if released != 1 || !reflect.DeepEqual(result.ChatIDs, []string{"9007199254740993", "chat2"}) || !reflect.DeepEqual(result.NotificationIDs, []string{"departed"}) || !result.IncludeSelf || result.Version != 9 {
		t.Fatalf("incomplete snapshot: %+v releases=%d", result, released)
	}
}

func TestReadSnapshotRejectsNonAdvancingCursorAndReleases(t *testing.T) {
	calls, released := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/conversation/sync_snapshot_release" {
			released++
			fmt.Fprint(w, `{"errCode":0}`)
			return
		}
		calls++
		fmt.Fprint(w, `{"errCode":0,"data":{"snapshotID":"s","policyVersion":"conversation-sync-v2.2","versionID":"v","mode":"full","scope":"chat","total":3,"hasMore":true,"cursor":"same","conversationIDs":["a"]}}`)
	}))
	defer server.Close()
	result, err := NewClient(server.URL, "", server.Client()).ReadSnapshot(context.Background(), "op", "u", 1)
	if err == nil || result != nil || calls != 2 || released != 1 {
		t.Fatalf("partial/broken snapshot accepted: result=%v err=%v calls=%d releases=%d", result, err, calls, released)
	}
}

func TestReadSnapshotReleaseFailureDoesNotRebuildCompleteSelection(t *testing.T) {
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/conversation/sync_snapshot_release" {
			fmt.Fprint(w, `{"errCode":14,"errMsg":"SYNC_V2_REDIS_UNAVAILABLE"}`)
			return
		}
		var req pb.SnapshotRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.SnapshotID == "" {
			creates++
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.SnapshotResponse{SnapshotID: "s", PolicyVersion: PolicyVersion, Mode: "full", Scope: req.Scope, IncludeSelf: req.Scope == "notifications"}})
	}))
	defer server.Close()
	selection, err := NewClient(server.URL, "", server.Client()).ReadSnapshot(context.Background(), "op", "u", 500)
	if err != nil || selection == nil || selection.ReleaseError == nil || creates != 1 {
		t.Fatalf("release failure discarded complete result: %+v %v", selection, err)
	}
}
