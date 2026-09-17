//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestChangeRecoveryIdleSurvivesDatabaseReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	database, err := db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close(ctx) }()
	counts := map[string]int{}
	httpFailure := 0
	changed := false
	failPublish := false
	var changeRequests []*pb.ChangeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts[r.URL.Path]++
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, IncrementalRecovery: true, IncrementalRecoveryVersion: "conversation-sync-changes-v1", SyncDomainID: "test-domain", MaxChangePageSize: 100}
		case "/conversation/sync_snapshot":
			var req pb.SnapshotRequest
			json.NewDecoder(r.Body).Decode(&req)
			data = &pb.SnapshotResponse{SnapshotID: "s", Scope: req.Scope, IncludeSelf: req.Scope == "notifications", Mode: "full", PolicyVersion: syncv2.PolicyVersion, VersionID: "v", Version: 1, BaselineCursor: "opaque-baseline"}
		case "/conversation/sync_snapshot_release":
			fmt.Fprint(w, `{"errCode":0}`)
			return
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			json.NewDecoder(r.Body).Decode(&req)
			resp := &pb.StateResponse{}
			if req.IncludeSelf {
				resp.States = append(resp.States, &pb.State{ConversationID: "n_u_u", Known: true, StateToken: "self"})
			}
			for _, id := range req.ConversationIDs {
				state := &pb.State{ConversationID: id, Known: true, StateToken: "chat-state", MinSeq: 1, MaxSeq: 1, UnreadCount: 1}
				if req.IncludeDetails {
					state.Conversation = &conversation.Conversation{ConversationID: id, OwnerUserID: "u", UserID: "peer", ConversationType: 1}
				}
				resp.States = append(resp.States, state)
			}
			data = resp
		case "/msg/sync_messages_v2":
			data = &pb.PullResponse{State: &pb.State{ConversationID: "new-chat", Known: true, StateToken: "chat-state", MinSeq: 1, MaxSeq: 1, UnreadCount: 1}, Messages: []*sdkws.MsgData{{Seq: 1, ClientMsgID: "one"}}}
		case "/conversation/sync_changes_v2":
			if httpFailure != 0 {
				w.WriteHeader(httpFailure)
				return
			}
			var req pb.ChangeRequest
			json.NewDecoder(r.Body).Decode(&req)
			changeRequests = append(changeRequests, &req)
			if req.Cursor != "opaque-baseline" && req.Cursor != "opaque-watermark" && req.Cursor != "opaque-event" {
				t.Errorf("lost cursor %q", req.Cursor)
			}
			data = &pb.ChangeResponse{NextCursor: "opaque-watermark", UntilCursor: "opaque-watermark", CaughtUp: true, SyncDomainID: "test-domain"}
			if changed {
				if req.Cursor == "opaque-watermark" {
					data = &pb.ChangeResponse{Changes: []*pb.Change{{EventCursor: "event", StreamKind: "chat", ConversationID: "new-chat"}, {EventCursor: "event-duplicate", StreamKind: "chat", ConversationID: "new-chat"}}, NextCursor: "opaque-event", UntilCursor: "fixed-end", HasMore: true, SyncDomainID: "test-domain"}
				} else {
					data = &pb.ChangeResponse{NextCursor: "fixed-end", UntilCursor: "fixed-end", CaughtUp: true, SyncDomainID: "test-domain"}
				}
			}
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	publishes := 0
	cachedDisplays := 0
	hooks := syncv2.CoordinatorHooks{
		PrepareCached:  func(context.Context) error { cachedDisplays++; return nil },
		PrepareRelated: func(context.Context) error { return nil },
		ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
			return &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1}, nil
		},
		Commit: func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, _ []*sdkws.MsgData) error {
			return database.CommitConversationSyncV2Messages(ctx, p.Epoch, p, nil)
		},
		Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error {
			publishes++
			if failPublish {
				return errors.New("projection write failed")
			}
			return nil
		},
	}
	interrupted := hooks
	interrupted.PrepareRelated = func(context.Context) error { return errors.New("process interrupted after details commit") }
	if _, err = client.Synchronize(ctx, "cold-interrupted", "u", database, interrupted); err == nil {
		t.Fatal("expected interruption")
	}
	pending, err := database.GetConversationSyncV2(ctx)
	if err != nil || pending.Stage != "messages" || pending.RecoveryBaselineReady || pending.ChangeCursor == "" {
		t.Fatalf("missing resumable baseline: %+v %v", pending, err)
	}
	if cachedDisplays != 0 {
		t.Fatal("cold account was presented as cached")
	}
	database.Close(ctx)
	database, err = db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts = map[string]int{}
	if _, err = client.Synchronize(ctx, "resume-interrupted-baseline", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if counts["/conversation/sync_snapshot"] != 0 || cachedDisplays != 1 {
		t.Fatalf("interrupted baseline started over: snapshots=%d cachedDisplays=%d", counts["/conversation/sync_snapshot"], cachedDisplays)
	}
	cachedDisplays = 0
	database.Close(ctx)
	database, err = db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts = map[string]int{}
	publishes = 0
	if _, err = client.Synchronize(ctx, "warm", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if cachedDisplays != 1 {
		t.Fatal("validated persisted cache was not released")
	}
	if counts["/conversation/sync_changes_v2"] != 1 || counts["/conversation/sync_snapshot"] != 0 || counts["/msg/sync_seqs_v2"] != 0 || publishes != 0 {
		t.Fatalf("idle recovery scanned old data: calls=%v publishes=%d", counts, publishes)
	}
	for _, status := range []int{429, 503} {
		httpFailure = status
		before, _ := database.GetConversationSyncV2(ctx)
		snapshots := counts["/conversation/sync_snapshot"]
		if _, err = client.Synchronize(ctx, "temporary-error", "u", database, hooks); err == nil || syncv2.RecoveryFor(err) != syncv2.Retry {
			t.Fatalf("temporary failure not retryable: %v", err)
		}
		after, _ := database.GetConversationSyncV2(ctx)
		if before.ChangeCursor != after.ChangeCursor || counts["/conversation/sync_snapshot"] != snapshots {
			t.Fatal("temporary failure replaced baseline")
		}
	}
	httpFailure = 0
	stale, _ := database.GetConversationSyncV2(ctx)
	if _, err = client.Synchronize(ctx, "idle-again", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if _, err = database.CommitConversationSyncV2ChangeCursor(ctx, stale, "old-cursor", "", "ready"); err == nil {
		t.Fatal("stale writer moved cursor backwards")
	}
	changed = true
	failPublish = true
	changeRequests = nil
	if _, err = client.Synchronize(ctx, "fail-projection", "u", database, hooks); err == nil {
		t.Fatal("expected projection failure")
	}
	cp, err := database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp.ChangeCursor != "opaque-watermark" || cp.ChangeUntilCursor != "fixed-end" {
		t.Fatalf("advanced cursor before projection: %s %s", cp.ChangeCursor, cp.ChangeUntilCursor)
	}
	database.Close(ctx)
	database, err = db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	failPublish = false
	changeRequests = nil
	if _, err = client.Synchronize(ctx, "retry-page", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if len(changeRequests) != 2 || changeRequests[0].Cursor != "opaque-watermark" || changeRequests[0].UntilCursor != "fixed-end" || changeRequests[1].UntilCursor != "fixed-end" {
		t.Fatalf("lost bounded replay: %+v", changeRequests)
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp.ChangeCursor != "fixed-end" || cp.ChangeUntilCursor != "" || cp.Stage != "ready" {
		t.Fatal("did not durably finish bounded recovery")
	}
	if len(cp.SelectedIDs) != 1 || cp.SelectedIDs[0] != "new-chat" {
		t.Fatalf("duplicate event changed selection: %v", cp.SelectedIDs)
	}

}
