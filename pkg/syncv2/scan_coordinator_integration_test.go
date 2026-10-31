//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestScanResumeAndBothBaselines(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"})
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	scans := map[string]int{}
	var changes []string
	failSecond := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 100, MaxPageSize: 100, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, ProgressiveScan: true, MaxScanPageSize: 100, IncrementalRecovery: true, IncrementalRecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", MaxChangePageSize: 100}
		case "/conversation/sync_scan":
			var q pb.ScanRequest
			json.NewDecoder(r.Body).Decode(&q)
			scans[q.Scope+":"+q.Cursor]++
			p := &pb.ScanResponse{Scope: q.Scope, Live: true, PolicyVersion: syncv2.PolicyVersion, VersionID: "version", Version: 1, ExpiresAt: time.Now().Add(time.Hour).Unix(), BaselineCursor: "base-" + q.Scope, IncludeSelf: q.Scope == "notifications" && q.Cursor == ""}
			if q.Cursor == "" {
				p.Cursor = "next-" + q.Scope
				p.HasMore = true
				if q.Scope == "chat" {
					p.ConversationIDs = []string{"si_u_v"}
					p.Scanned = 1
				}
			}
			data = p
		case "/msg/sync_seqs_v2":
			var q pb.StateRequest
			json.NewDecoder(r.Body).Decode(&q)
			p := &pb.StateResponse{}
			for _, id := range q.ConversationIDs {
				state := &pb.State{ConversationID: id, Known: true, StateToken: "state"}
				if q.Notifications {
					state.ConversationID = "n_u_v"
				} else {
					state.Conversation = &conversation.Conversation{OwnerUserID: "u", ConversationID: id, UserID: "v", ConversationType: 1}
				}
				p.States = append(p.States, state)
			}
			if q.IncludeSelf {
				p.States = []*pb.State{{ConversationID: "n_u_u", Known: true, StateToken: "state", MinSeq: 0, MaxSeq: 0}}
			}
			data = p
		case "/conversation/sync_changes_v2":
			var q pb.ChangeRequest
			json.NewDecoder(r.Body).Decode(&q)
			changes = append(changes, q.Cursor)
			if q.Cursor == "base-notifications" && failSecond {
				failSecond = false
				w.WriteHeader(400)
				return
			}
			data = &pb.ChangeResponse{NextCursor: "done-" + q.Cursor, UntilCursor: "until", CaughtUp: true, SyncDomainID: "domain"}
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	interrupted := errors.New("interrupt after first pages")
	publishFailure := errors.New("publish failed")
	failPublish := true
	hooks := syncv2.CoordinatorHooks{
		PrepareRelated: func(context.Context) error { return nil },
		ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
			t.Error("unexpected metadata read")
			return nil, nil
		},
		Commit: func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error { return nil },
		Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error {
			if failPublish {
				failPublish = false
				return publishFailure
			}
			return nil
		},
		PrepareInitial: func(context.Context, *syncv2.Round) error { return nil },
		Background:     func(context.Context) error { return interrupted },
	}
	_, err = client.Synchronize(ctx, "publish-failure", "u", database, hooks)
	if !errors.Is(err, publishFailure) {
		t.Fatalf("expected publish failure: %v", err)
	}
	failed, err := database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failed.ScanTask.Chat.Cursor != "" || failed.ScanTask.Chat.Page == nil {
		t.Fatal("cursor advanced after publish failure")
	}
	_, err = client.Synchronize(ctx, "op", "u", database, hooks)
	if !errors.Is(err, interrupted) {
		t.Fatalf("expected interruption, got %v", err)
	}
	cp, err := database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp.ScanTask == nil || !cp.ScanTask.InitialReady || cp.ScanTask.Chat.Cursor != "next-chat" {
		t.Fatalf("checkpoint not persisted: %+v", cp.ScanTask)
	}
	hooks.Background = func(context.Context) error { return nil }
	_, err = client.Synchronize(ctx, "resume", "u", database, hooks)
	if err == nil {
		t.Fatal("expected second baseline failure")
	}
	result, err := client.Synchronize(ctx, "retry", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if result.Checkpoint.ScanTask != nil || result.Checkpoint.Stage != "ready" {
		t.Fatal("scan not complete")
	}
	if len(changes) != 3 || changes[0] != "base-chat" || changes[1] != "base-notifications" {
		t.Fatalf("baselines %v", changes)
	}
	for key, n := range scans {
		if n != 1 {
			t.Fatalf("repeated committed page %s: %d", key, n)
		}
	}
}

func TestScanCapabilityDowngradeRebuildsInsteadOfResumingPartialSnapshot(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"})
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", PolicyVersion: syncv2.PolicyVersion, Stage: "snapshot", VersionID: "v", ScanTask: &model_struct.ConversationSyncV2ScanTask{PageSize: 100}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	snapshots, metadata := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/conversation/sync_capabilities" {
			json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 100, MaxPageSize: 100, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600}})
			return
		}
		if r.URL.Path == "/conversation/sync_snapshot" {
			snapshots++
			json.NewEncoder(w).Encode(map[string]any{"errCode": 9, "errMsg": "test-stop-after-snapshot"})
			return
		}
		t.Errorf("unexpected route %s", r.URL.Path)
		w.WriteHeader(500)
	}))
	defer server.Close()
	hooks := syncv2.CoordinatorHooks{PrepareRelated: func(context.Context) error { return nil }, ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
		metadata++
		return nil, errors.New("partial snapshot reused")
	}, Commit: func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error { return nil }, Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error { return nil }}
	_, err = syncv2.NewClient(server.URL, "", server.Client()).Synchronize(ctx, "op", "u", database, hooks)
	if err == nil || snapshots != 1 || metadata != 0 {
		t.Fatalf("err=%v snapshots=%d metadata=%d", err, snapshots, metadata)
	}
}
