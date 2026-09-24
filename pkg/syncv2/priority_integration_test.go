//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestPrioritySnapshotResumesBeforeReadingOldTailAndWarmStartUsesChanges(t *testing.T) {
	for _, count := range []int32{0, 1} {
		t.Run(fmt.Sprintf("priority_%d", count), func(t *testing.T) { testPrioritySnapshotResume(t, count, false) })
	}
}

func TestPrioritySnapshotPersistsPendingNotificationOutsideCommittedPrefix(t *testing.T) {
	testPrioritySnapshotResume(t, 1, true)
}

func testPrioritySnapshotResume(t *testing.T, priorityCount int32, pendingTail bool) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	token := strings.Repeat("a", 64)
	expiry := time.Now().Add(time.Hour).Unix()
	snapshots, states, guards, changes, releases := 0, 0, 0, 0, 0
	pendingJobs := 0
	ready := false
	interrupt := true
	guardFirst := false
	changed := false
	failChanges := false
	failRelease := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 3600, IncrementalRecovery: true, IncrementalRecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", MaxChangePageSize: 500, NotificationPriorityPrefix: true, MaxNotificationPriority: 500, DirectoryTokenGuard: true}
		case "/conversation/sync_snapshot":
			snapshots++
			var q pb.SnapshotRequest
			json.NewDecoder(r.Body).Decode(&q)
			p := &pb.SnapshotResponse{SnapshotID: "s", Scope: q.Scope, PolicyVersion: syncv2.PolicyVersion, Mode: "full", VersionID: "v", Version: 1, DirectoryToken: token, ExpiresAt: expiry, BaselineCursor: "base"}
			if q.Scope == "notifications" {
				p.Total = 3
				p.NotificationPriorityCount = priorityCount
				p.ConversationIDs = []string{"si_a_u", "si_b_u"}
				p.Cursor = "tail"
				p.HasMore = true
				p.IncludeSelf = q.Cursor == ""
				if q.Cursor == "tail" {
					if !ready {
						t.Error("tail fetched before priority ready")
					}
					p.ConversationIDs = []string{"si_c_u"}
					p.Cursor = ""
					p.HasMore = false
					if interrupt {
						w.WriteHeader(503)
						return
					}
				}
			} else {
				p.ConversationIDs = []string{"si_a_u", "si_b_u", "si_c_u"}
				p.Total = 3
			}
			data = p
		case "/msg/sync_seqs_v2":
			states++
			var q pb.StateRequest
			json.NewDecoder(r.Body).Decode(&q)
			if !q.Notifications && !q.IncludeDetails {
				t.Error("chat state fetched again after committed details")
			}
			if guardFirst && q.ExpectedDirectoryToken == "" {
				t.Error("old task read before directory guard")
			}
			guardFirst = false
			if q.ExpectedDirectoryToken != "" {
				guards++
				if q.ExpectedDirectoryToken != token {
					t.Error("wrong guard")
				}
				if changed {
					json.NewEncoder(w).Encode(map[string]any{"errCode": 9, "errMsg": "SYNC_V2_DIRECTORY_CHANGED"})
					return
				}
			}
			p := &pb.StateResponse{}
			if q.IncludeSelf {
				p.States = append(p.States, &pb.State{ConversationID: "n_u_u", Known: true, MinSeq: 1, StateToken: "s"})
			}
			for _, id := range q.ConversationIDs {
				sid := id
				if q.Notifications {
					sid = "n" + id[strings.IndexByte(id, '_'):]
				}
				s := &pb.State{ConversationID: sid, Known: true, MinSeq: 1, StateToken: "t"}
				if pendingTail && q.Notifications && id == "si_c_u" {
					s.Known = false
				}
				if q.IncludeDetails {
					s.Conversation = &conversation.Conversation{ConversationID: id, ConversationType: 1, OwnerUserID: "u", UserID: id}
				}
				p.States = append(p.States, s)
			}
			data = p
		case "/conversation/sync_changes_v2":
			changes++
			if failChanges {
				w.WriteHeader(503)
				return
			}
			data = &pb.ChangeResponse{SyncDomainID: "domain", NextCursor: "done", UntilCursor: "done", CaughtUp: true}
		case "/conversation/sync_snapshot_release":
			if failRelease {
				w.WriteHeader(503)
				return
			}
			releases++
			fmt.Fprint(w, `{"errCode":0}`)
			return
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	hooks := syncv2.CoordinatorHooks{PersistPending: func(ctx context.Context, result *syncv2.SyncResult) error {
		jobs, err := syncv2.PendingRetryTasks(result, 1000)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if err := database.PutConversationSyncV2Retry(ctx, job); err != nil {
				return err
			}
			pendingJobs++
		}
		return nil
	}, PrepareRelated: func(context.Context) error { return nil }, ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
		t.Error("legacy metadata read")
		return nil, fmt.Errorf("unexpected")
	}, PrepareInitial: func(_ context.Context, r *syncv2.Round) error {
		if !r.PriorityComplete {
			t.Error("missing priority proof")
		}
		ready = true
		// A foreground metadata event advances the live version while the
		// immutable snapshot must still accept later pages from version 1.
		current, e := database.GetConversationSyncV2(ctx)
		if e != nil {
			return e
		}
		current.Version = 9
		return database.SaveConversationSyncV2(ctx, current)
	}, Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error { return nil }, Commit: func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, _ []*sdkws.MsgData) error {
		return database.CommitConversationSyncV2Messages(ctx, p.Epoch, p, nil)
	}}
	if _, err = client.Synchronize(ctx, "cold", "u", database, hooks); err == nil {
		t.Fatal("expected interrupted tail")
	}
	t.Logf("interruption: %v", err)
	cp, err := database.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || cp.NotificationTask == nil || !cp.NotificationTask.PriorityReady || cp.RecoveryBaselineReady || releases != 0 {
		t.Fatalf("lost resumable task: ready=%v releases=%d", ready, releases)
	}
	// Resume an unchanged directory, retaining the committed prefix even while
	// the tail endpoint is still unavailable.
	guardFirst = true
	savedSnapshots := snapshots
	if _, err = client.Synchronize(ctx, "resume", "u", database, hooks); err == nil {
		t.Fatal("expected tail failure")
	}
	if guards != 1 || snapshots != savedSnapshots+1 {
		t.Fatalf("resume restarted snapshot: guards=%d new requests=%d", guards, snapshots-savedSnapshots)
	}
	guardFirst = true
	changed = true
	if _, err = client.Synchronize(ctx, "changed", "u", database, hooks); syncv2.RecoveryFor(err) != syncv2.RebuildSnapshot {
		t.Fatalf("directory change: %v", err)
	}
	cp, _ = database.GetConversationSyncV2(ctx)
	if cp.NotificationTask.Status != "invalidated" {
		t.Fatal("old task still active")
	}
	changed = false
	interrupt = false
	// Stop after durable directory capture, then expire the remote snapshot.
	originalPublish := hooks.Publish
	hooks.Publish = func(ctx context.Context, cp *model_struct.LocalConversationSyncV2, progress func(int, int)) error {
		if cp.NotificationTask != nil && cp.NotificationTask.Directory != nil && cp.NotificationTask.Directory.Complete {
			return fmt.Errorf("simulated process interruption after directory capture")
		}
		return originalPublish(ctx, cp, progress)
	}
	if _, err = client.Synchronize(ctx, "capture", "u", database, hooks); err == nil {
		t.Fatal("expected processing interruption")
	} else if !strings.Contains(err.Error(), "simulated process interruption after directory capture") {
		t.Fatalf("unexpected processing failure: %v", err)
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil || cp.NotificationTask.Directory == nil || !cp.NotificationTask.Directory.Complete {
		t.Fatalf("directory not durable: %v", err)
	}
	cp.NotificationTask.ExpiresAt = time.Now().Add(-time.Minute).Unix()
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if pendingTail && pendingJobs == 0 {
		t.Fatal("pending notification did not become a durable retry")
	}
	capturedSnapshots := snapshots
	hooks.Publish = originalPublish
	failChanges = true
	if _, err = client.Synchronize(ctx, "rebuild-fail-changes", "u", database, hooks); err == nil {
		t.Fatal("expected changes interruption")
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil || !cp.RecoveryBaselineReady || cp.NotificationTask == nil {
		t.Fatalf("lost cleanup owner after changes failure: %v", err)
	}
	failChanges = false
	failRelease = true
	if _, err = client.Synchronize(ctx, "resume-fail-release", "u", database, hooks); err == nil {
		t.Fatal("expected release interruption")
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil || cp.NotificationTask == nil {
		t.Fatalf("lost cleanup owner after release failure: %v", err)
	}
	failRelease = false
	if _, err = client.Synchronize(ctx, "rebuild", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if snapshots != capturedSnapshots {
		t.Fatal("expired captured directory was downloaded again")
	}
	if guards != 3 || releases != 2 {
		t.Fatalf("guards=%d releases=%d", guards, releases)
	}
	beforeSnapshots, beforeStates, beforeChanges := snapshots, states, changes
	if _, err = client.Synchronize(ctx, "warm", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if snapshots != beforeSnapshots || states != beforeStates || changes != beforeChanges+1 {
		t.Fatalf("warm start rescanned: snapshots=%d states=%d changes=%d", snapshots-beforeSnapshots, states-beforeStates, changes-beforeChanges)
	}
}

func TestPriorityCreateResponseLossSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	operations := []string{}
	snapshots := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 3600, IncrementalRecovery: true, IncrementalRecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", MaxChangePageSize: 500, NotificationPriorityPrefix: true, MaxNotificationPriority: 500, DirectoryTokenGuard: true}})
		case "/conversation/sync_snapshot":
			op := r.Header.Get("operationID")
			operations = append(operations, op)
			if snapshots[op] == "" {
				snapshots[op] = fmt.Sprintf("s-%d", len(snapshots))
			}
			// The server has committed the snapshot, but the caller loses its response.
			if len(operations) == 1 {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.SnapshotResponse{SnapshotID: snapshots[op], Scope: "notifications", PolicyVersion: syncv2.PolicyVersion, Mode: "full", VersionID: "v", Version: 1, DirectoryToken: strings.Repeat("a", 64), ExpiresAt: time.Now().Add(time.Hour).Unix(), BaselineCursor: "base", IncludeSelf: true}})
		case "/msg/sync_seqs_v2":
			w.WriteHeader(503)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	hooks := syncv2.CoordinatorHooks{PrepareRelated: func(context.Context) error { return nil }, ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
		return nil, nil
	}, Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error { return nil }, Commit: func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error { return nil }}
	for i := 0; i < 3; i++ {
		database, err := db.NewDataBase(ctx, "u", directory, 0)
		if err != nil {
			t.Fatal(err)
		}
		client := syncv2.NewClient(server.URL, "", server.Client())
		_, err = client.Synchronize(ctx, fmt.Sprintf("login-%d", i), "u", database, hooks)
		database.Close(ctx)
		if err == nil {
			t.Fatal("expected network interruption")
		}
	}
	if len(operations) != 2 || operations[0] == "" || operations[0] != operations[1] || len(snapshots) != 1 {
		t.Fatalf("lost durable create identity across restart: requests=%d snapshots=%d sameOperation=%t", len(operations), len(snapshots), len(operations) > 1 && operations[0] == operations[1])
	}
}

func TestExplicitSnapshotCancellationRetainsFailedCleanup(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "generation", Stage: "snapshot", NotificationTask: &model_struct.ConversationSyncV2Task{CreateOperationID: "stable-create", Status: "creating"}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	failRelease := true
	creates, releases := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversation/sync_snapshot":
			creates++
			if r.Header.Get("operationID") != "stable-create" {
				t.Error("cancel did not recover original creation")
			}
			json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.SnapshotResponse{SnapshotID: "known-snapshot"}})
		case "/conversation/sync_snapshot_release":
			var request pb.ReleaseRequest
			json.NewDecoder(r.Body).Decode(&request)
			if request.SnapshotID != "known-snapshot" {
				t.Error("released wrong snapshot")
			}
			releases++
			if failRelease {
				w.WriteHeader(503)
				return
			}
			fmt.Fprint(w, `{"errCode":0}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	c := syncv2.NewClient(server.URL, "", server.Client())
	if err = c.AbandonSnapshot(ctx, "cancel", "u", database); err == nil {
		t.Fatal("expected interrupted cleanup")
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil || cp.NotificationTask.Status != "releasing" || cp.SnapshotID != "known-snapshot" {
		t.Fatalf("lost cleanup state: %v", err)
	}
	failRelease = false
	// A new runtime must finish release without creating another snapshot.
	c = syncv2.NewClient(server.URL, "", server.Client())
	if err = c.AbandonSnapshot(ctx, "cancel-retry", "u", database); err != nil {
		t.Fatal(err)
	}
	if err = c.AbandonSnapshot(ctx, "cancel-again", "u", database); err != nil {
		t.Fatal(err)
	}
	cp, err = database.GetConversationSyncV2(ctx)
	if err != nil || cp.NotificationTask != nil || cp.SnapshotID != "" || creates != 1 || releases != 2 {
		t.Fatalf("cleanup not idempotent: creates=%d releases=%d err=%v", creates, releases, err)
	}
}
