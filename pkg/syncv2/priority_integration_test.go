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
		t.Run(fmt.Sprintf("priority_%d", count), func(t *testing.T) { testPrioritySnapshotResume(t, count) })
	}
}

func testPrioritySnapshotResume(t *testing.T, priorityCount int32) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	token := strings.Repeat("a", 64)
	expiry := time.Now().Add(time.Hour).Unix()
	snapshots, states, guards, changes, releases := 0, 0, 0, 0, 0
	ready := false
	interrupt := true
	guardFirst := false
	changed := false
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
				if q.IncludeDetails {
					s.Conversation = &conversation.Conversation{ConversationID: id, ConversationType: 1, OwnerUserID: "u", UserID: id}
				}
				p.States = append(p.States, s)
			}
			data = p
		case "/conversation/sync_changes_v2":
			changes++
			data = &pb.ChangeResponse{SyncDomainID: "domain", NextCursor: "done", UntilCursor: "done", CaughtUp: true}
		case "/conversation/sync_snapshot_release":
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
	hooks := syncv2.CoordinatorHooks{PrepareRelated: func(context.Context) error { return nil }, ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
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
	if _, err = client.Synchronize(ctx, "rebuild", "u", database, hooks); err != nil {
		t.Fatal(err)
	}
	if guards != 2 || releases != 1 {
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
