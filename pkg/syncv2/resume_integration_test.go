//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestWarmSyncRetainsCheckpointAndOnlyPullsNewSequences(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	snapshots := 0
	newlyUnread := false
	stateOnlyReads := 0
	detailReads := 0
	tail := int64(1)
	var pulled []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := func(id string) *pb.State {
			return &pb.State{ConversationID: id, Known: true, StateToken: "t", MinSeq: 1, MaxSeq: tail, Conversation: &conversation.Conversation{ConversationID: id, OwnerUserID: "u", UserID: "v", ConversationType: 1}}
		}
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600}
		case "/conversation/sync_snapshot":
			snapshots++
			var req pb.SnapshotRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			s := &pb.SnapshotResponse{SnapshotID: "s", Scope: req.Scope, IncludeSelf: req.Scope == "notifications", Mode: "full", PolicyVersion: syncv2.PolicyVersion, VersionID: "v", Version: 1}
			if req.Scope == "chat" {
				s.ConversationIDs = []string{"c"}
				if newlyUnread {
					s.ConversationIDs = append(s.ConversationIDs, "new-unread")
				}
				s.Total = int64(len(s.ConversationIDs))
			}
			data = s
		case "/conversation/sync_snapshot_release":
			fmt.Fprint(w, `{"errCode":0}`)
			return
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.IncludeDetails {
				detailReads += len(req.ConversationIDs)
			}
			if !req.Notifications && !req.IncludeDetails {
				stateOnlyReads++
			}
			s := &pb.StateResponse{}
			if req.IncludeSelf {
				s.States = append(s.States, &pb.State{ConversationID: "n_u_u", Known: true, StateToken: "self"})
			}
			for _, id := range req.ConversationIDs {
				row := state(id)
				if !req.IncludeDetails {
					row.Conversation = nil
				}
				s.States = append(s.States, row)
			}
			data = s
		case "/msg/sync_messages_v2":
			var req pb.PullRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			s := &pb.PullResponse{State: state(req.ConversationID)}
			for _, seq := range req.Seqs {
				pulled = append(pulled, seq)
				s.Messages = append(s.Messages, &sdkws.MsgData{Seq: seq, ClientMsgID: fmt.Sprint(seq)})
			}
			data = s
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	initial := 0
	metadataReads := 0
	full := false
	hooks := syncv2.CoordinatorHooks{
		PrepareInitial: func(context.Context, *syncv2.Round) error { initial++; return nil },
		PrepareRelated: func(context.Context) error { return nil },
		ReadMetadata: func(_ context.Context, version uint64, id string) (*conversation.GetIncrementalConversationResp, error) {
			metadataReads++
			if id != "v" || version != 1 {
				t.Errorf("lost metadata cursor")
			}
			return &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1, Full: full}, nil
		},
		Commit: func(ctx context.Context, p *model_struct.LocalConversationSyncV2Stream, _ []*sdkws.MsgData) error {
			return database.CommitConversationSyncV2Messages(ctx, p.Epoch, p, nil)
		},
		Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error { return nil },
	}
	first, err := client.Synchronize(ctx, "first", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	before := snapshots
	metadataReads = 0
	stateOnlyReads = 0
	detailReads = 0
	pulled = nil
	tail = 2
	second, err := client.Synchronize(ctx, "second", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if stateOnlyReads != 1 || detailReads != 0 {
		t.Fatalf("warm sync did not reuse unchanged details: stateReads=%d details=%d", stateOnlyReads, detailReads)
	}
	if metadataReads != 2 {
		t.Fatalf("warm recovery repeated full stream pass: metadata reads=%d", metadataReads)
	}
	if snapshots != before+2 || initial != 1 || second.Checkpoint.Generation != first.Checkpoint.Generation {
		t.Fatalf("warm login rebuilt snapshot: requests=%d->%d initial=%d", before, snapshots, initial)
	}
	if len(pulled) != 1 || pulled[0] != 2 {
		t.Fatalf("warm login replayed cached sequences: %v", pulled)
	}
	beforeStream, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	idleResult, err := client.SyncStreams(ctx, "unchanged", &pb.StateRequest{UserID: "u", ConversationIDs: []string{"c"}}, second.Capabilities, database, hooks.Commit)
	if err != nil || idleResult.Completed != 1 {
		t.Fatalf("idle sync failed: %v", err)
	}
	afterStream, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || afterStream.Epoch != beforeStream.Epoch {
		t.Fatal("unchanged stream still wrote a new epoch")
	}
	// Previously discovered holes remain durable without blocking new messages.
	afterStream.PendingSeqs = []int64{1}
	if err = database.CommitConversationSyncV2Messages(ctx, afterStream.Epoch, afterStream, nil); err != nil {
		t.Fatal(err)
	}
	pendingIdle, err := client.Synchronize(ctx, "unchanged-pending", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	preserved, err := database.GetConversationSyncV2Stream(ctx, "c")
	if err != nil || preserved.Epoch != afterStream.Epoch || len(pendingIdle.Chats.PendingIDs) != 1 {
		t.Fatal("unchanged historical holes caused a new scan or lost retry state")
	}
	pulled = nil
	tail = 3
	pendingResult, err := client.Synchronize(ctx, "pending-resume", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulled) != 1 || pulled[0] != 3 {
		t.Fatalf("startup retried old holes instead of only new messages: %v", pulled)
	}
	jobs, err := syncv2.PendingRetryTasks(pendingResult, 1)
	if err != nil || len(jobs) == 0 {
		t.Fatal("deferred holes lost their retry task")
	}
	pulled = nil
	if _, err = client.SyncStream(ctx, "background-retry", &pb.PullRequest{UserID: "u", ConversationID: "c"}, second.Capabilities, database, hooks.Commit); err != nil {
		t.Fatal(err)
	}
	if len(pulled) != 1 || pulled[0] != 1 {
		t.Fatalf("background retry stopped filling holes: %v", pulled)
	}

	// Message arrival does not have to advance the metadata version. Re-read the
	// selection to discover previously filtered, now-unread chats without replaying c.
	newlyUnread = true
	pulled = nil
	unreadResult, err := client.Synchronize(ctx, "offline-unread", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if unreadResult.Checkpoint.Generation != second.Checkpoint.Generation || len(unreadResult.Checkpoint.SelectedIDs) != 2 || len(pulled) != 3 {
		t.Fatalf("warm recovery missed offline unread chat or replayed cached stream: ids=%v pulled=%v", unreadResult.Checkpoint.SelectedIDs, pulled)
	}
	// The server can explicitly reject the old baseline. That must still rebuild.
	full = true
	hooks.ReadMetadata = func(_ context.Context, _ uint64, _ string) (*conversation.GetIncrementalConversationResp, error) {
		f := full
		full = false
		return &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1, Full: f}, nil
	}
	third, err := client.Synchronize(ctx, "reset", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if snapshots == before || third.Checkpoint.Generation == second.Checkpoint.Generation {
		t.Fatal("server full-sync request did not rebuild snapshot")
	}
	hooks.ForceSnapshot = true
	before = snapshots
	forced, err := client.Synchronize(ctx, "forced", "u", database, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if snapshots == before || forced.Checkpoint.Generation == third.Checkpoint.Generation {
		t.Fatal("classified snapshot error reused old checkpoint")
	}
	hooks.ForceSnapshot = false
	raced := &invalidatePreparedStateStore{DataBase: database, armed: true}
	beforePulls := len(pulled)
	if _, err = client.Synchronize(ctx, "permission-race", "u", raced, hooks); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("prepared state ignored intervening invalidation: %v", err)
	}
	if len(pulled) != beforePulls {
		t.Fatal("stale prepared permission pulled messages")
	}

}

type invalidatePreparedStateStore struct {
	*db.DataBase
	armed bool
}

func (s *invalidatePreparedStateStore) UnchangedConversationSyncV2Streams(ctx context.Context, epochs map[string]uint64, states []*pb.State, deferPending bool) (map[string]bool, error) {
	if s.armed {
		for _, state := range states {
			if state.ConversationID == "c" {
				s.armed = false
				if err := s.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
					return nil, err
				}
				break
			}
		}
	}
	return s.DataBase.UnchangedConversationSyncV2Streams(ctx, epochs, states, deferPending)
}
