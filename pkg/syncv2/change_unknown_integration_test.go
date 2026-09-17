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
	"github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

func TestChangeUnknownStateIsDurableBeforeCursor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	d, err := db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { d.Close(ctx) }()
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", PolicyVersion: syncv2.PolicyVersion, Generation: "g", VersionID: "v", Stage: "ready", RecoveryBaselineReady: true, RecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", ChangeCursor: "before"}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	known := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, IncrementalRecovery: true, IncrementalRecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", MaxChangePageSize: 100}
		case "/conversation/sync_changes_v2":
			data = &pb.ChangeResponse{Changes: []*pb.Change{{EventCursor: "event", StreamKind: "chat", ConversationID: "si_new-empty"}}, NextCursor: "after", UntilCursor: "after", CaughtUp: true, SyncDomainID: "domain"}
		case "/msg/sync_seqs_v2":
			data = &pb.StateResponse{States: []*pb.State{{ConversationID: "si_new-empty", Known: known, MinSeq: 1, StateToken: "unconfirmed", Conversation: &conversation.Conversation{ConversationID: "si_new-empty", OwnerUserID: "u", UserID: "peer", ConversationType: 1}}}}
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	persistenceFails := true
	persisted := 0
	hooks := syncv2.CoordinatorHooks{
		PrepareRelated: func(context.Context) error { return nil },
		ReadMetadata: func(context.Context, uint64, string) (*conversation.GetIncrementalConversationResp, error) {
			t.Fatal("unexpected legacy metadata")
			return nil, nil
		},
		Commit: func(context.Context, *model_struct.LocalConversationSyncV2Stream, []*sdkws.MsgData) error {
			t.Fatal("unknown state downloaded messages")
			return nil
		},
		Publish: func(context.Context, *model_struct.LocalConversationSyncV2, func(int, int)) error { return nil },
		PersistPending: func(ctx context.Context, r *syncv2.SyncResult) error {
			if persistenceFails {
				return fmt.Errorf("injected durable write failure")
			}
			jobs, e := syncv2.PendingRetryTasks(r, 1000)
			if e != nil {
				return e
			}
			for _, j := range jobs {
				if e = d.PutConversationSyncV2Retry(ctx, j); e != nil {
					return e
				}
				persisted++
			}
			return nil
		},
	}
	if _, err = client.Synchronize(ctx, "fail-write", "u", d, hooks); err == nil {
		t.Fatal("cursor advanced without durable task")
	}
	before, _ := d.GetConversationSyncV2(ctx)
	if before.ChangeCursor != "before" {
		t.Fatal("failed persistence advanced cursor")
	}
	persistenceFails = false
	if _, err = client.Synchronize(ctx, "retry", "u", d, hooks); err != nil {
		t.Fatal(err)
	}
	if persisted == 0 {
		t.Fatal("no durable retry")
	}
	d.Close(ctx)
	d, err = db.NewDataBase(ctx, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	after, err := d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.ChangeCursor != "after" || len(after.PendingStateIDs) != 1 || after.PendingStateIDs[0] != "si_new-empty" {
		t.Fatalf("lost pending state or cursor: %+v", after)
	}
	job, err := d.NextConversationSyncV2Retry(ctx)
	if err != nil || job == nil {
		t.Fatalf("retry lost on reopen: %v", err)
	}
	// A metadata recheck must not discard another deferred pending target.
	after.PendingStateIDs = append(after.PendingStateIDs, "si_other")
	if err = d.SaveConversationSyncV2(ctx, after); err != nil {
		t.Fatal(err)
	}
	caps := &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	if _, err = client.RevalidatePendingState(ctx, "still-unknown", "si_new-empty", caps, d); !errors.Is(err, syncv2.ErrStatePending) {
		t.Fatalf("unknown state lost guard: %v", err)
	}
	known = true
	if _, err = client.RevalidatePendingState(ctx, "now-known", "si_new-empty", caps, d); err != nil {
		t.Fatal(err)
	}
	current, err := d.GetConversationSyncV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.PendingStateIDs) != 1 || current.PendingStateIDs[0] != "si_other" || len(current.SelectedIDs) != 0 || current.Stage != "ready" || current.ChangeCursor != "after" {
		t.Fatal("state recheck promoted history or lost checkpoint")
	}
	if err = d.AcknowledgeConversationSyncV2Retry(ctx, job); err != nil {
		t.Fatal(err)
	}
	if job, err = d.NextConversationSyncV2Retry(ctx); err != nil || job != nil {
		t.Fatal("completed state retry not acknowledged")
	}
}
