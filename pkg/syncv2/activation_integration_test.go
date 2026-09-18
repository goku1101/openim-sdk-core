//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/conversation"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestActivationRefreshesOnlyTargetAndPreservesOtherProgress(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	before := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", VersionID: "v", Version: 8, SelectedIDs: []string{"loaded", "pending", "unavailable"}, NotificationIDs: []string{"loaded", "pending", "unavailable", "target"}, PendingStateIDs: []string{"pending"}, UnavailableIDs: []string{"unavailable"}}
	if err = database.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	before.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, before); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "target", DraftText: "keep", ShowName: "local", UnreadCount: 50}); err != nil {
		t.Fatal(err)
	}
	state := &pb.State{ConversationID: "target", Known: true, StateToken: "t", MinSeq: 5, MaxSeq: 20, UnreadCount: 3, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "target", UserID: "peer", MinSeq: 1, MaxSeq: 200}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req pb.StateRequest
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if !req.IncludeDetails || req.Notifications || !reflect.DeepEqual(req.ConversationIDs, []string{"target"}) {
			t.Errorf("activation expanded query: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{state}}})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	got, err := client.ActivateConversation(ctx, "op", "target", &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.SelectedIDs, model_struct.StringArray{"loaded", "pending", "unavailable", "target"}) || !reflect.DeepEqual(got.NotificationIDs, before.NotificationIDs) || !reflect.DeepEqual(got.PendingStateIDs, before.PendingStateIDs) || !reflect.DeepEqual(got.UnavailableIDs, before.UnavailableIDs) || got.Version != 8 || got.Revision != 1 {
		t.Fatalf("activation altered unrelated progress: %+v", got)
	}
	row, err := database.GetConversation(ctx, "target")
	if err != nil || row.DraftText != "keep" || row.ShowName != "local" || row.MinSeq != 5 || row.MaxSeq != 20 || row.UnreadCount != 3 {
		t.Fatalf("incorrect activated details: %+v %v", row, err)
	}

	if err = database.CommitConversationSyncV2Messages(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "target", ParentID: "target", StateToken: "old", MinSeq: 1, ScannedSeq: 4}, []*model_struct.LocalChatLog{{ClientMsgID: "late", Seq: 4}}); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("pre-activation pull accepted after new bounds: %v", err)
	}
	if err = database.ActivateConversationSyncV2(ctx, before, 0, state); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("stale checkpoint accepted: %v", err)
	}
	if err = database.InvalidateConversationSyncV2Stream(ctx, "target"); err != nil {
		t.Fatal(err)
	}
	if err = database.ActivateConversationSyncV2(ctx, got, 0, state); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("stale boundary accepted: %v", err)
	}
	after, err := database.GetConversationSyncV2(ctx)
	if err != nil || after.Revision != got.Revision {
		t.Fatalf("failed activation advanced checkpoint: %+v %v", after, err)
	}
}

func TestActivationDoesNotPromoteUnconfirmedOrUnavailableChat(t *testing.T) {
	for _, scenario := range []string{"unknown", "missing-detail", "unavailable", "wrong-owner", "http-error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close(ctx)
			cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", NotificationIDs: []string{"target"}}
			if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Stage = "ready"
			if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
				t.Fatal(err)
			}
			if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "target", DraftText: "draft", UnreadCount: 7, MinSeq: 2, MaxSeq: 9}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario == "http-error" {
					w.WriteHeader(500)
					return
				}
				state := &pb.State{ConversationID: "target", Known: true, StateToken: "t", Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "target"}}
				response := &pb.StateResponse{States: []*pb.State{state}}
				switch scenario {
				case "unknown":
					state.Known = false
				case "missing-detail":
					state.Conversation = nil
				case "wrong-owner":
					state.Conversation.OwnerUserID = "other"
				case "unavailable":
					response.States = nil
					response.UnavailableIDs = []string{"target"}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
			}))
			defer server.Close()
			client := syncv2.NewClient(server.URL, "", server.Client())
			if _, err = client.ActivateConversation(ctx, "op", "target", &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database); err == nil {
				t.Fatal("failed activation reported success")
			}
			after, err := database.GetConversationSyncV2(ctx)
			if err != nil || after.Revision != 0 || len(after.SelectedIDs) != 0 || after.Stage != "ready" {
				t.Fatalf("failed activation changed selection: %+v %v", after, err)
			}
			row, err := database.GetConversation(ctx, "target")
			if err != nil || row.DraftText != "draft" || row.UnreadCount != 7 || row.MinSeq != 2 || row.MaxSeq != 9 {
				t.Fatalf("failed activation erased local data: %+v %v", row, err)
			}
		})
	}
}

func TestActivationRetriesAfterConcurrentStreamInvalidation(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot"}
	if err = database.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "target", UserID: "peer"}); err != nil {
		t.Fatal(err)
	}
	state := &pb.State{ConversationID: "target", Known: true, StateToken: "fresh", MinSeq: 1, MaxSeq: 2, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "target", UserID: "peer"}}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{state}}})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	result := make(chan error, 1)
	go func() {
		_, err := client.ActivateConversation(ctx, "op", "target", &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database)
		result <- err
	}()
	<-started
	if err = database.InvalidateConversationSyncV2Stream(ctx, "target"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = <-result; err != nil {
		t.Fatalf("activation did not recover from concurrent invalidation: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected one retry after stale stream response, got %d calls", calls.Load())
	}
	stream, err := database.GetConversationSyncV2Stream(ctx, "target")
	if err != nil {
		t.Fatal(err)
	}
	if stream.StateToken != "" || stream.Epoch != 2 {
		t.Fatalf("retry did not preserve the invalidated stream epoch: %+v", stream)
	}
}

func TestActivationPreservesUnrelatedCheckpointAdvancementDuringEveryRead(t *testing.T) {
	ctx := context.Background()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", VersionID: "v", SelectedIDs: []string{"other"}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		current, e := database.GetConversationSyncV2(ctx)
		if e != nil {
			t.Error(e)
			w.WriteHeader(500)
			return
		}
		next := *current
		next.Stage = "messages"
		next.Version++
		if e = database.ApplyConversationSyncV2MetadataSubset(ctx, current, &next, &pb.StateResponse{}, nil, nil); e != nil {
			t.Error(e)
			w.WriteHeader(500)
			return
		}
		state := &pb.State{ConversationID: "target", Known: true, StateToken: "fresh", MinSeq: 1, MaxSeq: 0, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "target", UserID: "peer", ConversationType: 1}}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{state}}})
	}))
	defer server.Close()
	client := syncv2.NewClient(server.URL, "", server.Client())
	got, err := client.ActivateConversation(ctx, "op", "target", &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}, database)
	if err != nil {
		t.Fatalf("unrelated background progress blocked activation: %v", err)
	}
	if calls.Load() != 1 || got.Version != 1 || got.Revision != 2 || !reflect.DeepEqual(got.SelectedIDs, model_struct.StringArray{"other", "target"}) {
		t.Fatalf("incorrect checkpoint merge: calls=%d checkpoint=%+v", calls.Load(), got)
	}
}
