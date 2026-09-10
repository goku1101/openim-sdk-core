//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/wrapperspb"
)

func TestSyncV2SetConversationPinPersistsWithoutActivatingOtherArchives(t *testing.T) {
	testSetConversationPin(t, 0, false)
}

func TestSyncV2SetConversationPinSurvivesConcurrentMetadataCommit(t *testing.T) {
	testSetConversationPin(t, 1, false)
}

func TestSyncV2SetConversationPinStopsAfterRepeatedMetadataConflict(t *testing.T) {
	testSetConversationPin(t, 2, false)
}

func TestSyncV2SetConversationPinRevealsHiddenHistory(t *testing.T) {
	testSetConversationPin(t, 0, true)
}

func testSetConversationPin(t *testing.T, conflicts int, hidden bool) {
	t.Helper()
	var database *db.DataBase
	mutationCount := 0
	stateCount := 0

	writeStarted := make(chan struct{}, 1)
	releaseWrite := make(chan struct{})
	var mu sync.Mutex
	pinned := false
	version := uint64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var data any
		switch r.URL.Path {
		case "/conversation/set_conversations":
			var req conversation.SetConversationsReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if req.Conversation == nil || req.Conversation.ConversationID != "sg_target" || len(req.UserIDs) != 1 || req.UserIDs[0] != "u" {
				t.Errorf("unexpected mutation: %+v", req)
				w.WriteHeader(400)
				return
			}
			writeStarted <- struct{}{}
			select {
			case <-releaseWrite:
			case <-r.Context().Done():
				return
			}
			mutationCount++
			pinned = req.Conversation.IsPinned.GetValue()
			version++
			data = map[string]any{}
		case "/conversation/get_incremental_conversations":
			data = &conversation.GetIncrementalConversationResp{VersionID: "v", Version: version, Update: []*conversation.Conversation{{ConversationID: "sg_target", IsPinned: pinned}}}
		case "/msg/sync_seqs_v2":
			stateCount++
			if stateCount <= conflicts {
				before, err := database.GetConversationSyncV2(r.Context())
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				next := *before
				next.StateEpochs, err = database.GetConversationSyncV2Epochs(r.Context(), before.SelectedIDs)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				if err = database.ApplyConversationSyncV2Metadata(r.Context(), before, &next, &pb.StateResponse{}, nil); err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
			}
			var req pb.StateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if len(req.ConversationIDs) != 1 || req.ConversationIDs[0] != "sg_target" || !req.IncludeDetails {
				t.Errorf("unrelated archive activated: %+v", req)
			}
			data = &pb.StateResponse{States: []*pb.State{{ConversationID: "sg_target", Known: true, MinSeq: 1, MaxSeq: 2, IsPinned: pinned, StateToken: "t", Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "sg_target", ConversationType: 3, GroupID: "target", IsPinned: pinned}}}}
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	defer close(releaseWrite)
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx := ccontext.WithInfo(base, config)
	var err error
	database, err = db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	for _, id := range []string{"sg_target", "sg_archive"} {
		if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, ConversationType: 3, GroupID: id[3:], DraftText: "preserve draft", DraftTextTime: 99, LatestMsgSendTime: 88}); err != nil {
			t.Fatal(err)
		}
	}
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_loaded", ConversationType: 3, GroupID: "loaded", MinSeq: 1, MaxSeq: 2, UnreadCount: 4, DraftText: "unrelated"}); err != nil {
		t.Fatal(err)
	}
	checkpoint := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"sg_loaded"}}
	if err = database.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint.Stage = "messages"
	if err = database.SaveConversationSyncV2(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	if !c.ConfigureConversationSyncV2(ctx) {
		t.Fatal("V2 configuration failed")
	}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	wantDraft := "preserve draft"
	if hidden {
		if err = database.BatchInsertMessageList(ctx, "sg_target", []*model_struct.LocalChatLog{{ClientMsgID: "saved", Seq: 2, SendTime: 88, Status: 2}}); err != nil {
			t.Fatal(err)
		}
		if err = c.HideConversation(ctx, "sg_target"); err != nil {
			t.Fatal(err)
		}
		wantDraft = ""
	}
	for _, want := range []bool{true, false} {
		operationID := "test-unpin"
		if want {
			operationID = "test-pin"
		}
		ctx := ccontext.WithOperationID(ctx, operationID)
		done := make(chan error, 1)
		go func() {
			done <- c.SetConversation(ctx, "sg_target", &conversation.ConversationReq{IsPinned: &wrapperspb.BoolValue{Value: want}})
		}()
		select {
		case <-writeStarted:
		case <-ctx.Done():
			t.Fatal("mutation never reached server")
		}
		// Read a page while the real mutation is waiting for its HTTP response.
		page, err := c.GetConversationListSplit(ctx, 0, 1)
		if err != nil || len(page) != 1 {
			t.Fatalf("page blocked or missing before write: %v %v", page, err)
		}
		beforeFirst := "sg_archive"
		if !want {
			beforeFirst = "sg_target"
		}
		if page[0].ConversationID != beforeFirst {
			t.Fatalf("unexpected pre-write page: %s", page[0].ConversationID)
		}
		releaseWrite <- struct{}{}
		err = <-done
		if conflicts > 1 {
			if !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
				t.Fatalf("repeated conflict must fail: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if mutationCount != 1 || stateCount != 2 {
				t.Fatalf("unbounded retry or repeated mutation: writes=%d states=%d", mutationCount, stateCount)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		page, err = c.GetConversationListSplit(ctx, 0, 1)
		if err != nil || len(page) != 1 {
			t.Fatalf("missing committed page: %v %v", page, err)
		}
		afterFirst := "sg_archive"
		if want {
			afterFirst = "sg_target"
		}
		if page[0].ConversationID != afterFirst {
			t.Fatalf("committed sort not visible: %s", page[0].ConversationID)
		}
		second, err := c.GetConversationListSplit(ctx, 1, 1)
		if err != nil || len(second) != 1 || second[0].ConversationID == page[0].ConversationID {
			t.Fatalf("committed pages duplicate or missing: %v %v", second, err)
		}
		got, err := database.GetConversation(ctx, "sg_target")
		if err != nil {
			t.Fatal(err)
		}
		if got.IsPinned != want || got.DraftText != wantDraft {
			t.Fatalf("pin/draft mismatch: %+v", got)
		}
		if hidden {
			visibility, err := database.GetConversationSyncV2Visibility(ctx, "sg_target")
			if err != nil || visibility.Hidden || got.LatestMsgSendTime != 88 || got.LatestMsg == "" {
				t.Fatalf("pin did not reveal hidden history: visibility=%+v row=%+v err=%v", visibility, got, err)
			}
		}
		current, err := database.GetConversationSyncV2(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(current.SelectedIDs) != 2 || current.SelectedIDs[0] != "sg_loaded" || current.SelectedIDs[1] != "sg_target" {
			t.Fatalf("selection changed unexpectedly: %v", current.SelectedIDs)
		}
	}
	untouched, err := database.GetConversation(ctx, "sg_loaded")
	if err != nil || untouched.UnreadCount != 4 || untouched.DraftText != "unrelated" {
		t.Fatalf("unrelated state lost: %+v %v", untouched, err)
	}
	epochs, err := database.GetConversationSyncV2Epochs(ctx, []string{"sg_loaded"})
	if err != nil || epochs["sg_loaded"] != uint64(conflicts) {
		t.Fatalf("mutation invalidated unrelated stream: %v %v", epochs, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if mutationCount != 2 {
		t.Fatalf("server mutation replayed: %d", mutationCount)
	}
	expectedStates := 2
	if conflicts > 0 {
		expectedStates++
	}
	if stateCount != expectedStates {
		t.Fatalf("unexpected state refresh count: %d", stateCount)
	}

}
