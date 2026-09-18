//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"testing"
)

type metadataBoundaryListener struct {
	syncV2Listener
	changed []string
}

func (l *metadataBoundaryListener) OnConversationChanged(s string) { l.changed = append(l.changed, s) }
func TestSyncV2MetadataPublishesClearedBoundaryWithoutAnotherMessage(t *testing.T) {
	testMetadataBoundaryNotification(t, false, false, false, false)
}
func TestSyncV2MetadataClearKeepsPendingLocalMessage(t *testing.T) {
	testMetadataBoundaryNotification(t, true, false, false, false)
}
func TestSyncV2MetadataRetriesNotificationAfterProjectionConflict(t *testing.T) {
	testMetadataBoundaryNotification(t, false, true, false, false)
}

type metadataProjectionConflict struct {
	conversationSyncV2Store
	injected bool
	calls    int
	failAt   int
}

func (s *metadataProjectionConflict) ApplyConversationSyncV2Projection(ctx context.Context, cp *model_struct.LocalConversationSyncV2, epochs map[string]uint64, rows []*model_struct.LocalConversation) error {
	s.calls++
	if !s.injected && (s.failAt == 0 || s.calls == s.failAt) {
		s.injected = true
		if err := s.InvalidateConversationSyncV2Stream(ctx, rows[0].ConversationID); err != nil {
			return err
		}
	}
	return s.conversationSyncV2Store.ApplyConversationSyncV2Projection(ctx, cp, epochs, rows)
}
func TestSyncV2MetadataNewlyPinnedArchiveUsesVisiblePreview(t *testing.T) {
	testMetadataBoundaryNotification(t, false, false, true, false)
}
func TestSyncV2MetadataNotificationKeepsChangedBoundaryAtSameVersion(t *testing.T) {
	testMetadataBoundaryNotification(t, false, false, false, true)
}
func TestSyncV2MetadataNotificationRetriesProjectionAtSameVersion(t *testing.T) {
	testMetadataBoundaryNotification(t, false, true, false, true)
}
func TestSyncV2MetadataNotificationActivatesNewPinWithoutRefreshingUnrelatedChat(t *testing.T) {
	testMetadataBoundaryNotification(t, false, false, true, true)
}
func testMetadataBoundaryNotification(t *testing.T, pending, conflict, newlyPinned, notification bool) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/conversation/get_incremental_conversations":
			delta := &conversation.GetIncrementalConversationResp{VersionID: "v", Version: 1}
			if newlyPinned {
				delta.Update = []*conversation.Conversation{{ConversationID: "sg_target", IsPinned: true}}
			}
			data = delta
		case "/msg/sync_seqs_v2":
			if notification {
				var req pb.StateRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if len(req.ConversationIDs) != 1 || req.ConversationIDs[0] != "sg_target" {
					t.Errorf("notification scanned unrelated selection: %v", req.ConversationIDs)
				}
			}
			data = &pb.StateResponse{States: []*pb.State{{ConversationID: "sg_target", Known: true, IsPinned: newlyPinned, MinSeq: 7, MaxSeq: 6, StateToken: "cleared", Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "sg_target", ConversationType: 3, GroupID: "target", IsPinned: newlyPinned}}}}
		default:
			t.Errorf("unexpected history/network request: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx := ccontext.WithInfo(context.Background(), config)
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_target", ShowName: "Target", ConversationType: 3, GroupID: "target", MinSeq: 6, MaxSeq: 6, LatestMsg: `{"clientMsgID":"old","seq":6}`, LatestMsgSendTime: 10, DraftText: "keep"}); err != nil {
		t.Fatal(err)
	}
	if pending {
		if err = database.BatchInsertMessageList(ctx, "sg_target", []*model_struct.LocalChatLog{{ClientMsgID: "pending", Seq: 0, SendTime: 20, Status: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", SnapshotID: "s", Stage: "snapshot", VersionID: "v", Version: 1, SelectedIDs: []string{"sg_target"}}
	if newlyPinned {
		cp.SelectedIDs = nil
	}
	if notification {
		cp.SelectedIDs = append(cp.SelectedIDs, "sg_untouched")
		cp.PendingStateIDs = []string{"sg_untouched"}
		if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_untouched", MinSeq: 2, MaxSeq: 9, UnreadCount: 4, DraftText: "untouched"}); err != nil {
			t.Fatal(err)
		}
	}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	if !c.ConfigureConversationSyncV2(ctx) {
		t.Fatal("configuration failed")
	}
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	listener := &metadataBoundaryListener{}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })
	syncMetadata := func() error {
		if !notification {
			return c.IncrSyncConversations(ctx)
		}
		detail, err := json.Marshal(&sdkws.ConversationUpdateTips{UserID: "u", ConversationIDList: []string{"sg_target"}})
		if err != nil {
			return err
		}
		content, err := json.Marshal(&sdkws.NotificationElem{Detail: string(detail)})
		if err != nil {
			return err
		}
		return c.DoConversationChangedNotification(ctx, &sdkws.MsgData{Content: content})
	}
	var injected *metadataProjectionConflict
	if conflict {
		injected = &metadataProjectionConflict{conversationSyncV2Store: c.syncV2.store}
		c.syncV2.store = injected
	}

	if err = syncMetadata(); err != nil {
		t.Fatal(err)
	}
	if conflict && (!injected.injected || injected.calls < 2) {
		t.Fatal("projection conflict was not exercised and retried")
	}
	row, err := database.GetConversation(ctx, "sg_target")
	if err != nil || row.MinSeq != 7 {
		t.Fatalf("boundary did not persist: %+v %v", row, err)
	}
	if len(listener.changed) != 1 {
		t.Fatalf("boundary persisted but no single update callback: %v", listener.changed)
	}
	var rows []*model_struct.LocalConversation
	if err = json.Unmarshal([]byte(listener.changed[0]), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].MinSeq != 7 || rows[0].DraftText != "keep" {
		t.Fatalf("stale or destructive callback: %+v", rows)
	}
	if pending {
		var latest struct {
			ClientMsgID string `json:"clientMsgID"`
			Seq         int64  `json:"seq"`
		}
		if err = json.Unmarshal([]byte(rows[0].LatestMsg), &latest); err != nil || latest.ClientMsgID != "pending" || latest.Seq != 0 {
			t.Fatalf("pending message removed: %+v %v", latest, err)
		}
	} else if rows[0].LatestMsg != "" || rows[0].LatestMsgSendTime != 10 {
		t.Fatal("cleared preview retained or conversation visibility timestamp lost")
	}
	page, err := c.GetConversationListSplit(ctx, 0, 10)
	if err != nil || len(page) != 1 || page[0].ConversationID != "sg_target" {
		t.Fatalf("clear hid the conversation: %+v %v", page, err)
	}
	if err = syncMetadata(); err != nil {
		t.Fatal(err)
	}
	if len(listener.changed) != 1 {
		t.Fatal("unchanged metadata emitted another update")
	}
	if notification {
		epochs, err := database.GetConversationSyncV2Epochs(ctx, []string{"sg_untouched"})
		if err != nil || epochs["sg_untouched"] != 0 {
			t.Fatalf("unrelated request invalidated: %v %v", epochs, err)
		}
		untouched, err := database.GetConversation(ctx, "sg_untouched")
		if err != nil || untouched.MinSeq != 2 || untouched.MaxSeq != 9 || untouched.UnreadCount != 4 || untouched.DraftText != "untouched" {
			t.Fatalf("unrelated state changed: %+v %v", untouched, err)
		}
	}
}

func TestSyncV2MetadataRetryDoesNotReplaySuccessfulBatches(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"})
	d, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot"}
	before := make(map[string]*model_struct.LocalConversation)
	for i := 0; i < 301; i++ {
		id := fmt.Sprintf("qa_%03d", i)
		cp.SelectedIDs = append(cp.SelectedIDs, id)
		row := &model_struct.LocalConversation{ConversationID: id, MinSeq: 7, MaxSeq: 6, LatestMsg: `{"seq":6}`, LatestMsgSendTime: 10}
		if err = d.InsertConversation(ctx, row); err != nil {
			t.Fatal(err)
		}
		old := *row
		old.MinSeq = 6
		before[id] = &old
	}
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = d.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	c := NewConversation(ctx, &interaction.LongConnMgr{}, d, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.store = &metadataProjectionConflict{conversationSyncV2Store: c.syncV2.store, failAt: 2}
	l := &metadataBoundaryListener{}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return l })
	if err = c.notifyConversationSyncV2Metadata(ctx, before, cp); err != nil {
		t.Fatalf("second-batch conflict was not recovered: %v", err)
	}
	if wrapper := c.syncV2.store.(*metadataProjectionConflict); !wrapper.injected || wrapper.calls != 3 {
		t.Fatalf("unexpected projection attempts: %d", wrapper.calls)
	}
	if len(l.changed) != 2 {
		t.Fatalf("batches were not delivered once: %d", len(l.changed))
	}
	if err = c.notifyConversationSyncV2Metadata(ctx, before, cp); err != nil {
		t.Fatal(err)
	}
	if len(l.changed) != 2 {
		t.Fatalf("successful batch replayed: %d", len(l.changed))
	}
	seen := map[string]bool{}
	for _, payload := range l.changed {
		var rows []*model_struct.LocalConversation
		if err = json.Unmarshal([]byte(payload), &rows); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if seen[row.ConversationID] {
				t.Fatalf("duplicate callback: %s", row.ConversationID)
			}
			seen[row.ConversationID] = true
		}
	}
	if len(seen) != 301 {
		t.Fatalf("notification lost: %d", len(seen))
	}
}

func TestSyncV2HiddenMetadataDoesNotPublishEntryOrUnread(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "u"})
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	id := "sg_hidden"
	for _, row := range []*model_struct.LocalConversation{{ConversationID: id, UnreadCount: 7, LatestMsgSendTime: 80}, {ConversationID: "visible", UnreadCount: 3, LatestMsgSendTime: 90}} {
		if err = database.InsertConversation(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", SelectedIDs: []string{id}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "messages"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = database.HideConversationSyncV2(ctx, id); err != nil {
		t.Fatal(err)
	}
	before, err := database.GetConversation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.UpdateColumnsConversation(ctx, id, map[string]interface{}{"unread_count": 9, "min_seq": 1, "max_seq": 2}); err != nil {
		t.Fatal(err)
	}
	if err = database.BatchInsertMessageList(ctx, id, []*model_struct.LocalChatLog{{ClientMsgID: "saved", Seq: 2, SendTime: 80, Status: 2}}); err != nil {
		t.Fatal(err)
	}
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	if !c.ConfigureConversationSyncV2(ctx) {
		t.Fatal("configure failed")
	}
	c.syncV2.mode.Store(syncV2Enabled)
	listener := &metadataBoundaryListener{}
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })
	if err = c.notifyConversationSyncV2Metadata(ctx, map[string]*model_struct.LocalConversation{id: before}, cp); err != nil {
		t.Fatal(err)
	}
	if len(listener.changed) != 0 {
		t.Fatalf("hidden metadata published entry: %v", listener.changed)
	}
	total, err := database.GetTotalUnreadMsgCountDB(ctx)
	if err != nil || total != 3 {
		t.Fatalf("hidden metadata leaked unread total: %d %v", total, err)
	}
	row, err := database.GetConversation(ctx, id)
	if err != nil || row.LatestMsgSendTime != 0 || row.UnreadCount != 9 {
		t.Fatalf("hidden projection lost authoritative state: %+v %v", row, err)
	}
}
