//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	sdk "github.com/openimsdk/openim-sdk-core/v3/pkg/sdk_params_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSyncV2CachedHistoryRefreshesInvalidatedPermissionBeforeLimit(t *testing.T) {
	var unknown atomic.Bool
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			t.Errorf("fully cached page made message request: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		reads.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": &pb.StateResponse{States: []*pb.State{{ConversationID: "c", Known: !unknown.Load(), StateToken: "new", MinSeq: 5, MaxSeq: 8, PermissionMaxSeq: 8}}}})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), config))
	defer cancel()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "c", MinSeq: 1, MaxSeq: 12, DraftText: "draft"}); err != nil {
		t.Fatal(err)
	}
	var rows []*model_struct.LocalChatLog
	for seq := int64(1); seq <= 12; seq++ {
		rows = append(rows, &model_struct.LocalChatLog{ClientMsgID: fmt.Sprintf("m%d", seq), Seq: seq, SendTime: seq, ContentType: constant.Text, Content: `{"content":"cached"}`})
	}
	if err = database.CommitConversationSyncV2Messages(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "c", ParentID: "c", StateToken: "old", MinSeq: 1, ScannedSeq: 12}, rows); err != nil {
		t.Fatal(err)
	}
	if err = database.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	c.syncV2.mode.Store(syncV2Enabled)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	result, err := c.GetAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2})
	if err != nil || len(result.MessageList) != 2 || result.MessageList[0].Seq != 7 || result.MessageList[1].Seq != 8 || reads.Load() != 1 {
		t.Fatalf("cached page bypassed new permission: %+v %v reads=%d", result, err, reads.Load())
	}
	reverse, err := c.GetAdvancedHistoryMessageListReverse(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2})
	if err != nil || len(reverse.MessageList) != 2 || reverse.MessageList[0].Seq != 5 || reverse.MessageList[1].Seq != 6 || reads.Load() != 1 {
		t.Fatalf("reverse bounds/cache reuse incorrect: %+v %v", reverse, err)
	}
	if err = database.InsertMessage(ctx, "c", &model_struct.LocalChatLog{ClientMsgID: "local", SendTime: 100, ContentType: constant.Text, Content: `{"content":"pending"}`}); err != nil {
		t.Fatal(err)
	}
	localPage, err := c.GetAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2})
	if err != nil || len(localPage.MessageList) != 2 || localPage.MessageList[1].ClientMsgID != "local" || reads.Load() != 1 {
		t.Fatalf("permission filter hid local pending message: %+v %v", localPage, err)
	}
	if _, err = database.GetMessageBySeq(ctx, "c", 12); err != nil {
		t.Fatal("permission filtering deleted stored history")
	}

	for _, stage := range []string{"read", "delivery"} {
		for _, reverse := range []bool{false, true} {
			wrapper := &invalidatingHistoryStore{conversationSyncV2Store: database, stage: stage, remaining: 1}
			c.syncV2.store = wrapper
			beforeReads := reads.Load()
			page, retryErr := c.getAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2}, reverse)
			if retryErr != nil || page == nil || len(page.MessageList) != 2 || wrapper.checks != 2 || reads.Load() != beforeReads+1 {
				t.Fatalf("%s reverse=%v invalidation not recovered: page=%+v err=%v checks=%d", stage, reverse, page, retryErr, wrapper.checks)
			}
			for _, row := range page.MessageList {
				if row.Seq != 0 && (row.Seq < 5 || row.Seq > 8) {
					t.Fatal("retry exposed revoked history")
				}
			}
		}
	}
	wrapper := &invalidatingHistoryStore{conversationSyncV2Store: database, stage: "delivery", remaining: 10}
	c.syncV2.store = wrapper
	exhausted, retryErr := c.getAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2}, false)
	if exhausted != nil || !errors.Is(retryErr, sdkerrs.ErrSyncV2StateChanged) || wrapper.checks != 8 {
		t.Fatalf("continuous invalidation was not bounded: page=%+v err=%v checks=%d", exhausted, retryErr, wrapper.checks)
	}
	c.syncV2.store = database
	unknown.Store(true)
	if err = database.InvalidateConversationSyncV2Stream(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	result, err = c.GetAdvancedHistoryMessageList(ctx, sdk.GetAdvancedHistoryMessageListParams{ConversationID: "c", Count: 2})
	if !errors.Is(err, syncv2.ErrStatePending) || result != nil {
		t.Fatalf("unknown permission exposed cached rows: %+v %v", result, err)
	}
	if _, err = database.FilterConversationSyncV2History(ctx, "c", rows); err == nil {
		t.Fatal("final delivery check accepted invalidated cache")
	}
}

// Invalidate after the initial permission check, at either the cached read or
// the final delivery check. The returned page must come from a fresh attempt.
type invalidatingHistoryStore struct {
	conversationSyncV2Store
	stage     string
	remaining int
	checks    int
}

func (s *invalidatingHistoryStore) invalidate(ctx context.Context, id, stage string) error {
	if s.stage != stage {
		return nil
	}
	s.checks++
	if s.remaining == 0 {
		return nil
	}
	s.remaining--
	return s.InvalidateConversationSyncV2Stream(ctx, id)
}
func (s *invalidatingHistoryStore) GetConversationSyncV2HistoryPage(ctx context.Context, id string, count int, startTime, startSeq int64, startID string, reverse bool) ([]*model_struct.LocalChatLog, error) {
	if err := s.invalidate(ctx, id, "read"); err != nil {
		return nil, err
	}
	return s.conversationSyncV2Store.GetConversationSyncV2HistoryPage(ctx, id, count, startTime, startSeq, startID, reverse)
}
func (s *invalidatingHistoryStore) FilterConversationSyncV2History(ctx context.Context, id string, rows []*model_struct.LocalChatLog) ([]*model_struct.LocalChatLog, error) {
	if err := s.invalidate(ctx, id, "delivery"); err != nil {
		return nil, err
	}
	return s.conversationSyncV2Store.FilterConversationSyncV2History(ctx, id, rows)
}
