//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	conversation "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
)

type deferredProfileListener struct {
	open_im_sdk_callback.OnGroupListener
	changes int
}

func (l *deferredProfileListener) OnGroupInfoChanged(string) { l.changes++ }

func TestSyncV2DeferredProfileAndReadNotificationsCommitWithoutHistory(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) { testDeferredNotifications(t, cached) })
	}
}
func testDeferredNotifications(t *testing.T, cached bool) {
	var states, unexpected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_seqs_v2" {
			unexpected.Add(1)
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		var q pb.StateRequest
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
		}
		if len(q.ConversationIDs) != 1 || q.ConversationIDs[0] != "sg_g" || !q.IncludeDetails || q.Notifications {
			t.Errorf("unexpected state scope: %+v", &q)
		}
		states.Add(1)
		data := &pb.StateResponse{States: []*pb.State{{ConversationID: "sg_g", Known: true, StateToken: "read-state", MinSeq: 10, MaxSeq: 9, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "sg_g", ConversationType: 3, GroupID: "g"}}}}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx, cancel := context.WithCancel(ccontext.WithOperationID(ccontext.WithInfo(context.Background(), config), "deferred-notifications"))
	defer cancel()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	if err = database.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: "g", GroupType: 2, GroupName: "before"}); err != nil {
		t.Fatal(err)
	}
	if err = database.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: "local_group_entities_version", EntityID: "g", VersionID: "members", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if cached {
		if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_g", ConversationType: 3, GroupID: "g"}); err != nil {
			t.Fatal(err)
		}
	}
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "messages", SelectedIDs: []string{"si_u_other"}}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 100)
	g := group.NewGroup("u", database, queue)
	listener := &deferredProfileListener{}
	g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return listener })
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, g, nil, nil)
	if !c.ConfigureConversationSyncV2(ctx) {
		t.Fatal("configure failed")
	}
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "ready", Progress: 100})
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	profile, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_g", ParentID: "sg_g", Notifications: true, StateToken: "profile", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	read, err := database.AcceptConversationSyncV2State(ctx, 0, &model_struct.LocalConversationSyncV2Stream{ConversationID: "n_u_u", Notifications: true, Self: true, StateToken: "read", MinSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	consume := func(stream *model_struct.LocalConversationSyncV2Stream, seq int64, kind int32, detail any) {
		t.Helper()
		raw, e := json.Marshal(detail)
		if e != nil {
			t.Fatal(e)
		}
		content, e := json.Marshal(&sdkws.NotificationElem{Detail: string(raw)})
		if e != nil {
			t.Fatal(e)
		}
		progress := *stream
		progress.ScannedSeq = seq
		msg := &sdkws.MsgData{ClientMsgID: fmt.Sprintf("%s-%d", stream.ConversationID, seq), Seq: seq, ContentType: kind, Content: content}
		if e = c.ConsumeConversationSyncV2(ctx, &progress, []*sdkws.MsgData{msg}); e != nil {
			t.Fatal(e)
		}
		current, e := database.GetConversationSyncV2Stream(ctx, stream.ConversationID)
		if e != nil || current.ScannedSeq != seq {
			t.Fatalf("notification not committed: %+v %v", current, e)
		}
	}
	for i := int64(1); i <= 3; i++ {
		consume(profile, i, constant.GroupInfoSetNotification, &sdkws.GroupInfoSetTips{Group: &sdkws.GroupInfo{GroupID: "g", GroupType: 2, GroupName: fmt.Sprintf("profile-%d", i)}, GroupMemberVersionID: "members", GroupMemberVersion: uint64(i + 1)})
		consume(read, i, constant.HasReadReceipt, &sdkws.MarkAsReadTips{ConversationID: "sg_g", MarkAsReadUserID: "u", HasReadSeq: 9, Seqs: []int64{9}})
		current, e := database.GetConversationSyncV2(ctx)
		if e != nil || len(current.SelectedIDs) != 1 || current.SelectedIDs[0] != "si_u_other" {
			t.Fatalf("deferred chat activated: %+v %v", current, e)
		}
		row, e := database.GetConversation(ctx, "sg_g")
		if cached {
			if e != nil || row.LatestMsg != "" || row.LatestMsgSendTime != 0 || row.UnreadCount != 0 {
				t.Fatalf("notification created visible history: %+v %v", row, e)
			}
		} else if !errors.Is(e, gorm.ErrRecordNotFound) {
			t.Fatalf("notification created absent conversation: %+v %v", row, e)
		}
		groupRow, e := database.GetGroupInfoByGroupID(ctx, "g")
		if e != nil || groupRow.GroupName != fmt.Sprintf("profile-%d", i) {
			t.Fatalf("profile not persisted: %+v %v", groupRow, e)
		}
		if _, e = database.GetMessageBySeq(ctx, "sg_g", 9); e == nil {
			t.Fatal("invented an undownloaded message")
		}
	}
	wantStates := int32(0)
	if cached {
		wantStates = 3
	}
	if states.Load() != wantStates || unexpected.Load() != 0 || listener.changes != 3 {
		t.Fatalf("requests/callbacks: states=%d unexpected=%d profiles=%d", states.Load(), unexpected.Load(), listener.changes)
	}
}
