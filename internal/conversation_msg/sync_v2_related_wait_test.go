//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/internal/relation"
	"github.com/openimsdk/openim-sdk-core/v3/internal/user"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

func TestSyncV2OpenDoesNotWaitForRelatedDirectory(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/conversation/sync_capabilities":
			data = &pb.Capabilities{ConversationSyncV2: true, PolicyVersion: syncv2.PolicyVersion, MaxBatchSize: 500, MaxPageSize: 500, MaxRequestBytes: 65536, SnapshotTTLSeconds: 600, IncrementalRecovery: true, IncrementalRecoveryVersion: syncv2.RecoveryVersion, SyncDomainID: "domain", MaxChangePageSize: 100}
		case "/group/get_incremental_join_groups":
			enterOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(http.StatusForbidden)
			return
		case "/msg/sync_seqs_v2":
			data = &pb.StateResponse{States: []*pb.State{{ConversationID: "si_peer_u", Known: true, StateToken: "state", MinSeq: 1, Conversation: &conversation.Conversation{OwnerUserID: "u", ConversationID: "si_peer_u", UserID: "peer", ConversationType: 1}}}}
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	defer releaseOnce.Do(func() { close(release) })
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), config))
	defer cancel()
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Stage: "snapshot", Generation: "g", PolicyVersion: syncv2.PolicyVersion, SyncDomainID: "domain", RecoveryVersion: syncv2.RecoveryVersion, ChangeCursor: "cursor", RecoveryBaselineReady: true, VersionID: "v"}
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = database.SaveConversationSyncV2(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertFriend(ctx, &model_struct.LocalFriend{OwnerUserID: "u", FriendUserID: "peer", Nickname: "Peer"}); err != nil {
		t.Fatal(err)
	}
	queue := make(chan common.Cmd2Value, 10)
	u := user.NewUser(database, "u", queue)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, relation.NewFriend("u", database, u, queue), group.NewGroup("u", database, queue), u, nil)
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	c.ConfigureConversationSyncV2(ctx)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, c) }()
	defer func() { cancel(); <-stopped }()
	background := make(chan error, 1)
	go func() { _, err := c.SynchronizeConversationV2(ctx, false); background <- err }()
	defer func() { releaseOnce.Do(func() { close(release) }); <-background }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("directory request not reached")
	}
	opened := make(chan error, 1)
	go func() {
		row, err := c.GetOneConversation(ctx, constant.SingleChatType, "peer")
		if err == nil && (row == nil || row.ShowName != "Peer") {
			err = fmt.Errorf("missing foreground conversation")
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(release) })
		<-opened
		t.Fatal("opening conversation waited for related directory")
	}
}
