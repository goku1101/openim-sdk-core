package group

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/db_interface"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"net/http"
	"testing"
)

func TestMemberBatchUnavailableCleanup(t *testing.T) {
	for _, code := range []int{0, 1002, 1204, 1501} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			calls := 0
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				var q protocol.BatchGetIncrementalGroupMemberReq
				json.NewDecoder(r.Body).Decode(&q)
				if code != 0 {
					json.NewEncoder(w).Encode(map[string]any{"errCode": code, "errMsg": "rejected"})
					return
				}
				memberReply(w, &protocol.BatchGetIncrementalGroupMemberResp{})
			})
			l := &permissionNotificationListener{}
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return l })
			d.InsertGroupMember(ctx, &model_struct.LocalGroupMember{GroupID: "g", UserID: "member"})
			d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: g.groupAndMemberVersionTableName(), EntityID: "g", VersionID: "v", Version: 1})
			g.groupMemberCache.Store("cached", &model_struct.LocalGroupMember{GroupID: "g", UserID: "member"})
			err := g.syncLegacyGroupAndMember(ctx, "g")
			if code == 1501 {
				if err == nil {
					t.Fatal("auth error swallowed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if groups, err := d.GetJoinedGroupListDB(ctx); err != nil || len(groups) != 0 {
				t.Fatal("group remains", err)
			}
			if members, err := d.GetGroupMemberListByGroupID(ctx, "g"); err != nil || len(members) != 0 {
				t.Fatal("members remain", err)
			}
			if _, err = d.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), "g"); !errs.ErrRecordNotFound.Is(err) {
				t.Fatal("version remains", err)
			}
			if _, ok := g.groupMemberCache.Load("cached"); ok {
				t.Fatal("cache remains")
			}
			if l.deleted != 1 {
				t.Fatalf("delete events %d", l.deleted)
			}
			if err = g.syncLegacyGroupAndMember(ctx, "g"); err != nil {
				t.Fatal("repeat cleanup", err)
			}
			if l.deleted != 1 {
				t.Fatal("duplicate delete event")
			}
			if calls > 4 {
				t.Fatal("unbounded retry")
			}
		})
	}
}

func TestMemberBatchMixedGroupsAndWriteFailure(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		t.Run(fmt.Sprint(failWrite), func(t *testing.T) {
			calls := 0
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				memberReply(w, &protocol.BatchGetIncrementalGroupMemberResp{RespList: map[string]*protocol.GetIncrementalGroupMemberResp{"healthy": {VersionID: "v", Version: 1, Insert: []*sdkws.GroupMemberFullInfo{{GroupID: "healthy", UserID: "member"}}}}})
			})
			g.conversationCh = make(chan common.Cmd2Value, 10)
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return &cleanupListener{} })
			if err := d.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: "healthy"}); err != nil {
				t.Fatal(err)
			}
			if failWrite {
				g.db = &failedMemberDB{DataBase: g.db}
			}
			err := g.syncLegacyGroupAndMember(ctx, "g", "healthy")
			if failWrite {
				if err == nil {
					t.Fatal("database error swallowed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			groups, err := d.GetJoinedGroupListDB(ctx)
			if err != nil || len(groups) != 1 || groups[0].GroupID != "healthy" {
				t.Fatal("mixed cleanup corrupted normal group", err)
			}
			if calls != 2 {
				t.Fatal("legacy omission must be confirmed once", calls)
			}
		})
	}
}

type failedMemberDB struct{ db_interface.DataBase }

func (d *failedMemberDB) GetGroupMemberListByGroupID(context.Context, string) ([]*model_struct.LocalGroupMember, error) {
	return nil, fmt.Errorf("injected database failure")
}

type cleanupListener struct{ permissionNotificationListener }

func (*cleanupListener) OnGroupMemberAdded(string) {}
