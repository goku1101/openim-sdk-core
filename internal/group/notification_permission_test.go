//go:build !js

package group

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	protocolgroup "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
)

type permissionNotificationListener struct {
	open_im_sdk_callback.OnGroupListener
	deleted int
}

func (l *permissionNotificationListener) OnJoinedGroupDeleted(string) { l.deleted++ }

func TestNotificationMemberPermissionAfterLeaving(t *testing.T) {
	for _, kind := range []int32{constant.GroupCreatedNotification, constant.GroupInfoSetNotification, constant.MemberInvitedNotification, constant.MemberEnterNotification} {
		for _, scenario := range []string{"left", "still-joined", "refresh-failed", "authentication-failed"} {
			t.Run(fmt.Sprint(kind)+"/"+scenario, func(t *testing.T) {
				const gid = "group_9007199254740993"
				memberCalls, joinCalls := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/group/sync_capabilities":
						w.WriteHeader(404)
					case "/group/get_incremental_group_members_batch":
						memberCalls++
						code := 1002
						if scenario == "authentication-failed" {
							code = 1501
						}
						json.NewEncoder(w).Encode(map[string]any{"errCode": code, "errMsg": "member request rejected"})
					case "/group/get_incremental_join_groups":
						joinCalls++
						if scenario == "refresh-failed" && memberCalls > 0 {
							w.WriteHeader(503)
							return
						}
						resp := &protocolgroup.GetIncrementalJoinGroupResp{VersionID: "v", Version: 1}
						if scenario == "left" && memberCalls > 0 {
							resp.Version = 2
							resp.Delete = []string{gid}
						}
						json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": resp})
					default:
						t.Errorf("unexpected API %s", r.URL.Path)
						w.WriteHeader(404)
					}
				}))
				defer server.Close()
				cfg := &ccontext.GlobalConfig{UserID: "u"}
				cfg.ApiAddr = server.URL
				ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), cfg), "late-group-notification")
				database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close(ctx)
				if err = database.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: gid}); err != nil {
					t.Fatal(err)
				}
				if err = database.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: model_struct.LocalGroup{}.TableName(), EntityID: "u", VersionID: "v", Version: 1, UIDList: []string{gid}}); err != nil {
					t.Fatal(err)
				}
				if err = database.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: "sg_" + gid, DraftText: "keep", IsPinned: true}); err != nil {
					t.Fatal(err)
				}
				g := NewGroup("u", database, make(chan common.Cmd2Value, 10))
				l := &permissionNotificationListener{}
				g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return l })
				invalidations := 0
				g.SetConversationSyncV2Invalidator(func(_ context.Context, id string) error {
					if id != gid {
						t.Errorf("changed group ID %q", id)
					}
					invalidations++
					return nil
				})
				info := &sdkws.GroupInfo{GroupID: gid}
				var detail any
				switch kind {
				case constant.GroupCreatedNotification:
					detail = &sdkws.GroupCreatedTips{Group: info}
				case constant.GroupInfoSetNotification:
					detail = &sdkws.GroupInfoSetTips{Group: info, GroupMemberVersionID: "changed", GroupMemberVersion: 10}
				case constant.MemberInvitedNotification:
					detail = &sdkws.MemberInvitedTips{Group: info, InvitedUserList: []*sdkws.GroupMemberFullInfo{{UserID: "u"}}}
				case constant.MemberEnterNotification:
					detail = &sdkws.MemberEnterTips{Group: info, EntrantUser: &sdkws.GroupMemberFullInfo{UserID: "u"}}
				}
				raw, _ := json.Marshal(detail)
				content, _ := json.Marshal(map[string]string{"detail": string(raw)})
				err = g.DoNotification(ctx, &sdkws.MsgData{ContentType: kind, Content: content})
				isolated := kind != constant.GroupInfoSetNotification && scenario != "authentication-failed"
				expectedCalls := 1
				if isolated {
					expectedCalls = 2
				}
				if memberCalls != expectedCalls {
					t.Fatalf("member API calls=%d", memberCalls)
				}
				if scenario == "left" || isolated {
					if err != nil {
						t.Fatalf("obsolete notification stopped sync: %v", err)
					}
					groups, readErr := database.GetJoinedGroupListDB(ctx)
					if readErr != nil || len(groups) != 0 {
						t.Fatalf("group survived removal: %v", err)
					}
					if l.deleted != 1 || invalidations != 1 {
						t.Fatalf("permission removal not applied: deleted=%d invalidated=%d", l.deleted, invalidations)
					}
				} else if err == nil {
					t.Fatal("real permission/auth/refresh error was swallowed")
				}
				conv, err := database.GetConversation(ctx, "sg_"+gid)
				if err != nil || conv.DraftText != "keep" || !conv.IsPinned {
					t.Fatalf("local conversation changed: %v %v", conv, err)
				}
				if scenario == "authentication-failed" && joinCalls > 1 {
					t.Fatal("authentication error triggered permission reconciliation")
				}
			})
		}
	}
}
