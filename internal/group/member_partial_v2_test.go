package group

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdk_params_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func memberFixture(t *testing.T, handle http.HandlerFunc) (*Group, context.Context, *db.DataBase) {
	t.Helper()
	server := httptest.NewServer(handle)
	t.Cleanup(server.Close)
	cfg := &ccontext.GlobalConfig{UserID: "self"}
	cfg.ApiAddr = server.URL
	cfg.DataDir = t.TempDir()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), cfg), "partial-test")
	database, err := db.NewDataBase(ctx, "self", cfg.DataDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close(ctx) })
	if err = database.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: "g", MemberCount: 25600}); err != nil {
		t.Fatal(err)
	}
	database.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: model_struct.LocalGroup{}.TableName(), EntityID: "self", VersionID: "joined", Version: 1, UIDList: model_struct.StringArray{"g"}})
	return NewGroup("self", database, nil), ctx, database
}
func memberReply(w http.ResponseWriter, data any) {
	json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
}
func memberCaps(w http.ResponseWriter) {
	memberReply(w, &pb.GroupMemberCapabilities{PartialMemberSync: true, MemberDirectoryV2: true, ProtocolVersion: "group-member-v2.1", LargeGroupThreshold: 1000, MaxPageSize: 100})
}
func ownerPage(w http.ResponseWriter, version uint64) {
	memberReply(w, &pb.GroupMemberPageResponse{Live: true, VersionID: "v", Version: version, Members: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "owner", RoleLevel: 100}}})
}
func pointReply(w http.ResponseWriter, r *http.Request) {
	var req protocol.GetGroupMembersInfoReq
	json.NewDecoder(r.Body).Decode(&req)
	members := []*sdkws.GroupMemberFullInfo{}
	for _, id := range req.UserIDs {
		members = append(members, &sdkws.GroupMemberFullInfo{GroupID: req.GroupID, UserID: id, RoleLevel: 60})
	}
	memberReply(w, &protocol.GetGroupMembersInfoResp{Members: members})
}

// Member count must not turn cold admission into a full member download.
func TestPartialMemberColdAdmissionScaleBoundaries(t *testing.T) {
	for _, count := range []int32{999, 1000, 25600, 100000} {
		t.Run(fmt.Sprintf("members_%d", count), func(t *testing.T) {
			pages, pointReads := 0, 0
			g, ctx, database := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/group/sync_capabilities":
					memberCaps(w)
				case "/group/get_group_member_page_v2":
					var req pb.GroupMemberPageRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.Filter != 1 || req.PageSize < 1 || req.PageSize > 100 {
						t.Errorf("unbounded cold member request: %+v", req)
					}
					pages++
					ownerPage(w, 1)
				case "/group/get_group_members_info":
					var req protocol.GetGroupMembersInfoReq
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if len(req.UserIDs) != 1 || req.UserIDs[0] != "self" {
						t.Errorf("cold sync fetched ordinary members: %+v", req.UserIDs)
					}
					pointReads++
					memberReply(w, &protocol.GetGroupMembersInfoResp{Members: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "self", RoleLevel: 60}}})
				default:
					t.Errorf("unexpected full member API: %s", r.URL.Path)
					w.WriteHeader(500)
				}
			})
			if err := database.UpdateGroup(ctx, &model_struct.LocalGroup{GroupID: "g", MemberCount: count}); err != nil {
				t.Fatal(err)
			}
			enabled, err := g.ensurePartialGroup(ctx, "g")
			if err != nil || enabled != (count >= 1000) {
				t.Fatalf("threshold admission: enabled=%v err=%v", enabled, err)
			}
			if count < 1000 {
				if pages != 0 || pointReads != 0 {
					t.Fatal("small group entered partial baseline")
				}
				return
			}
			members, err := database.GetGroupMemberListByGroupID(ctx, "g")
			if err != nil || len(members) != 2 || pages != 1 || pointReads != 1 {
				t.Fatalf("cold baseline: persisted=%d pages=%d points=%d err=%v", len(members), pages, pointReads, err)
			}
			for _, member := range members {
				if member.UserID != "self" && member.UserID != "owner" {
					t.Fatalf("ordinary member persisted: %s", member.UserID)
				}
			}
			if _, err := g.ensurePartialGroup(ctx, "g"); err != nil || pages != 1 || pointReads != 1 {
				t.Fatalf("cached baseline downloaded again: pages=%d points=%d err=%v", pages, pointReads, err)
			}
		})
	}
}

func TestPartialMemberBaselineDeltaAndUint64Reopen(t *testing.T) {
	var pages int
	g, ctx, database := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/group/sync_capabilities":
			memberCaps(w)
		case "/group/get_group_member_page_v2":
			pages++
			ownerPage(w, math.MaxUint64-1)
		case "/group/get_group_members_info":
			pointReply(w, r)
		default:
			t.Errorf("unexpected full-sync/API %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	database.InsertGroupMember(ctx, &model_struct.LocalGroupMember{GroupID: "g", UserID: "old"})
	database.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: g.groupAndMemberVersionTableName(), EntityID: "g", VersionID: "old", Version: 5, UIDList: model_struct.StringArray{"old"}})
	ok, err := g.ensurePartialGroup(ctx, "g")
	if err != nil || !ok {
		t.Fatalf("baseline %v %v", ok, err)
	}
	before, err := database.GetVersionSync(ctx, partialMemberTable, "g")
	if err != nil || before.Version != math.MaxUint64-1 || len(before.UIDList) != 0 {
		t.Fatalf("version %+v %v", before, err)
	}
	if _, err = database.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), "g"); !errs.ErrRecordNotFound.Is(err) {
		t.Fatal("retained full version", err)
	}
	g.groupMemberCache.Store("g:ordinary", &model_struct.LocalGroupMember{GroupID: "g", UserID: "ordinary"})
	err = g.applyPartialDeltaLocked(ctx, "g", before, &protocol.GetIncrementalGroupMemberResp{VersionID: "v", Version: math.MaxUint64, Insert: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "ordinary", RoleLevel: 20}}, Update: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "self", RoleLevel: 60}}})
	if err != nil {
		t.Fatal(err)
	}
	members, err := database.GetGroupMemberListByGroupID(ctx, "g")
	if err != nil || len(members) != 2 {
		t.Fatalf("persisted members %v %v", members, err)
	}
	if _, ok = g.groupMemberCache.Load("g:ordinary"); ok {
		t.Fatal("stale profile retained")
	}
	after, err := database.GetVersionSync(ctx, partialMemberTable, "g")
	if err != nil || after.Version != math.MaxUint64 {
		t.Fatalf("uint64 lost %+v %v", after, err)
	}
	if err = database.CommitGroupMemberPartialV2(ctx, "g", before, "v", 3, members); err == nil {
		t.Fatal("stale writer accepted")
	}
	// Close and reopen SQLite, not merely the process-level cache.
	if err = database.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := db.NewDataBase(ctx, "self", ccontext.Info(ctx).DataDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)
	after, err = reopened.GetVersionSync(ctx, partialMemberTable, "g")
	if err != nil || after.Version != math.MaxUint64 {
		t.Fatalf("reopened uint64 %+v %v", after, err)
	}
	restored := NewGroup("self", reopened, nil)
	if ok, err = restored.ensurePartialGroup(ctx, "g"); err != nil || !ok || pages != 1 {
		t.Fatalf("unnecessary baseline %d %v", pages, err)
	}
}
func TestPartialMemberDirectorySearchAndEmptyContinuation(t *testing.T) {
	var queries []*pb.GroupMemberPageRequest
	g, ctx, database := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/group/sync_capabilities":
			memberCaps(w)
		case "/group/get_group_members_info":
			pointReply(w, r)
		case "/group/get_group_member_page_v2":
			var req pb.GroupMemberPageRequest
			json.NewDecoder(r.Body).Decode(&req)
			if req.Filter == 1 {
				ownerPage(w, 1)
				return
			}
			queries = append(queries, &req)
			if req.Cursor == "" {
				memberReply(w, &pb.GroupMemberPageResponse{Live: true, VersionID: "v", HasMore: true, NextCursor: "opaque-next"})
				return
			}
			memberReply(w, &pb.GroupMemberPageResponse{Live: true, VersionID: "v", Members: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "row", Nickname: "prefix name"}}})
		default:
			t.Errorf("unexpected API %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	got, err := g.GetGroupMemberList(ctx, "g", 0, 0, 20)
	if err != nil || len(got) != 1 || len(queries) != 2 || queries[1].Cursor != "opaque-next" {
		t.Fatalf("empty continuation %v %v", got, err)
	}
	queries = nil
	got, err = g.SearchGroupMembers(ctx, &sdk_params_callback.SearchGroupMembersParam{GroupID: "g", KeywordList: []string{"prefix"}, IsSearchUserID: true, IsSearchMemberNickname: true, Count: 20})
	if err != nil || len(got) != 2 || len(queries) != 2 || queries[0].Keyword != "prefix" || len(queries[0].ExcludeUserIDs) != 1 {
		t.Fatalf("search OR %v queries=%v err=%v", got, queries, err)
	}
	rows, _ := database.GetGroupMemberListByGroupID(ctx, "g")
	if len(rows) != 2 {
		t.Fatal("directory persisted ordinary rows", len(rows))
	}
	ids, err := g.GetUsersInGroup(ctx, "g", []string{"uncached"})
	if err != nil || len(ids) != 1 {
		t.Fatalf("uncached membership %v %v", ids, err)
	}
	for i := 0; i < 5000; i++ {
		g.groupMemberCache.Store(fmt.Sprint(i), &model_struct.LocalGroupMember{UserID: fmt.Sprint(i)})
	}
	if g.groupMemberCache.data.Len() != 4096 {
		t.Fatal("unbounded profiles")
	}
	for i := 0; i < 2000; i++ {
		g.memberDirectory.pages.Add(fmt.Sprint(i), &memberPagePosition{})
	}
	if g.memberDirectory.pages.Len() != 1024 {
		t.Fatal("unbounded positions")
	}
}
func TestPartialMemberTransientFailureDoesNotSelectLegacy(t *testing.T) {
	var calls atomic.Int32
	g, ctx, _ := memberFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) })
	_, err := g.syncPartialGroup(ctx, "g")
	if err == nil || calls.Load() != 1 {
		t.Fatalf("capability failure fallback calls=%d err=%v", calls.Load(), err)
	}
}
func TestPartialMemberRollbackRebuildsLegacyBeforeReturning(t *testing.T) {
	rollingBack := false
	legacyFailure := true
	var fullIDs int
	g, ctx, database := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/group/sync_capabilities":
			memberCaps(w)
		case "/group/get_group_members_info":
			pointReply(w, r)
		case "/group/get_group_member_page_v2":
			if rollingBack {
				w.WriteHeader(404)
			} else {
				ownerPage(w, 1)
			}
		case "/group/get_incremental_group_members_batch":
			memberReply(w, &protocol.BatchGetIncrementalGroupMemberResp{RespList: map[string]*protocol.GetIncrementalGroupMemberResp{"g": {VersionID: "legacy", Version: 2, Full: true}}})
		case "/group/get_group_member_list":
			if legacyFailure {
				w.WriteHeader(503)
				return
			}
			memberReply(w, &protocol.GetGroupMemberListResp{Members: []*sdkws.GroupMemberFullInfo{{GroupID: "g", UserID: "self", RoleLevel: 60}, {GroupID: "g", UserID: "owner", RoleLevel: 100}, {GroupID: "g", UserID: "ordinary", RoleLevel: 20}}})
		case "/group/get_full_group_member_user_ids":
			fullIDs++
			memberReply(w, &protocol.GetFullGroupMemberUserIDsResp{UserIDs: []string{"self", "owner", "ordinary"}})
		default:
			t.Errorf("unexpected rollback API %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	if _, err := g.ensurePartialGroup(ctx, "g"); err != nil {
		t.Fatal(err)
	}
	rollingBack = true
	// The legacy synchronizer emits callbacks; use the existing no-op listener.
	g.listener = func() open_im_sdk_callback.OnGroupListener { return &permissionNotificationListener{} }
	if _, err := g.GetGroupMemberList(ctx, "g", 0, 0, 100); err == nil {
		t.Fatal("failed full rebuild exposed as success")
	}
	if _, err := database.GetVersionSync(ctx, partialMemberTable, "g"); err != nil {
		t.Fatal("lost retry marker", err)
	}
	legacyFailure = false
	rows, err := g.GetGroupMemberList(ctx, "g", 0, 0, 100)
	if err != nil || len(rows) != 3 || fullIDs != 1 {
		t.Fatalf("rollback rows=%d fullIDs=%d err=%v", len(rows), fullIDs, err)
	}
	if _, err = database.GetVersionSync(ctx, partialMemberTable, "g"); !errs.ErrRecordNotFound.Is(err) {
		t.Fatal("partial marker retained", err)
	}
	version, err := database.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), "g")
	if err != nil || len(version.UIDList) != 3 {
		t.Fatalf("legacy not rebuilt %+v %v", version, err)
	}
}
