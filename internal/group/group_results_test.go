package group

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	protocol "github.com/openimsdk/protocol/group"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
)

// Exercise the actual HTTP decoder, SQLite writes and delete callbacks together.
func TestGroupResultsDeferredKeepsLocalData(t *testing.T) {
	calls := 0
	g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req protocol.BatchGetIncrementalGroupMemberReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		status := "deferred"
		reason := "batch_change_limit"
		if calls > 1 {
			status, reason = "success", ""
		}
		memberReply(w, map[string]any{
			"groupResults": map[string]any{"g": map[string]string{"status": status, "reason": reason}},
			"respList":     map[string]any{"g": map[string]any{"versionID": "v", "version": 1}},
		})
	})
	g.conversationCh = make(chan common.Cmd2Value, 10)
	listener := &permissionNotificationListener{}
	g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return listener })
	if err := d.InsertGroupMember(ctx, &model_struct.LocalGroupMember{GroupID: "g", UserID: "member"}); err != nil {
		t.Fatal(err)
	}
	if err := g.syncLegacyGroupAndMember(ctx, "g"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("deferred group was not retried: %d calls", calls)
	}
	if groups, err := d.GetJoinedGroupListDB(ctx); err != nil || len(groups) != 1 {
		t.Fatal("deferred group deleted", err)
	}
	if listener.deleted != 0 {
		t.Fatal("deferred group emitted deletion")
	}
}

func TestGroupResultsInaccessiblePlaceholder(t *testing.T) {
	g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		memberReply(w, map[string]any{
			"groupResults": map[string]any{"g": map[string]string{"status": "inaccessible", "reason": "left_or_dismissed"}},
			"respList":     map[string]any{"g": map[string]any{}},
		})
	})
	listener := &permissionNotificationListener{}
	g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return listener })
	if err := g.syncLegacyGroupAndMember(ctx, "g"); err != nil {
		t.Fatal(err)
	}
	if groups, err := d.GetJoinedGroupListDB(ctx); err != nil || len(groups) != 0 {
		t.Fatal("terminal placeholder was applied as success", err)
	}
	if listener.deleted != 1 {
		t.Fatal("missing deletion callback")
	}
}

func TestGroupResultsInvalidResponsePreservesGroup(t *testing.T) {
	for _, status := range []string{"success", "unknown", ""} {
		t.Run(status, func(t *testing.T) {
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				memberReply(w, map[string]any{"groupResults": map[string]any{"g": map[string]string{"status": status}}})
			})
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return &permissionNotificationListener{} })
			if err := g.syncLegacyGroupAndMember(ctx, "g"); err == nil {
				t.Fatal("invalid response accepted")
			}
			if groups, err := d.GetJoinedGroupListDB(ctx); err != nil || len(groups) != 1 {
				t.Fatal("invalid response deleted group", err)
			}
		})
	}
}

func TestGroupResultsMixedDeferredAndTerminal(t *testing.T) {
	for _, reason := range []string{"batch_change_limit", "not_returned"} {
		t.Run(reason, func(t *testing.T) {
			calls := 0
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req protocol.BatchGetIncrementalGroupMemberReq
				json.NewDecoder(r.Body).Decode(&req)
				outcomes := map[string]any{}
				payloads := map[string]any{}
				for _, item := range req.ReqList {
					switch item.GroupID {
					case "g":
						outcomes["g"] = map[string]string{"status": "inaccessible", "reason": "left_or_dismissed"}
						payloads["g"] = map[string]any{}
					case "healthy":
						outcomes["healthy"] = map[string]string{"status": "success"}
						payloads["healthy"] = map[string]any{"versionID": "v", "version": 1, "insert": []any{map[string]any{"groupID": "healthy", "userID": "member"}}}
					case "later":
						if calls == 1 {
							outcomes["later"] = map[string]string{"status": "deferred", "reason": reason}
						} else {
							outcomes["later"] = map[string]string{"status": "success"}
							payloads["later"] = map[string]any{"versionID": "v", "version": 1, "insert": []any{map[string]any{"groupID": "later", "userID": "member"}}}
						}
					}
				}
				memberReply(w, map[string]any{"groupResults": outcomes, "respList": payloads})
			})
			g.conversationCh = make(chan common.Cmd2Value, 10)
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return &cleanupListener{} })
			for _, id := range []string{"healthy", "later"} {
				if err := d.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: id}); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.syncLegacyGroupAndMember(ctx, "g", "healthy", "later"); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("unexpected batch count: %d", calls)
			}
			groups, err := d.GetJoinedGroupListDB(ctx)
			if err != nil || len(groups) != 2 {
				t.Fatal("normal group removed", err)
			}
			for _, id := range []string{"healthy", "later"} {
				v, err := d.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), id)
				if err != nil || v.Version != 1 {
					t.Fatal("successful group was not committed", id, err)
				}
			}
		})
	}
}

func TestGroupResultsDeferredBoundedAndCancelable(t *testing.T) {
	for _, path := range []string{"batch", "notification", "partial", "cancel"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/group/sync_capabilities" {
					memberCaps(w)
					return
				}
				calls++
				memberReply(w, map[string]any{"groupResults": map[string]any{"g": map[string]string{"status": "deferred", "reason": "not_returned"}}})
			})
			if err := d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: g.groupAndMemberVersionTableName(), EntityID: "g", VersionID: "old", Version: 2}); err != nil {
				t.Fatal(err)
			}
			var err error
			switch path {
			case "notification":
				err = g.onlineSyncGroupAndMember(ctx, "g", nil, nil, nil, &sdkws.GroupInfo{GroupID: "g"}, 0, 4, "new")
			case "partial":
				if err := d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: partialMemberTable, EntityID: "g", VersionID: "old", Version: 2}); err != nil {
					t.Fatal(err)
				}
				_, err = g.syncPartialGroup(ctx, "g")
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				err = g.syncLegacyGroupAndMember(canceled, "g")
			default:
				err = g.syncLegacyGroupAndMember(ctx, "g")
			}
			if err == nil {
				t.Fatal("deferred task falsely completed")
			}
			if path != "cancel" && (!errors.Is(err, errGroupDeferred) || calls != 4) {
				t.Fatalf("unexpected deferred error/calls: %v / %d", err, calls)
			}
			groups, readErr := d.GetJoinedGroupListDB(ctx)
			if readErr != nil || len(groups) != 1 {
				t.Fatal("deferred group removed", readErr)
			}
			v, readErr := d.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), "g")
			if readErr != nil || v.Version != 2 {
				t.Fatal("deferred advanced cursor", readErr)
			}
		})
	}
}

func TestGroupResultsNotificationTerminalCleanup(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "legacy"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/group/sync_capabilities" {
					memberCaps(w)
					return
				}
				memberReply(w, map[string]any{"groupResults": map[string]any{"g": map[string]string{"status": "inaccessible", "reason": "left_or_dismissed"}}, "respList": map[string]any{"g": map[string]any{}}})
			})
			listener := &permissionNotificationListener{}
			g.SetGroupListener(func() open_im_sdk_callback.OnGroupListener { return listener })
			if err := d.InsertGroupMember(ctx, &model_struct.LocalGroupMember{GroupID: "g", UserID: "member"}); err != nil {
				t.Fatal(err)
			}
			g.groupMemberCache.Store("g:member", &model_struct.LocalGroupMember{GroupID: "g", UserID: "member"})
			if partial {
				if err := d.SetVersionSync(ctx, &model_struct.LocalVersionSync{Table: partialMemberTable, EntityID: "g", VersionID: "v", Version: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.onlineSyncGroupAndMember(ctx, "g", nil, nil, nil, &sdkws.GroupInfo{GroupID: "g"}, 0, 4, "new"); err != nil {
				t.Fatal(err)
			}
			if _, err := d.GetVersionSync(ctx, g.groupAndMemberVersionTableName(), "g"); !errs.ErrRecordNotFound.Is(err) {
				t.Fatal("terminal placeholder wrote a version", err)
			}
			if _, ok := g.groupMemberCache.Load("g:member"); ok {
				t.Fatal("profile cache survived")
			}
			if err := g.clearUnavailableGroup(ctx, "g"); err != nil {
				t.Fatal(err)
			}
			if listener.deleted != 1 {
				t.Fatal("cleanup was not idempotent")
			}
		})
	}
}

func TestGroupResultsLegacyOmissionConfirmsNormalGroup(t *testing.T) {
	calls := 0
	g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			memberReply(w, map[string]any{"respList": map[string]any{"healthy": map[string]any{"versionID": "v", "version": 1}}})
			return
		}
		memberReply(w, map[string]any{"respList": map[string]any{"g": map[string]any{"versionID": "v", "version": 1}}})
	})
	if err := d.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: "healthy"}); err != nil {
		t.Fatal(err)
	}
	if err := g.syncLegacyGroupAndMember(ctx, "g", "healthy"); err != nil {
		t.Fatal(err)
	}
	groups, err := d.GetJoinedGroupListDB(ctx)
	if err != nil || len(groups) != 2 || calls != 2 {
		t.Fatalf("legacy cutoff deleted a normal group: groups=%d calls=%d err=%v", len(groups), calls, err)
	}
}

func TestGroupResultsBudgetProgressDoesNotExhaustRetries(t *testing.T) {
	calls := 0
	g, ctx, d := memberFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req protocol.BatchGetIncrementalGroupMemberReq
		json.NewDecoder(r.Body).Decode(&req)
		results := map[string]any{}
		payloads := map[string]any{}
		for i, item := range req.ReqList {
			if i == 0 {
				results[item.GroupID] = map[string]string{"status": "success"}
				payloads[item.GroupID] = map[string]any{}
			} else {
				results[item.GroupID] = map[string]string{"status": "deferred", "reason": "batch_change_limit"}
			}
		}
		memberReply(w, map[string]any{"groupResults": results, "respList": payloads})
	})
	ids := []string{"g"}
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("normal-%d", i)
		ids = append(ids, id)
		if err := d.InsertGroup(ctx, &model_struct.LocalGroup{GroupID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.syncLegacyGroupAndMember(ctx, ids...); err != nil {
		t.Fatal("progressing budget batches must complete", err)
	}
	if calls != len(ids) {
		t.Fatalf("unexpected requests %d", calls)
	}
}
