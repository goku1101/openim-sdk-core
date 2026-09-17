package syncv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestDirectoryTransportPreservesCursorsAndUint64(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("token") != "test" || r.Header.Get("operationID") != "op" {
			t.Error("missing authenticated POST")
		}
		switch r.URL.Path {
		case "/conversation/sync_changes_v2":
			var req pb.ChangeRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Cursor != "opaque/+==" || req.UntilCursor != "fixed/==" || req.UserID != "9007199254740993" {
				t.Errorf("changed recovery request: %v", &req)
			}
			fmt.Fprint(w, `{"errCode":0,"data":{"changes":[],"nextCursor":"fixed/==","untilCursor":"fixed/==","caughtUp":true,"syncDomainID":"domain"}}`)
		case "/group/get_group_member_page_v2":
			var req pb.GroupMemberPageRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.GroupID != "9007199254740993" || req.Keyword != "Ab" || len(req.ExcludeUserIDs) != 1 || req.ExcludeUserIDs[0] != "9007199254740995" {
				t.Errorf("changed directory request: %v", &req)
			}
			fmt.Fprint(w, `{"errCode":0,"data":{"members":[],"hasMore":true,"nextCursor":"next/+==","version":18446744073709551615,"live":true,"totalKnown":false}}`)
		case "/conversation/sync_scan":
			fmt.Fprint(w, `{"errCode":0,"data":{"conversationIDs":[],"hasMore":true,"cursor":"scan/+==","live":true,"scanned":100}}`)
		case "/group/sync_capabilities":
			fmt.Fprint(w, `{"errCode":0,"data":{"partialMemberSync":true,"memberDirectoryV2":true,"largeGroupThreshold":1000,"maxPageSize":100,"protocolVersion":"group-member-v2.1"}}`)
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := NewClient(server.URL, "test", server.Client())
	ctx := context.Background()
	changes, err := c.Changes(ctx, "op", &pb.ChangeRequest{UserID: "9007199254740993", Cursor: "opaque/+==", UntilCursor: "fixed/==", PageSize: 100})
	if err != nil || !changes.CaughtUp || changes.NextCursor != "fixed/==" {
		t.Fatalf("changes=%v err=%v", changes, err)
	}
	page, err := c.GroupMemberPage(ctx, "op", &pb.GroupMemberPageRequest{GroupID: "9007199254740993", Keyword: "Ab", ExcludeUserIDs: []string{"9007199254740995"}, PageSize: 100})
	if err != nil || page.Version != ^uint64(0) || page.TotalKnown || !page.HasMore || page.NextCursor != "next/+==" {
		t.Fatalf("page=%v err=%v", page, err)
	}
	scan, err := c.Scan(ctx, "op", &pb.ScanRequest{UserID: "u", Scope: "chat", PageSize: 100})
	if err != nil || !scan.HasMore || len(scan.ConversationIDs) != 0 || scan.Cursor != "scan/+==" {
		t.Fatalf("scan=%v err=%v", scan, err)
	}
	caps, err := c.GroupMemberCapabilities(ctx, "op", &pb.GroupMemberCapabilitiesRequest{UserID: "u"})
	if err != nil || !caps.PartialMemberSync || caps.LargeGroupThreshold != 1000 {
		t.Fatalf("caps=%v err=%v", caps, err)
	}
}
