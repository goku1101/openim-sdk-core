package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

func TestCapabilitiesUsesAuthenticatedPost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/conversation/sync_capabilities" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("token") != "test-token" || r.Header.Get("operationID") != "probe-op" {
			t.Error("missing auth/operation headers")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["userID"] != "9007199254740993" {
			t.Error("userID was changed")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"snapshotTTLSeconds":600,"maxRequestBytes":65536}}`))
	}))
	defer server.Close()
	client := NewClient(server.URL, "test-token", server.Client())
	caps, err := client.Capabilities(context.Background(), "probe-op", &pb.UserRequest{UserID: "9007199254740993"})
	if err != nil {
		t.Fatal(err)
	}
	if !caps.ConversationSyncV2 || caps.PolicyVersion != "conversation-sync-v2.2" {
		t.Fatalf("unexpected capability: %v", caps)
	}
}

func TestCapabilitiesRejectsAmbiguousFailures(t *testing.T) {
	for _, body := range []string{`{}`, `{"errCode":0}`, `{"errCode":0,"data":null}`, `{"errCode":0,"data":{}}`, `{"errCode":0,"data":{"conversationSyncV2":null}}`, `{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"unknown"}}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			if _, err := NewClient(server.URL, "", server.Client()).Capabilities(context.Background(), "op", &pb.UserRequest{UserID: "u"}); err == nil {
				t.Fatal("ambiguous response accepted as capability")
			}
		})
	}
}

func TestClientPreservesServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"errCode":14,"errMsg":"SYNC_V2_REDIS_UNAVAILABLE","errDlt":"retry"}`))
	}))
	defer server.Close()
	_, err := NewClient(server.URL, "", server.Client()).Capabilities(context.Background(), "op", &pb.UserRequest{UserID: "u"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.ErrCode != 14 || apiErr.ErrMsg != "SYNC_V2_REDIS_UNAVAILABLE" || apiErr.ErrDlt != "retry" {
		t.Fatalf("lost server error: %v", err)
	}
	if !strings.Contains(err.Error(), "/conversation/sync_capabilities") || strings.Contains(err.Error(), "retry") {
		t.Fatalf("missing safe route or leaked error detail: %v", err)
	}
	if RecoveryFor(err) != Retry {
		t.Fatalf("route context changed recovery: %v", err)
	}

}

func TestReleaseAcceptsServerEmptyEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversation/sync_snapshot_release" {
			t.Errorf("wrong route: %s", r.URL.Path)
		}
		var req pb.ReleaseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SnapshotID != "s" || req.UserID != "u" {
			t.Errorf("bad release request: %v", &req)
		}
		_, _ = w.Write([]byte(`{"errCode":0,"errMsg":"","errDlt":""}`))
	}))
	defer server.Close()
	if err := NewClient(server.URL, "", server.Client()).Release(context.Background(), "op", &pb.ReleaseRequest{UserID: "u", SnapshotID: "s"}); err != nil {
		t.Fatal(err)
	}
}

func TestPullUsesServerJSONBinding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/msg/sync_messages_v2" {
			t.Errorf("wrong route: %s", r.URL.Path)
		}
		// The pinned server's a2r binding uses encoding/json, not protojson.
		var req pb.PullRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("server cannot decode pull: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.ConversationID != "9007199254740993" || len(req.Seqs) != 1 || req.Seqs[0] != 9007199254740993 {
			t.Errorf("request changed: %v", &req)
		}
		_, _ = w.Write([]byte(`{"errCode":0,"data":{"state":{"conversationID":"9007199254740993","known":true,"maxSeq":9007199254740993},"unresolvedSeqs":[9007199254740993]}}`))
	}))
	defer server.Close()
	resp, err := NewClient(server.URL, "test", server.Client()).Pull(context.Background(), "op", &pb.PullRequest{UserID: "u", ConversationID: "9007199254740993", Seqs: []int64{9007199254740993}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.State.MaxSeq != 9007199254740993 || resp.UnresolvedSeqs[0] != 9007199254740993 {
		t.Fatal("response precision lost")
	}
}
