//go:build !js

package syncv2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Invoked by the pinned server's disposable Mongo/Redis/gRPC fixture process.
// Independent processes keep server and SDK protobuf registries separate.
func TestPinnedServerHTTPDetailsFixture(t *testing.T) {
	endpoint := os.Getenv("SYNCV2_SDK_FIXTURE_URL")
	if endpoint == "" {
		t.Skip("requires the isolated server fixture runner")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" {
		t.Fatal("only localhost fixture endpoints allowed")
	}
	user := os.Getenv("SYNCV2_SDK_FIXTURE_USER")
	count, err := strconv.Atoi(os.Getenv("SYNCV2_SDK_FIXTURE_COUNT")) // fixture size, not a business ID
	if err != nil {
		t.Fatal(err)
	}
	selected, err := strconv.Atoi(os.Getenv("SYNCV2_SDK_FIXTURE_SELECTED"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	database, err := db.NewDataBase(ctx, user, t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	chats, parents := map[string]bool{}, map[string]bool{}
	requests, bytesOut, bytesIn, self, releases := 0, 0, 0, 0, 0
	transport := fixtureTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		bytesOut += len(body)
		if len(body) > 65536 {
			t.Error("oversized SDK request")
		}
		switch r.URL.Path {
		case "/conversation/sync_capabilities", "/conversation/sync_snapshot":
		case "/conversation/sync_snapshot_release":
			releases++
		case "/msg/sync_seqs_v2":
			var req pb.StateRequest
			if err := json.Unmarshal(body, &req); err != nil {
				return nil, err
			}
			if len(req.ConversationIDs) > 500 {
				t.Error("oversized ID batch")
			}
			if req.IncludeSelf {
				self++
				if !req.Notifications || len(req.ConversationIDs) != 0 {
					t.Error("self notification mixed with parent batch")
				}
			}
			seen := chats
			if req.Notifications {
				seen = parents
				if req.IncludeDetails {
					t.Error("notification details requested")
				}
			} else if !req.IncludeDetails || len(req.ConversationIDs) == 0 {
				t.Error("invalid chat detail request")
			}
			for _, id := range req.ConversationIDs {
				if seen[id] {
					t.Error("duplicate state ID")
				}
				seen[id] = true
			}
		default:
			t.Errorf("unexpected route: %s", r.URL.Path)
		}
		resp, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		bytesIn += len(data)
		resp.Body = io.NopCloser(bytes.NewReader(data))
		return resp, nil
	})
	started := time.Now()
	round, err := syncv2.NewClient(endpoint, "fixture-only", &http.Client{Transport: transport}).PrepareDetails(ctx, "cross-process-fixture", user, database)
	if err != nil {
		var apiError *syncv2.APIError
		if errors.As(err, &apiError) {
			t.Fatalf("fixture API: code=%d reason=%s detail=%s", apiError.ErrCode, apiError.ErrMsg, apiError.ErrDlt)
		}
		t.Fatal(err)
	}
	if !round.Enabled || len(chats) != selected || len(parents) != count || self != 1 || releases != 1 {
		t.Fatalf("enabled=%v chats=%d parents=%d self=%d releases=%d", round.Enabled, len(chats), len(parents), self, releases)
	}
	if len(round.Selection.ChatIDs) != selected || len(round.Selection.NotificationIDs) != count {
		t.Fatal("snapshot selection mismatch")
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("si_%s_%05d", user, i)
		if !parents[id] || chats[id] != (i < selected) {
			t.Fatalf("request scope mismatch at fixture index %d", i)
		}
	}
	ids, err := database.GetAllConversationIDList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != selected {
		t.Fatalf("SQLite conversations=%d want %d", len(ids), selected)
	}
	for _, id := range ids {
		if !chats[id] {
			t.Fatal("unexpected conversation persisted")
		}
	}
	checkpoint, err := database.GetConversationSyncV2(ctx)
	if err != nil || checkpoint.Stage != "messages" || len(checkpoint.SelectedIDs) != selected {
		t.Fatal("detail checkpoint not committed", err)
	}
	t.Logf("SDK HTTP + SQLite: selected=%d parents=%d requests=%d requestBytes=%d responseBytes=%d elapsed=%s", selected, count, requests, bytesOut, bytesIn, time.Since(started))
}
