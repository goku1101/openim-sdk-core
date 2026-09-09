//go:build !js

package syncv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	conv "github.com/openimsdk/openim-sdk-core/v3/internal/conversation_msg"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

func TestPinnedServerHTTPReadyFixture(t *testing.T) {
	endpoint := os.Getenv("SYNCV2_SDK_FIXTURE_URL")
	if endpoint == "" {
		t.Skip("requires isolated server fixture")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" {
		t.Fatal("only localhost fixture endpoints allowed")
	}
	user := os.Getenv("SYNCV2_SDK_FIXTURE_USER")
	selected, err := strconv.Atoi(os.Getenv("SYNCV2_SDK_FIXTURE_SELECTED")) // fixture size
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{}
	for i := 0; i < selected; i++ {
		allowed[fmt.Sprintf("si_%s_%05d", user, i)] = true
	}
	ctx, cancel := context.WithTimeout(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: user}), 8*time.Minute)
	defer cancel()
	database, err := db.NewDataBase(ctx, user, t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	queue := make(chan common.Cmd2Value, 100)
	consumer := conv.NewConversation(ctx, nil, database, queue, nil, nil, nil, nil, nil)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, consumer) }()
	defer func() { cancel(); <-stopped }()
	var mu sync.Mutex
	pulls := map[string]int{}
	transport := fixtureTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/msg/sync_messages_v2" {
			body, err := r.GetBody()
			if err != nil {
				return nil, err
			}
			var req pb.PullRequest
			err = json.NewDecoder(body).Decode(&req)
			body.Close()
			if err != nil {
				return nil, err
			}
			if req.Notifications || !allowed[req.ConversationID] || len(req.Seqs) != 1 || req.Seqs[0] != 10 {
				t.Errorf("pull outside fixture visibility: notifications=%v seqs=%v", req.Notifications, req.Seqs)
			}
			mu.Lock()
			pulls[req.ConversationID]++
			mu.Unlock()
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	published := false
	progress := 0
	start := time.Now()
	result, err := syncv2.NewClient(endpoint, "fixture-only", &http.Client{Transport: transport}).Synchronize(ctx, "ready-fixture", user, database, syncv2.CoordinatorHooks{
		PrepareRelated: func(context.Context) error { return nil },
		// Fixture metadata remains fixed; this does not test the legacy metadata API.
		ReadMetadata: func(_ context.Context, v uint64, id string) (*conversation.GetIncrementalConversationResp, error) {
			return &conversation.GetIncrementalConversationResp{Version: v, VersionID: id}, nil
		},
		Commit: consumer.ConsumeConversationSyncV2,
		Publish: func(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2, _ func(int, int)) error {
			if checkpoint.Stage == "ready" {
				return fmt.Errorf("ready before publish")
			}
			for id := range allowed {
				msg, err := database.GetMessageBySeq(ctx, id, 10)
				if err != nil {
					return err
				}
				if msg.ClientMsgID != id {
					return fmt.Errorf("wrong persisted message")
				}
			}
			published = true
			return nil
		},
		Progress: func(v int) {
			if v <= progress || v >= 100 {
				t.Errorf("invalid progress %d -> %d", progress, v)
			}
			progress = v
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Enabled || !published || result.Checkpoint.Stage != "ready" || progress != 99 {
		t.Fatal("coordinator did not reach ready after consumption")
	}
	checkpoint, err := database.GetConversationSyncV2(ctx)
	if err != nil || checkpoint.Stage != "ready" {
		t.Fatal("ready not persisted", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(pulls) != selected {
		t.Fatalf("pulled conversations=%d want %d", len(pulls), selected)
	}
	for _, n := range pulls {
		if n != 1 {
			t.Fatalf("duplicate message pull: %d", n)
		}
	}
	t.Logf("SDK coordinator ready: selected=%d message pulls=%d elapsed=%s", selected, len(pulls), time.Since(start))
}
