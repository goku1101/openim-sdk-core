//go:build !js

package conversation_msg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
)

func TestSyncV2RetryWorkerResumesDurableUnknownAndStopsUnavailable(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "unavailable"}[unavailable], func(t *testing.T) {
			var known atomic.Bool
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/msg/sync_seqs_v2" {
					t.Errorf("retry used unrelated route: %s", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				calls.Add(1)
				var req pb.StateRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if !req.Notifications || len(req.ConversationIDs) != 1 || req.ConversationIDs[0] != "sg_pending" {
					t.Error("retry widened selection")
				}
				response := &pb.StateResponse{States: []*pb.State{{ConversationID: "n_pending", Known: known.Load(), StateToken: "t", MinSeq: 1}}}
				if known.Load() && unavailable {
					response = &pb.StateResponse{UnavailableIDs: []string{"sg_pending"}}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": response})
			}))
			defer server.Close()
			config := &ccontext.GlobalConfig{UserID: "u"}
			config.ApiAddr = server.URL
			base := ccontext.WithInfo(context.Background(), config)
			dir := t.TempDir()
			database, err := db.NewDataBase(base, "u", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			start := func(database *db.DataBase) (*Conversation, context.CancelFunc, <-chan struct{}) {
				ctx, cancel := context.WithCancel(base)
				queue := make(chan common.Cmd2Value, 10)
				c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
				c.ConfigureConversationSyncV2(ctx)
				c.syncV2.mode.Store(syncV2Enabled)
				c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
				c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "ready", Progress: 100})
				done := make(chan struct{})
				go func() { defer close(done); common.DoListener(ctx, c) }()
				if err := c.StartConversationSyncV2Retries(ctx); err != nil {
					t.Fatal(err)
				}
				if err := c.StartConversationSyncV2Retries(ctx); err != nil {
					t.Fatal(err)
				}
				return c, cancel, done
			}
			c, cancel, done := start(database)
			result := &syncv2.SyncResult{Checkpoint: &model_struct.LocalConversationSyncV2{UserID: "u", NotificationIDs: []string{"sg_pending"}}, Notifications: &syncv2.StreamResults{PendingIDs: []string{"n_pending"}}}
			if err = c.persistConversationSyncV2Pending(base, result); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				job, err := database.NextConversationSyncV2Retry(base)
				if err != nil {
					t.Fatal(err)
				}
				if job != nil && job.Attempts > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("unknown state not rescheduled")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !c.GetConversationSyncState().PendingRetries {
				t.Fatal("pending queue hidden from status")
			}
			cancel()
			c.WaitConversationSyncV2Retries()
			<-done
			before := calls.Load()
			if err = database.Close(base); err != nil {
				t.Fatal(err)
			}
			known.Store(true)
			database, err = db.NewDataBase(base, "u", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			c, cancel, done = start(database)
			defer func() { cancel(); c.WaitConversationSyncV2Retries(); <-done; _ = database.Close(base) }()
			deadline = time.Now().Add(3 * time.Second)
			for {
				job, err := database.NextConversationSyncV2Retry(base)
				if err != nil {
					t.Fatal(err)
				}
				if job == nil && !c.GetConversationSyncState().PendingRetries {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("restarted task not acknowledged")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if calls.Load() != before+1 {
				t.Fatalf("retried completed/unavailable stream: %d -> %d", before, calls.Load())
			}
			if !unavailable {
				stream, err := database.GetConversationSyncV2Stream(base, "n_pending")
				if err != nil || stream.StateToken != "t" {
					t.Fatalf("acknowledged before durable stream commit: %+v %v", stream, err)
				}
			}
		})
	}
}

var _ syncv2.RetryStore = (*db.DataBase)(nil)

func TestSyncV2RetryCancellationJoinsInflightReadBeforeDatabaseClose(t *testing.T) {
	requested := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requested)
		<-release
	}))
	defer func() { close(release); server.Close() }()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	base := ccontext.WithInfo(context.Background(), config)
	database, err := db.NewDataBase(base, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(base)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	c.ConfigureConversationSyncV2(ctx)
	c.syncV2.mode.Store(syncV2Enabled)
	c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
	c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: "ready", Progress: 100})
	payload, _ := json.Marshal(&pb.PullRequest{UserID: "u", ConversationID: "sg_pending", Notifications: true})
	if err = database.PutConversationSyncV2Retry(base, &model_struct.LocalConversationSyncV2Retry{TaskID: "reconcile:n_pending", Kind: "reconcile", Payload: string(payload), DueAt: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err = c.StartConversationSyncV2Retries(ctx); err != nil {
		t.Fatal(err)
	}
	defer cancel()
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not issue request")
	}
	cancel()
	stopped := make(chan struct{})
	go func() { c.WaitConversationSyncV2Retries(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not join canceled network request")
	}
	job, err := database.NextConversationSyncV2Retry(base)
	if err != nil || job == nil || job.Attempts != 0 {
		t.Fatalf("cancellation lost in-flight task: %+v %v", job, err)
	}
	if err = database.Close(base); err != nil {
		t.Fatal(err)
	}
}
