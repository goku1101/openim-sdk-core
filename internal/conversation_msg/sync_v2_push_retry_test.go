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
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2DurablePushSurvivesRestartWithoutReplayingTransientNotification(t *testing.T) {
	testDurablePushRestart(t, "ready")
}

func TestSyncV2DurablePushRetriesDuringPriorityReadyBackground(t *testing.T) {
	testDurablePushRestart(t, "syncing")
}

func testDurablePushRestart(t *testing.T, phase string) {
	var known atomic.Bool
	var reads atomic.Int32
	var deniedReads atomic.Int32
	var database *db.DataBase
	var observedDurable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		state := &pb.State{ConversationID: "n_pending", Known: known.Load(), StateToken: "token", MinSeq: 1, MaxSeq: 1}
		switch r.URL.Path {
		case "/msg/sync_seqs_v2":
			reads.Add(1)
			job, err := database.NextConversationSyncV2Retry(context.Background())
			if err != nil || job == nil || job.Kind != "push" {
				t.Error("request started before durable push was stored")
			} else {
				observedDurable.Store(true)
			}
			var req pb.StateRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			response := &pb.StateResponse{States: []*pb.State{state}}
			for _, id := range req.ConversationIDs {
				if id == "sg_denied" {
					deniedReads.Add(1)
					response.UnavailableIDs = append(response.UnavailableIDs, id)
				}
			}
			data = response
		case "/msg/sync_messages_v2":
			data = &pb.PullResponse{State: state, Messages: []*sdkws.MsgData{{ClientMsgID: "persisted", Seq: 1, ContentType: constant.BusinessNotification, Content: []byte(`{"detail":"durable"}`)}}}
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": data})
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	base := ccontext.WithInfo(context.Background(), config)
	dir := t.TempDir()
	var err error
	database, err = db.NewDataBase(base, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	cp := &model_struct.LocalConversationSyncV2{UserID: "u", Generation: "g", Stage: "snapshot", NotificationIDs: []string{"sg_pending", "sg_denied"}}
	if err = database.SaveConversationSyncV2(base, cp); err != nil {
		t.Fatal(err)
	}
	cp.Stage = "ready"
	if err = database.SaveConversationSyncV2(base, cp); err != nil {
		t.Fatal(err)
	}
	var transient, durable atomic.Int32
	makeSession := func(startWorker bool) (*Conversation, context.CancelFunc, <-chan struct{}) {
		ctx, cancel := context.WithCancel(base)
		queue := make(chan common.Cmd2Value, 10)
		c := NewConversation(ctx, &interaction.LongConnMgr{}, database, queue, nil, nil, nil, nil, nil)
		c.ConfigureConversationSyncV2(ctx)
		c.syncV2.mode.Store(syncV2Enabled)
		c.syncV2.caps = &pb.Capabilities{MaxBatchSize: 500, MaxRequestBytes: 65536}
		c.syncV2.status.Store(&sdk_struct.ConversationSyncState{Phase: phase, Progress: 60, PriorityReady: true})
		c.SetBusinessListener(func() open_im_sdk_callback.OnCustomBusinessListener {
			return &syncV2PushBusinessListener{receive: func(value string) {
				if value == "transient" {
					transient.Add(1)
				}
				if value == "durable" {
					durable.Add(1)
				}
			}}
		})
		done := make(chan struct{})
		go func() { defer close(done); common.DoListener(ctx, c) }()
		if startWorker {
			if err := c.StartConversationSyncV2Retries(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return c, cancel, done
	}
	c, cancel, done := makeSession(false)
	push := &sdkws.PushMessages{NotificationMsgs: map[string]*sdkws.PullMsgs{"n_pending": {Msgs: []*sdkws.MsgData{{Seq: 1}, {Seq: 0, ContentType: constant.BusinessNotification, Content: []byte(`{"detail":"transient"}`)}}}}}
	push.NotificationMsgs["n_denied"] = &sdkws.PullMsgs{Msgs: []*sdkws.MsgData{{Seq: 2}}}
	handled, err := c.ProcessConversationSyncV2Push(base, push)
	if !handled || err != syncv2.ErrStatePending || !observedDurable.Load() {
		t.Fatalf("unknown push not kept: %v %v", handled, err)
	}
	job, err := database.NextConversationSyncV2Retry(base)
	if err != nil || job == nil {
		t.Fatal("pending push lost")
	}
	var saved sdkws.PushMessages
	if err = json.Unmarshal([]byte(job.Payload), &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.NotificationMsgs) != 1 || len(push.NotificationMsgs) != 2 {
		t.Fatal("terminal stream retained or live input mutated")
	}
	if len(saved.NotificationMsgs["n_pending"].Msgs) != 1 || saved.NotificationMsgs["n_pending"].Msgs[0].Seq != 1 {
		t.Fatal("transient notification persisted")
	}
	cancel()
	c.WaitConversationSyncV2Retries()
	<-done
	if err = database.Close(base); err != nil {
		t.Fatal(err)
	}
	database, err = db.NewDataBase(base, "u", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	known.Store(true)
	c, cancel, done = makeSession(true)
	defer func() { cancel(); c.WaitConversationSyncV2Retries(); <-done; _ = database.Close(base) }()
	deadline := time.Now().Add(4 * time.Second)
	for {
		job, err = database.NextConversationSyncV2Retry(base)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push replay did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if transient.Load() != 1 || durable.Load() != 1 || reads.Load() != 2 || deniedReads.Load() != 1 {
		t.Fatalf("lost or duplicate delivery: transient=%d durable=%d reads=%d", transient.Load(), durable.Load(), reads.Load())
	}
}
