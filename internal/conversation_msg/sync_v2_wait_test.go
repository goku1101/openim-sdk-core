//go:build !js

package conversation_msg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/protocol/sdkws"
)

// Hold an actual synchronization HTTP request, then cancel callers queued
// behind it. They must finish without releasing the unrelated network request.
func TestSyncV2CancelledOperationsDoNotWaitForBackgroundNetwork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enteredOnce.Do(func() { close(entered) })
		<-release
		fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":false}}`)
	}))
	defer server.Close()
	defer releaseOnce.Do(func() { close(release) })
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx := ccontext.WithInfo(context.Background(), config)
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	c.ConfigureConversationSyncV2(ctx)
	background := make(chan error, 1)
	go func() { _, err := c.SynchronizeConversationV2(ctx, false); background <- err }()
	defer func() { releaseOnce.Do(func() { close(release) }); <-background }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background did not reach network")
	}
	before := c.GetConversationSyncState()
	for _, operation := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"open", func(ctx context.Context) error { _, err := c.activateConversationSyncV2Once(ctx, "peer"); return err }},
		{"push", func(ctx context.Context) error {
			return c.processConversationSyncV2PushRound(ctx, &sdkws.PushMessages{}, false, nil)
		}},
		{"sync", func(ctx context.Context) error { _, err := c.SynchronizeConversationV2(ctx, false); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			request, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- operation.run(request) }()
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("cancellation lost: %v", err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Error("cancelled operation still waits for full background synchronization")
				// Permit cleanup on the unmodified implementation without leaking a worker.
				releaseOnce.Do(func() { close(release) })
				<-done
			}
		})
	}
	if after := c.GetConversationSyncState(); !reflect.DeepEqual(after, before) {
		t.Fatalf("cancelled waiter changed active round status: before=%+v after=%+v", before, after)
	}
}

func TestSyncV2RunGateCancellationDoesNotReleaseAnotherOwner(t *testing.T) {
	r := &conversationSyncV2Runtime{}
	if err := r.acquireRun(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- r.acquireRun(ctx) }()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	second := make(chan error, 1)
	go func() { second <- r.acquireRun(context.Background()) }()
	select {
	case <-second:
		t.Fatal("cancelled waiter released another owner's gate")
	case <-time.After(20 * time.Millisecond):
	}
	r.releaseRun()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
		r.releaseRun()
	case <-time.After(time.Second):
		t.Fatal("cancellation left a leaked waiter")
	}
}
