//go:build !js

package conversation_msg

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
)

func TestSyncV2StatusDoesNotWaitForNetworkOrConsumer(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":false}}`)
	}))
	defer server.Close()
	config := &ccontext.GlobalConfig{UserID: "u"}
	config.ApiAddr = server.URL
	ctx := ccontext.WithInfo(context.Background(), config)
	database, err := db.NewDataBase(ctx, "u", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	c := NewConversation(ctx, &interaction.LongConnMgr{}, database, make(chan common.Cmd2Value), nil, nil, nil, nil, nil)
	c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return &syncV2Listener{} })
	if state := c.GetConversationSyncState(); state.Mode != "uninitialized" {
		t.Fatal(state)
	}
	c.ConfigureConversationSyncV2(ctx)
	done := make(chan error, 1)
	go func() { _, err := c.SynchronizeConversationV2(ctx, false); done <- err }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	<-entered
	observed := make(chan sdk_struct.ConversationSyncState, 1)
	go func() { observed <- c.GetConversationSyncState() }()
	select {
	case state := <-observed:
		if state.Mode != "negotiating" || state.Phase != "syncing" || state.Progress != 1 {
			t.Fatalf("in-flight state: %+v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("state query waited for the synchronization lock")
	}
}
