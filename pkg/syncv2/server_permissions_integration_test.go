//go:build !js

package syncv2_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	conv "github.com/openimsdk/openim-sdk-core/v3/internal/conversation_msg"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/protocol/constant"
)

func TestPinnedServerHTTPPermissionsFixture(t *testing.T) {
	endpoint := os.Getenv("SYNCV2_SDK_FIXTURE_URL")
	chat := os.Getenv("SYNCV2_SDK_FIXTURE_CHAT")
	if endpoint == "" || chat == "" {
		t.Skip("requires isolated permission fixture")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" {
		t.Fatal("only localhost fixture endpoints allowed")
	}
	user := os.Getenv("SYNCV2_SDK_FIXTURE_USER")
	ctx, cancel := context.WithTimeout(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: user}), time.Minute)
	defer cancel()
	database, err := db.NewDataBase(ctx, user, t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	queue := make(chan common.Cmd2Value, 10)
	consumer := conv.NewConversation(ctx, nil, database, queue, nil, nil, nil, nil, nil)
	stopped := make(chan struct{})
	go func() { defer close(stopped); common.DoListener(ctx, consumer) }()
	defer func() { cancel(); <-stopped }()
	client := syncv2.NewClient(endpoint, "fixture-only", &http.Client{Timeout: 15 * time.Second})
	caps, err := client.Capabilities(ctx, "permission-caps", &pb.UserRequest{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	states, err := client.States(ctx, "permission-state", &pb.StateRequest{UserID: user, ConversationIDs: []string{chat}})
	if err != nil || len(states.States) != 1 {
		t.Fatal(states, err)
	}
	state := states.States[0]
	if !state.Known || state.MinSeq != 1 || state.MaxSeq != 10 || state.PermissionMaxSeq != 10 || state.HasReadSeq != 1 || state.UnreadCount != 9 {
		t.Fatalf("wrong personal boundary: %+v", state)
	}
	seqs := make([]int64, 20)
	for i := range seqs {
		seqs[i] = int64(i + 1)
	}
	page, err := client.ReadSequencePage(ctx, "permission-history", &pb.PullRequest{UserID: user, ConversationID: chat, Seqs: seqs}, caps, database)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Response.Messages) != 10 {
		t.Fatalf("visible messages=%d", len(page.Response.Messages))
	}
	for i, m := range page.Response.Messages {
		if m.Seq != int64(i+1) {
			t.Fatal("out-of-bound history", m.Seq)
		}
	}
	if err := consumer.ConsumeConversationSyncV2(ctx, page.Progress, page.Response.Messages); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 10; seq++ {
		if _, err := database.GetMessageBySeq(ctx, chat, seq); err != nil {
			t.Fatal("visible message not persisted", seq, err)
		}
	}
	// Retain the old HTTP page as the response delayed until after device B clears.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/qa/clear", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("operationID", "fixture-clear")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatal("fixture clear failed", response.StatusCode)
	}
	_, err = client.Pull(ctx, "stale-history", &pb.PullRequest{UserID: user, ConversationID: chat, Seqs: []int64{10}, StateToken: state.StateToken})
	var apiErr *syncv2.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrCode != 10 {
		t.Fatalf("old state token survived clear: %v", err)
	}
	cleared, err := client.ReadSequencePage(ctx, "cleared-history", &pb.PullRequest{UserID: user, ConversationID: chat, Seqs: seqs}, caps, database)
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared.Response.Messages) != 0 || cleared.Response.State.MinSeq != 11 || cleared.Response.State.HasReadSeq != 1 || cleared.Response.State.UnreadCount != 0 {
		t.Fatalf("clear with lagging read seq: %+v", cleared.Response)
	}
	if err := consumer.ConsumeConversationSyncV2(ctx, cleared.Progress, cleared.Response.Messages); err != nil {
		t.Fatal(err)
	}
	if err := consumer.ConsumeConversationSyncV2(ctx, page.Progress, page.Response.Messages); !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) {
		t.Fatalf("late response survived new boundary: %v", err)
	}
	current, err := database.GetConversationSyncV2Stream(ctx, chat)
	if err != nil || current.MinSeq != 11 || current.Epoch <= page.Progress.Epoch {
		t.Fatal("late commit reverted visibility", current, err)
	}
	t.Log("personal max 10 clips server max 20; clear min 11 with read 1 has zero unread; stale token and delayed consumer commit rejected")
	// These are the real server's terminal-notification fixtures. Verify the SDK
	// receives only the addressed departure, never subsequent group notifications.
	for _, kind := range []int32{constant.MemberQuitNotification, constant.MemberKickedNotification, constant.GroupDismissedNotification} {
		parent := fmt.Sprintf("sg_%s_terminal_%d", user, kind)
		states, err := client.States(ctx, "terminal-state", &pb.StateRequest{UserID: user, ConversationIDs: []string{parent}, Notifications: true})
		if err != nil || len(states.States) != 1 {
			t.Fatal(kind, states, err)
		}
		s := states.States[0]
		if s.MaxSeq != 7 || s.PermissionMaxSeq != 7 {
			t.Fatal("terminal boundary", kind, s)
		}
		pulled, err := client.PullBatched(ctx, "terminal-pull", &pb.PullRequest{UserID: user, ConversationID: parent, Notifications: true, Seqs: []int64{7, 8, 9}, StateToken: s.StateToken}, caps)
		if err != nil || len(pulled.Messages) != 1 || pulled.Messages[0].Seq != 7 || pulled.Messages[0].ContentType != kind {
			t.Fatal("terminal visibility", kind, pulled, err)
		}
		t.Logf("terminal notification kind=%d cap=7 delivered only seq 7", kind)
	}
}
