//go:build !js

package conversation_msg

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestSendRetryRetainsOriginalWireRequestAcrossRestart(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
	dir := t.TempDir()
	database, err := db.NewDataBase(ctx, "self", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	makeConversation := func() *Conversation {
		c := &Conversation{db: database, loginUserID: "self"}
		c.syncV2 = &conversationSyncV2Runtime{store: database}
		c.syncV2.mode.Store(syncV2Enabled)
		return c
	}
	c := makeConversation()
	lc := &model_struct.LocalConversation{ConversationID: "si_peer_self"}
	msg := &sdk_struct.MsgStruct{ClientMsgID: "original", CreateTime: 123, SendTime: 123, SendID: "self", RecvID: "peer", SessionType: constant.SingleChatType, ContentType: constant.Text, Content: `{"content":"original"}`, AttachedInfoElem: &sdk_struct.AttachedInfoElem{}}
	first, err := c.prepareSendRequest(ctx, msg, lc, &sdkws.OfflinePushInfo{Title: "original"}, map[string]bool{constant.IsNotPrivate: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	first = proto.Clone(first).(*sdkws.MsgData)
	if err = database.Close(ctx); err != nil {
		t.Fatal(err)
	}
	database, err = db.NewDataBase(ctx, "self", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	c = makeConversation()
	// Retry preparation may observe changed preferences/profile, but must send
	// exactly the first request if the previous server result was uncertain.
	msg.AttachedInfoElem.IsPrivateChat = true
	msg.AttachedInfoElem.BurnDuration = 30
	msg.SenderNickname = "updated profile"
	msg.Content = `{"content":"changed after uncertain send"}`
	retry, err := c.prepareSendRequest(ctx, msg, lc, &sdkws.OfflinePushInfo{Title: "new title"}, map[string]bool{constant.IsNotPrivate: false}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first, retry) {
		t.Fatal("retry changed the original request fingerprint")
	}
	if msg.TextElem == nil || msg.TextElem.Content != "original" || msg.AttachedInfoElem.IsPrivateChat {
		t.Fatal("retry callback describes a different payload")
	}

	if retry.SendTime != 0 || retry.ClientMsgID != "original" || retry.CreateTime != 123 {
		t.Fatal("retry changed original identity")
	}
}

func TestSendTimeoutAfterAccountDatabaseClosedReturnsError(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
	database, err := db.NewDataBase(ctx, "self", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	conn := &interaction.LongConnMgr{}
	conn.SetSyncRequestGuard(func(context.Context, int) error { return sdkerrs.ErrNetworkTimeOut })
	c := NewConversation(ctx, conn, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	if err = database.Close(ctx); err != nil {
		t.Fatal(err)
	}
	msg := &sdk_struct.MsgStruct{ClientMsgID: "uncertain", CreateTime: 123, ContentType: constant.Text, Content: `{"content":"original"}`}
	got, err := c.sendMessageToServer(ctx, msg, &model_struct.LocalConversation{ConversationID: "si_peer_self"}, nil, nil, nil, map[string]bool{}, false)
	if !sdkerrs.ErrNetworkTimeOut.Is(err) || got.ClientMsgID != "uncertain" || got.Status != constant.MsgStatusSendFailed {
		t.Fatalf("timeout lost retry identity: %v", err)
	}
}

type failingSendRequestStore struct{ conversationSyncV2Store }

func (f failingSendRequestStore) FreezeSendRequest(context.Context, string, string, []byte) ([]byte, error) {
	return nil, context.Canceled
}

func TestSendDoesNotDispatchBeforeRequestIsDurable(t *testing.T) {
	ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
	database, err := db.NewDataBase(ctx, "self", t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(ctx)
	dispatched := false
	conn := &interaction.LongConnMgr{}
	conn.SetSyncRequestGuard(func(context.Context, int) error { dispatched = true; return sdkerrs.ErrNetworkTimeOut })
	c := NewConversation(ctx, conn, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
	c.syncV2 = &conversationSyncV2Runtime{store: failingSendRequestStore{database}}
	c.syncV2.mode.Store(syncV2Enabled)
	msg := &sdk_struct.MsgStruct{ClientMsgID: "original", CreateTime: 123, ContentType: constant.Text, Content: `{"content":"original"}`}
	_, err = c.sendMessageToServer(ctx, msg, &model_struct.LocalConversation{ConversationID: "si_peer_self"}, nil, nil, nil, map[string]bool{}, false)
	if err != context.Canceled || dispatched {
		t.Fatalf("non-durable request sent: %v %v", dispatched, err)
	}
}

func TestFrozenMediaRetryReachesTransportWithoutUploadingAgain(t *testing.T) {
	bodies := map[int32]string{
		constant.Picture: `{"sourcePath":"/missing/photo.jpg","sourcePicture":{"uuid":"p","url":"https://example.invalid/photo.jpg","width":1,"height":1,"size":1}}`,
		constant.Sound:   `{"soundPath":"/missing/voice.aac","uuid":"s","sourceUrl":"https://example.invalid/voice.aac","duration":1,"dataSize":1}`,
		constant.Video:   `{"videoPath":"/missing/video.mp4","snapshotPath":"/missing/snapshot.jpg","videoUUID":"v","videoUrl":"https://example.invalid/video.mp4","snapshotUrl":"https://example.invalid/snapshot.jpg","duration":1,"videoSize":1}`,
		constant.File:    `{"filePath":"/missing/file.txt","uuid":"f","sourceUrl":"https://example.invalid/file.txt","fileName":"file.txt","fileSize":1}`,
	}
	for _, contentType := range []int32{constant.Picture, constant.Sound, constant.Video, constant.File} {
		t.Run(fmt.Sprint(contentType), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("frozen media retry entered upload preparation: %v", r)
				}
			}()
			ctx := ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self"})
			d, err := db.NewDataBase(ctx, "self", t.TempDir(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close(ctx)
			id := "si_peer_self"
			if err = d.InsertConversation(ctx, &model_struct.LocalConversation{ConversationID: id, UserID: "peer", ConversationType: constant.SingleChatType}); err != nil {
				t.Fatal(err)
			}
			if err = d.BatchInsertMessageList(ctx, id, []*model_struct.LocalChatLog{{ClientMsgID: "media", Status: constant.MsgStatusSendFailed}}); err != nil {
				t.Fatal(err)
			}
			reached := false
			conn := &interaction.LongConnMgr{}
			conn.SetSyncRequestGuard(func(context.Context, int) error { reached = true; return sdkerrs.ErrNetworkTimeOut })
			c := NewConversation(ctx, conn, d, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
			c.syncV2 = &conversationSyncV2Runtime{store: d}
			c.syncV2.mode.Store(syncV2Enabled)
			original := &sdkws.MsgData{ClientMsgID: "media", CreateTime: 123, SendID: "self", RecvID: "peer", SessionType: constant.SingleChatType, ContentType: contentType, Content: []byte(bodies[contentType])}
			payload, err := proto.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = d.FreezeSendRequest(ctx, id, "media", payload); err != nil {
				t.Fatal(err)
			}
			retry := LocalChatLogToMsgStruct(MsgDataToLocalChatLog(original))
			retry.Status = constant.MsgStatusSendFailed
			got, err := c.sendMessage(ctx, retry, "peer", "", nil, false)
			if !reached || !sdkerrs.ErrNetworkTimeOut.Is(err) || got == nil || got.ClientMsgID != "media" {
				t.Fatalf("frozen media failed before original transport: reached=%v err=%v", reached, err)
			}
		})
	}
}
