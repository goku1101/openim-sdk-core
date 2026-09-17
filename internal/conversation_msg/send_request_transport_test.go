//go:build !js

package conversation_msg

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

type retryTransportListener struct{}

func (retryTransportListener) OnConnecting()                 {}
func (retryTransportListener) OnConnectSuccess()             {}
func (retryTransportListener) OnConnectFailed(int32, string) {}
func (retryTransportListener) OnKickedOffline()              {}
func (retryTransportListener) OnUserTokenExpired()           {}
func (retryTransportListener) OnUserTokenInvalid(string)     {}

// Real loopback WebSocket, Gob and gzip. The fixture accepts the first request
// but drops its reply, so the actual SDK transport must reach its timeout.
// No external IM server or real user receives a message.
func TestSendLostReplyRetainsWireIdentityAfterRestart(t *testing.T) {
	for _, kind := range []string{"text", "uploaded-file"} {
		t.Run(kind, func(t *testing.T) {
			var mu sync.Mutex
			var first *sdkws.MsgData
			requests, publications := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer ws.Close()
				encoder, compressor := interaction.NewGobEncoder(), interaction.NewGzipCompressor()
				for {
					_, wire, err := ws.ReadMessage()
					if err != nil {
						return
					}
					wire, err = compressor.DecompressWithPool(wire)
					if err != nil {
						t.Error(err)
						return
					}
					var req interaction.GeneralWsReq
					if err = encoder.Decode(wire, &req); err != nil {
						t.Error(err)
						return
					}
					if req.ReqIdentifier != constant.SendMsg {
						t.Errorf("unexpected request %d", req.ReqIdentifier)
						return
					}
					var message sdkws.MsgData
					if err = proto.Unmarshal(req.Data, &message); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					requests++
					drop := first == nil
					if drop {
						first = proto.Clone(&message).(*sdkws.MsgData)
						publications++
					}
					equal := proto.Equal(first, &message)
					mu.Unlock()
					if !equal {
						t.Error("retry changed the accepted wire request")
					}
					if drop {
						continue
					}
					modified := proto.Clone(&message).(*sdkws.MsgData)
					modified.Content = []byte(`{"content":"server modified"}`)
					reply, err := proto.Marshal(&pbmsg.SendMsgResp{ClientMsgID: message.ClientMsgID, ServerMsgID: "server-original", SendTime: 456, Modify: modified})
					if err != nil {
						t.Error(err)
						return
					}
					data, err := encoder.Encode(interaction.GeneralWsResp{ReqIdentifier: req.ReqIdentifier, MsgIncr: req.MsgIncr, OperationID: req.OperationID, Data: reply})
					if err != nil {
						t.Error(err)
						return
					}
					data, err = compressor.CompressWithPool(data)
					if err != nil {
						t.Error(err)
						return
					}
					if err = ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
						t.Error(err)
						return
					}
				}
			}))
			defer server.Close()
			base := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "fixture", IMConfig: sdk_struct.IMConfig{WsAddr: "ws" + strings.TrimPrefix(server.URL, "http")}}), "lost-reply")
			dir := t.TempDir()
			database, err := db.NewDataBase(base, "self", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = database.Close(base) }()
			id := "si_peer_self"
			if err = database.InsertConversation(base, &model_struct.LocalConversation{ConversationID: id, UserID: "peer", ConversationType: constant.SingleChatType}); err != nil {
				t.Fatal(err)
			}
			connect := func() (*Conversation, func()) {
				ctx, cancel := context.WithCancel(base)
				conn := interaction.NewLongConnMgr(ctx, retryTransportListener{}, nil, make(chan common.Cmd2Value, 10), make(chan common.Cmd2Value, 10))
				conn.Run(ctx)
				stop := func() { cancel(); conn.Close(ctx) }
				deadline := time.Now().Add(3 * time.Second)
				for !conn.IsConnected() {
					if time.Now().After(deadline) {
						stop()
						t.Fatal("loopback did not connect")
					}
					time.Sleep(time.Millisecond)
				}
				c := NewConversation(ctx, conn, database, make(chan common.Cmd2Value, 20), nil, nil, nil, nil, nil)
				c.syncV2 = &conversationSyncV2Runtime{store: database}
				c.syncV2.mode.Store(syncV2Enabled)
				return c, stop
			}
			c, stop := connect()
			message := &sdk_struct.MsgStruct{ClientMsgID: "same-message", CreateTime: 123, SendTime: 123, Status: constant.MsgStatusSending, ContentType: constant.Text, TextElem: &sdk_struct.TextElem{Content: "original"}}
			if kind == "uploaded-file" {
				message.ContentType = constant.File
				message.FileElem = &sdk_struct.FileElem{FilePath: "/missing/already-uploaded.txt", UUID: "file", SourceURL: "https://example.invalid/file", FileName: "file.txt", FileSize: 1}
			}
			firstCtx, firstCancel := context.WithTimeout(base, 15*time.Second)
			failed, err := c.sendMessageNotOss(firstCtx, message, "peer", "", nil, false)
			firstCancel()
			stop()
			if !sdkerrs.ErrNetworkTimeOut.Is(err) || failed == nil || failed.ClientMsgID != "same-message" {
				t.Fatalf("expected uncertain transport timeout: %v", err)
			}
			if err = database.Close(base); err != nil {
				t.Fatal(err)
			}
			database, err = db.NewDataBase(base, "self", dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := database.GetMessage(base, id, "same-message")
			if err != nil {
				t.Fatal(err)
			}
			retry := LocalChatLogToMsgStruct(saved)
			if err = database.UpdateConversation(base, &model_struct.LocalConversation{ConversationID: id, IsPrivateChat: true, BurnDuration: 30}); err != nil {
				t.Fatal(err)
			}
			c, stop = connect()
			defer stop()
			retryCtx, cancel := context.WithTimeout(base, 5*time.Second)
			defer cancel()
			// Use the ordinary API on retry, including the media upload path.
			sent, err := c.sendMessage(retryCtx, retry, "peer", "", &sdkws.OfflinePushInfo{Title: "changed"}, false)
			if err != nil || sent == nil || sent.ServerMsgID != "server-original" || sent.SendTime != 456 {
				t.Fatalf("retry response: %v", err)
			}
			mu.Lock()
			count, published := requests, publications
			mu.Unlock()
			if count != 2 || published != 1 {
				t.Fatalf("requests=%d logical publications=%d", count, published)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				pending, err := database.GetSendRequest(base, id, "same-message")
				if err != nil {
					t.Fatal(err)
				}
				if pending == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("confirmed request not released")
				}
				time.Sleep(time.Millisecond)
			}
			saved, err = database.GetMessage(base, id, "same-message")
			if err != nil || saved.Status != constant.MsgStatusSendSuccess {
				t.Fatalf("confirmation not durable: %v", err)
			}
			if kind == "text" && sent.TextElem.Content != "original" {
				t.Fatal("Modify overwrote original message")
			}
			if kind == "uploaded-file" && sent.FileElem.SourceURL != "https://example.invalid/file" {
				t.Fatal("retry changed uploaded media")
			}
		})
	}
}
