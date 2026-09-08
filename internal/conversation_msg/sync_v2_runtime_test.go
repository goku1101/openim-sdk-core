//go:build !js

package conversation_msg

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/internal/interaction"
	"github.com/openimsdk/openim-sdk-core/v3/open_im_sdk_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db"
	"github.com/openimsdk/protocol/sdkws"
)

type syncV2Listener struct{ starts, finishes, failures int }

func (l *syncV2Listener) OnSyncServerStart(bool)                    { l.starts++ }
func (l *syncV2Listener) OnSyncServerFinish(bool)                   { l.finishes++ }
func (l *syncV2Listener) OnSyncServerFailed(bool)                   { l.failures++ }
func (*syncV2Listener) OnSyncServerProgress(int)                    {}
func (*syncV2Listener) OnNewConversation(string)                    {}
func (*syncV2Listener) OnConversationChanged(string)                {}
func (*syncV2Listener) OnTotalUnreadMessageCountChanged(int32)      {}
func (*syncV2Listener) OnConversationUserInputStatusChanged(string) {}

func TestSyncV2RuntimeFailureNeverFinishesOrSelectsLegacy(t *testing.T) {
	for _, scenario := range []string{"unsupported", "capability-error", "snapshot-error", "mode-changed"} {
		t.Run(scenario, func(t *testing.T) {
			capabilityCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/conversation/sync_snapshot" && scenario == "mode-changed" {
					fmt.Fprint(w, `{"errCode":12,"errMsg":"SYNC_V2_DOWNSTREAM_VERSION"}`)
					return
				}
				if r.URL.Path != "/conversation/sync_capabilities" || scenario == "capability-error" {
					w.WriteHeader(404)
					return
				}
				capabilityCalls++
				if scenario == "unsupported" || scenario == "mode-changed" && capabilityCalls > 1 {
					fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":false}}`)
					return
				}
				fmt.Fprint(w, `{"errCode":0,"data":{"conversationSyncV2":true,"policyVersion":"conversation-sync-v2.2","maxBatchSize":500,"maxPageSize":500,"maxRequestBytes":65536,"snapshotTTLSeconds":600}}`)
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
			connection := &interaction.LongConnMgr{}
			c := NewConversation(ctx, connection, database, make(chan common.Cmd2Value, 10), nil, nil, nil, nil, nil)
			listener := &syncV2Listener{}
			c.SetConversationListener(func() open_im_sdk_callback.OnConversationListener { return listener })
			if !c.ConfigureConversationSyncV2(ctx) {
				t.Fatal("native login did not configure V2")
			}
			handled, err := c.SynchronizeConversationV2(ctx, true)
			if listener.finishes != 0 {
				t.Fatal("failed/unsupported V2 emitted success")
			}
			if scenario == "unsupported" {
				if err != nil || handled || c.syncV2.mode.Load() != syncV2Legacy || listener.starts != 0 || listener.failures != 0 {
					t.Fatalf("explicit unsupported did not hand off cleanly: %v %v %+v", handled, err, listener)
				}
			} else {
				if err == nil || !handled || c.syncV2.mode.Load() == syncV2Legacy || listener.failures != 1 {
					t.Fatalf("error fell back or hid failure: %v %v %+v", handled, err, listener)
				}
				if err = connection.SendReqWaitResp(ctx, &sdkws.GetMaxSeqReq{}, constant.GetNewestSeq, &sdkws.GetMaxSeqResp{}); err == nil {
					t.Fatal("failed negotiation allowed legacy full scan")
				}
				if (scenario == "snapshot-error" || scenario == "mode-changed") && listener.starts != 1 {
					t.Fatal("negotiated V2 did not emit one start")
				}
			}
		})
	}
}
