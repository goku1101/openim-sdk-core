package interaction

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/db_interface"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
)

type startupSeqDB struct {
	db_interface.DataBase
	ids             []string
	checks          atomic.Int64
	notifications   int
	version         *model_struct.LocalAppSDKVersion
	versionErr      error
	idsErr          error
	notificationErr error
}

func (d *startupSeqDB) GetAllConversationIDList(context.Context) ([]string, error) {
	return d.ids, d.idsErr
}
func (d *startupSeqDB) CheckConversationNormalMsgSeq(context.Context, string) (int64, error) {
	d.checks.Add(1)
	return 12, nil
}
func (d *startupSeqDB) GetNotificationAllSeqs(context.Context) ([]*model_struct.NotificationSeqs, error) {
	d.notifications++
	return []*model_struct.NotificationSeqs{{ConversationID: "n_c", Seq: 7}}, d.notificationErr
}

func TestV2StartupDefersLegacySequenceScan(t *testing.T) {
	ctx := context.Background()
	d := &startupSeqDB{}
	for i := 0; i < 1000; i++ {
		d.ids = append(d.ids, fmt.Sprintf("c_%d", i))
	}
	m, err := NewMsgSyncer(ctx, make(chan common.Cmd2Value, 10), nil, "user", nil, d, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	m.SetConversationSyncV2(func(context.Context, bool) (bool, error) { return true, nil })
	m.doConnected(ctx)
	m.SetConversationSyncV2Push(func(context.Context, *sdkws.PushMessages) (bool, error) { return true, nil })
	m.doPushMsg(ctx, &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"c": {Msgs: []*sdkws.MsgData{{Seq: 1}}}}})
	if d.checks.Load() != 0 || d.notifications != 0 {
		t.Fatalf("V2 startup loaded legacy cursors: chat reads=%d, notification reads=%d", d.checks.Load(), d.notifications)
	}
	m.SetConversationSyncV2Push(func(context.Context, *sdkws.PushMessages) (bool, error) { return false, nil })
	for i := 0; i < 2; i++ {
		m.doPushMsg(ctx, &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"c": {Msgs: []*sdkws.MsgData{{Seq: 0}}}}})
	}
	if d.checks.Load() != 1000 || d.notifications != 1 {
		t.Fatalf("explicit legacy fallback must load once: %d/%d", d.checks.Load(), d.notifications)
	}
	if m.syncedMaxSeqs["c_0"] != 12 || m.syncedMaxSeqs["n_c"] != 7 || len(m.conversationCh) != 2 {
		t.Fatal("legacy cursors or transient delivery lost")
	}
}

func (d *startupSeqDB) GetAppSDKVersion(context.Context) (*model_struct.LocalAppSDKVersion, error) {
	return d.version, d.versionErr
}

func TestStartupInstallWindowAndLegacyInitialization(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, tt := range []struct {
			name    string
			ids     []string
			version *model_struct.LocalAppSDKVersion
			missing bool
			fresh   bool
		}{
			{name: "fresh", missing: true, fresh: true},
			{name: "unfinished-install", version: &model_struct.LocalAppSDKVersion{}, fresh: true},
			{name: "installed-empty", version: &model_struct.LocalAppSDKVersion{Installed: true}},
			{name: "retained-conversations", ids: []string{"c"}},
		} {
			t.Run(fmt.Sprintf("%s/deferred=%t", tt.name, deferred), func(t *testing.T) {
				d := &startupSeqDB{ids: tt.ids, version: tt.version}
				if tt.missing {
					d.versionErr = errs.ErrRecordNotFound
				}
				m, err := NewMsgSyncer(context.Background(), make(chan common.Cmd2Value, 10), nil, "user", nil, d, 0, deferred)
				if err != nil {
					t.Fatal(err)
				}
				if m.reinstalled != tt.fresh {
					t.Fatalf("install window changed: got fresh=%t", m.reinstalled)
				}
				expected := int64(len(tt.ids))
				if deferred {
					expected = 0
				}
				if d.checks.Load() != expected {
					t.Fatalf("unexpected eager scan: %d", d.checks.Load())
				}
				if deferred {
					calls := 0
					m.SetConversationSyncV2(func(_ context.Context, fresh bool) (bool, error) {
						calls++
						if fresh != tt.fresh {
							t.Error("V2 received wrong install window")
						}
						return false, nil
					})
					if m.tryConversationSyncV2(context.Background()) {
						t.Fatal("explicit legacy result was swallowed")
					}
					if calls != 1 || d.notifications != 1 || d.checks.Load() != int64(len(tt.ids)) {
						t.Fatal("legacy fallback did not initialize cursors")
					}
				}
			})
		}
	}
}

func TestStartupSequenceErrorsDoNotOpenLegacyPath(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, deferred := range []bool{false, true} {
		for _, d := range []*startupSeqDB{{idsErr: failure}, {versionErr: failure}} {
			m, err := NewMsgSyncer(context.Background(), nil, nil, "user", nil, d, 0, deferred)
			if !errors.Is(err, failure) || m != nil {
				t.Fatalf("initialization error lost: %v", err)
			}
		}
	}
	d := &startupSeqDB{ids: []string{"c"}}
	m, err := NewMsgSyncer(context.Background(), make(chan common.Cmd2Value, 10), nil, "user", nil, d, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	// Negotiation failure must not read any old cursors or emit old sync flags.
	m.SetConversationSyncV2(func(context.Context, bool) (bool, error) { return false, failure })
	if !m.tryConversationSyncV2(context.Background()) || d.checks.Load() != 0 || len(m.conversationCh) != 0 {
		t.Fatal("V2 failure fell through to legacy")
	}
	m.SetConversationSyncV2(func(context.Context, bool) (bool, error) { return false, nil })
	d.notificationErr = failure
	if !m.tryConversationSyncV2(context.Background()) || !m.legacySeqPending || len(m.conversationCh) != 1 {
		t.Fatal("failed legacy initialization was allowed")
	}
	d.notificationErr = nil
	if m.tryConversationSyncV2(context.Background()) || m.legacySeqPending {
		t.Fatal("legacy initialization could not retry")
	}
}
