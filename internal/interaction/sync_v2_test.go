package interaction

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/protocol/sdkws"
)

func TestSyncV2OwnsConnectWakeupAndManualSync(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			calls := 0
			m := &MsgSyncer{ctx: context.Background(), conversationCh: make(chan common.Cmd2Value, 10)}
			m.SetConversationSyncV2(func(ctx context.Context, reinstalled bool) (bool, error) {
				calls++
				if failure {
					return false, errors.New("V2 request failed")
				}
				return true, nil
			})
			// No legacy connection exists: any WS fallback would panic this test.
			m.doConnected(context.Background())
			m.doWakeupDataSync(context.Background())
			m.doIMMessageSync(context.Background())
			if calls != 3 {
				t.Fatalf("V2 did not own every entry: %d", calls)
			}
			if len(m.conversationCh) != 0 {
				t.Fatal("legacy sync flags emitted during V2 handling")
			}
		})
	}
}

func TestSyncV2PushDoesNotFallbackAfterHandlingOrFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		m := &MsgSyncer{ctx: context.Background(), conversationCh: make(chan common.Cmd2Value, 10)}
		calls := 0
		push := &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"c": {Msgs: []*sdkws.MsgData{{Seq: 1}}}}}
		m.SetConversationSyncV2Push(func(ctx context.Context, got *sdkws.PushMessages) (bool, error) {
			calls++
			if got != push {
				t.Error("push replaced before V2 consumer")
			}
			if failure {
				return false, errors.New("V2 persistence failed")
			}
			return true, nil
		})
		// Legacy would write the absent syncedMaxSeqs map and panic.
		m.doPushMsg(context.Background(), push)
		if calls != 1 || len(m.conversationCh) != 0 {
			t.Fatalf("push ownership lost: calls=%d legacy events=%d", calls, len(m.conversationCh))
		}
	}
}

func TestSyncV2PushExplicitLegacyPreservesTransientDelivery(t *testing.T) {
	m := &MsgSyncer{ctx: context.Background(), conversationCh: make(chan common.Cmd2Value, 10)}
	m.SetConversationSyncV2Push(func(context.Context, *sdkws.PushMessages) (bool, error) { return false, nil })
	m.doPushMsg(context.Background(), &sdkws.PushMessages{Msgs: map[string]*sdkws.PullMsgs{"c": {Msgs: []*sdkws.MsgData{{Seq: 0}}}}})
	if len(m.conversationCh) != 1 {
		t.Fatal("explicit legacy transient event was lost")
	}
}
