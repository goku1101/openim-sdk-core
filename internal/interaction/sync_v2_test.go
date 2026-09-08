package interaction

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
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
