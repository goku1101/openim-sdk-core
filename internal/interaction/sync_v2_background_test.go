package interaction

import (
	"context"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"testing"
	"time"
)

func TestPriorityReadyReleasesListenerBeforeBackgroundFinishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tail := make(chan struct{})
	defer close(tail)
	m := &MsgSyncer{reinstalled: true}
	m.SetConversationSyncV2(func(ctx context.Context, fresh bool) (bool, error) {
		if !fresh {
			t.Error("lost first install")
		}
		common.NotifySyncV2Background(ctx)
		<-tail
		return true, nil
	})
	result := make(chan bool, 1)
	go func() { result <- m.tryConversationSyncV2(ctx) }()
	select {
	case handled := <-result:
		if !handled {
			t.Fatal("fell back to legacy")
		}
	case <-time.After(time.Second):
		t.Fatal("listener blocked on tail")
	}
	if m.reinstalled || m.v2BackgroundDone == nil {
		t.Fatal("missing background ownership")
	}
	if !m.tryConversationSyncV2(ctx) {
		t.Fatal("duplicate round fell back to legacy")
	}
}
