package interaction

import (
	"context"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/protocol/sdkws"
)

func TestCancelledRequestDoesNotBlockOnFullSendQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{}))
	conn := NewLongConnMgr(ctx, nil, nil, nil, nil)
	for i := 0; i < cap(conn.send); i++ {
		conn.send <- Message{}
	}
	done := make(chan error, 1)
	go func() { done <- conn.SendReqWaitResp(ctx, &sdkws.PushMessages{}, 0, &sdkws.PushMessages{}) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled request succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled request stuck on full send queue")
	}
}

func TestCancelledConnectionJoinsAllPumps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := NewLongConnMgr(ctx, nil, nil, nil, nil)
	cancel()
	conn.Run(ctx)
	done := make(chan struct{})
	go func() { conn.WaitStopped(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection pumps survived cancelled session")
	}
}
