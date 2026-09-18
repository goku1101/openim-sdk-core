package conversation_msg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
)

func TestSenderShutdownJoinsInflightAndRejectsQueuedWork(t *testing.T) {
	// One worker makes the queued/in-flight boundary deterministic.
	sender := &messageSender{queue: make(chan *sendTask, 2)}
	sender.wg.Add(1)
	go sender.worker()
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	sender.queue <- &sendTask{ctx: context.Background(), exec: func(context.Context) (*sdk_struct.MsgStruct, error) {
		close(started)
		<-release
		return nil, errors.New("cancelled test task")
	}}
	<-started
	sender.queue <- &sendTask{ctx: context.Background(), exec: func(context.Context) (*sdk_struct.MsgStruct, error) {
		t.Error("queued task executed after shutdown")
		return nil, errors.New("unexpected")
	}}
	go func() { sender.stopAndWait(); close(exited) }()
	select {
	case <-exited:
		t.Fatal("shutdown returned before active task ended")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join workers")
	}
	if err := sender.submit(&sendTask{ctx: context.Background()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("late submit = %v", err)
	}
}
