package conversation_msg

import (
	"context"
	"testing"
	"time"
)

func TestLegacyBackgroundSyncJoinsBeforeShutdown(t *testing.T) {
	c := &Conversation{}
	ctx, cancel := context.WithCancel(context.Background())
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.runSyncFunctions(ctx, []func(context.Context) error{func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		<-release
		return ctx.Err()
	}}, asyncNoWait)
	<-started
	cancel()
	go func() { c.WaitBackgroundTasks(); close(done) }()
	select {
	case <-done:
		t.Fatal("database could close while old sync still runs")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background task was not joined")
	}
	if c.backgroundTasks.start(context.Background(), func() { t.Error("late work") }) {
		t.Fatal("accepted work after shutdown")
	}
}
