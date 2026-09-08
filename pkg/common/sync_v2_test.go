package common

import (
	"context"
	"errors"
	"testing"
)

func TestSyncV2QueueWaitsForConsumptionAndReturnsFailure(t *testing.T) {
	queue := make(chan Cmd2Value, 1)
	failure := errors.New("write failed")
	done := make(chan error, 1)
	go func() { done <- RunConversationSyncV2(context.Background(), queue, func() error { return failure }) }()
	cmd := <-queue
	select {
	case <-done:
		t.Fatal("enqueue was reported as consumption")
	default:
	}
	cmd.Value.(*ConversationSyncV2Task).Run(cmd.Ctx)
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("consumer error lost: %v", err)
	}
}
func TestSyncV2QueueDoesNotConsumeCancelledTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan Cmd2Value, 1)
	done := make(chan error, 1)
	go func() {
		done <- RunConversationSyncV2(ctx, queue, func() error { t.Error("cancelled task executed"); return nil })
	}()
	cmd := <-queue
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	cmd.Value.(*ConversationSyncV2Task).Run(cmd.Ctx)
}
