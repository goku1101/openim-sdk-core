package common

import (
	"context"
	"fmt"
	"time"

	"github.com/openimsdk/tools/log"
)

const CmdConversationSyncV2 = "conversationSyncV2"

type syncV2TimingKey struct{}

func WithSyncV2TaskTiming(ctx context.Context, report func(time.Duration, time.Duration, bool)) context.Context {
	if parent, ok := ctx.Value(syncV2TimingKey{}).(func(time.Duration, time.Duration, bool)); ok {
		child := report
		report = func(wait, work time.Duration, failed bool) { parent(wait, work, failed); child(wait, work, failed) }
	}
	return context.WithValue(ctx, syncV2TimingKey{}, report)
}

// The private closure keeps message contents out of generic queue logging.
// The one-slot acknowledgement remains writable if logout cancels the caller.
type ConversationSyncV2Task struct {
	consume  func() error
	result   chan error
	queuedAt time.Time
}

func (task *ConversationSyncV2Task) Run(ctx context.Context) {
	var err error
	started := time.Now()
	if !task.queuedAt.IsZero() && started.Sub(task.queuedAt) >= time.Second {
		log.ZInfo(ctx, "sync v2 consumer queue delayed", "waitMs", started.Sub(task.queuedAt).Milliseconds())
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sync v2: consumer panic: %v", recovered)
		}
		if elapsed := time.Since(started); elapsed >= time.Second {
			log.ZInfo(ctx, "sync v2 consumer task slow", "elapsedMs", elapsed.Milliseconds(), "succeeded", err == nil)
		}
		if report, ok := ctx.Value(syncV2TimingKey{}).(func(time.Duration, time.Duration, bool)); ok {
			report(started.Sub(task.queuedAt), time.Since(started), err != nil)
		}
		task.result <- err
	}()
	if err = ctx.Err(); err != nil {
		return
	}
	err = task.consume()
}

// RunConversationSyncV2 must be called outside the conversation queue worker.
// Success means consumption completed; merely placing the command is not enough.
func RunConversationSyncV2(ctx context.Context, queue chan Cmd2Value, consume func() error) error {
	if queue == nil {
		return ErrChanNil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	task := &ConversationSyncV2Task{consume: consume, result: make(chan error, 1), queuedAt: time.Now()}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case queue <- Cmd2Value{Cmd: CmdConversationSyncV2, Value: task, Ctx: ctx, Caller: GetCaller(2)}:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrTimeout
	}
	select {
	case err := <-task.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
