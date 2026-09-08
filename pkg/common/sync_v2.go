package common

import (
	"context"
	"fmt"
	"time"
)

const CmdConversationSyncV2 = "conversationSyncV2"

// The private closure keeps message contents out of generic queue logging.
// The one-slot acknowledgement remains writable if logout cancels the caller.
type ConversationSyncV2Task struct {
	consume func() error
	result  chan error
}

func (task *ConversationSyncV2Task) Run(ctx context.Context) {
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sync v2: consumer panic: %v", recovered)
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
	task := &ConversationSyncV2Task{consume: consume, result: make(chan error, 1)}
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
