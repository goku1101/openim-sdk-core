package conversation_msg

import (
	"context"
	"sync"
)

// The admission lock prevents Add from racing with shutdown's Wait.
type conversationTasks struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func (tasks *conversationTasks) start(ctx context.Context, fn func()) bool {
	tasks.mu.Lock()
	defer tasks.mu.Unlock()
	if tasks.closed || ctx.Err() != nil {
		return false
	}
	tasks.wg.Add(1)
	go func() { defer tasks.wg.Done(); fn() }()
	return true
}

func (tasks *conversationTasks) stopAndWait() {
	tasks.mu.Lock()
	tasks.closed = true
	tasks.mu.Unlock()
	tasks.wg.Wait()
}

// Called after cancelling the session and joining its event listeners, before DB close.
func (c *Conversation) WaitBackgroundTasks() {
	if c == nil {
		return
	}
	c.backgroundTasks.stopAndWait()
	// Once also serializes a concurrent first send with shutdown.
	c.getSender().stopAndWait()
}
