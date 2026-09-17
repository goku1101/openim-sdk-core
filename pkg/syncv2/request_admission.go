package syncv2

import (
	"context"
	"sync"
)

type foregroundReadKey struct{}

// WithForegroundRead prioritizes reads for an explicit user operation. It does
// not increase server concurrency or interrupt a request already in flight.
func WithForegroundRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, foregroundReadKey{}, true)
}

type admissionWaiter struct {
	ready   chan struct{}
	granted bool
}

// Bounded in-flight requests. After four foreground grants, a waiting background
// read gets a turn, so an active chat cannot starve durable catchup forever.
type requestAdmission struct {
	mu                     sync.Mutex
	active                 int
	limit                  int // zero defaults to one for serial control operations
	foreground, background []*admissionWaiter
	foregroundGrants       int
}

func (a *requestAdmission) acquire(ctx context.Context) error {
	a.mu.Lock()
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return err
	}
	if a.active < max(1, a.limit) {
		a.active++
		a.mu.Unlock()
		return nil
	}
	w := &admissionWaiter{ready: make(chan struct{})}
	foreground, _ := ctx.Value(foregroundReadKey{}).(bool)
	if foreground {
		a.foreground = append(a.foreground, w)
	} else {
		a.background = append(a.background, w)
	}
	a.mu.Unlock()
	select {
	case <-w.ready:
		if err := ctx.Err(); err != nil {
			a.release()
			return err
		}
		return nil
	case <-ctx.Done():
		a.mu.Lock()
		granted := w.granted
		if !granted {
			queue := &a.background
			if foreground {
				queue = &a.foreground
			}
			for i, item := range *queue {
				if item == w {
					copy((*queue)[i:], (*queue)[i+1:])
					(*queue)[len(*queue)-1] = nil
					*queue = (*queue)[:len(*queue)-1]
					break
				}
			}
		}
		a.mu.Unlock()
		if granted {
			a.release()
		}
		return ctx.Err()
	}
}

func (a *requestAdmission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	queue := &a.background
	if len(a.foreground) > 0 && (len(a.background) == 0 || a.foregroundGrants < 4) {
		queue = &a.foreground
		a.foregroundGrants++
	} else {
		a.foregroundGrants = 0
	}
	if len(*queue) == 0 {
		a.active--
		return
	}
	w := (*queue)[0]
	(*queue)[0] = nil
	*queue = (*queue)[1:]
	w.granted = true
	close(w.ready)
}
