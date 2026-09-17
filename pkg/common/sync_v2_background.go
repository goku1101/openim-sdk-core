package common

import "context"

type syncV2BackgroundKey struct{}

// WithSyncV2Background lets the message listener resume only after V2 has
// committed its priority prefix. Legacy synchronization remains synchronous.
func WithSyncV2Background(ctx context.Context, ready func()) context.Context {
	return context.WithValue(ctx, syncV2BackgroundKey{}, ready)
}
func NotifySyncV2Background(ctx context.Context) {
	if ready, ok := ctx.Value(syncV2BackgroundKey{}).(func()); ok {
		ready()
	}
}
