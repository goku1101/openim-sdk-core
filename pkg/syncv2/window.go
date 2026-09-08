package syncv2

import "context"

type chatWindowKey struct{}

// WithChatWindow applies the SDK's existing recent-message budget to this
// synchronization call. Notifications and previously unresolved sequences are
// never truncated. Older chat history remains available through ReadSequencePage.
func WithChatWindow(ctx context.Context, count int64) context.Context {
	return context.WithValue(ctx, chatWindowKey{}, count)
}

func chatWindowStart(ctx context.Context, notifications bool, lower, upper int64) int64 {
	count, _ := ctx.Value(chatWindowKey{}).(int64)
	if notifications || count <= 0 || upper < lower {
		return lower
	}
	return max(lower, upper-min(count-1, upper-lower))
}
