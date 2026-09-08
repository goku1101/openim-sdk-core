package syncv2

import "context"

type streamProgressKey struct{}

func withStreamProgress(ctx context.Context, progress func(int, int)) context.Context {
	return context.WithValue(ctx, streamProgressKey{}, progress)
}

type stateProgressKey struct{}

func withStateProgress(ctx context.Context, progress func(int, int)) context.Context {
	return context.WithValue(ctx, stateProgressKey{}, progress)
}
