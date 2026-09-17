package syncv2

import (
	"context"
	"sync"
)

type streamProgressKey struct{}

func withStreamProgress(ctx context.Context, progress func(int, int)) context.Context {
	return context.WithValue(ctx, streamProgressKey{}, progress)
}

type stateProgressKey struct{}

func withStateProgress(ctx context.Context, progress func(int, int)) context.Context {
	return context.WithValue(ctx, stateProgressKey{}, progress)
}

// DetailProgress is scoped to a single stage. A nil total means the server has
// not supplied a bounded workload; it must not be inferred from percentages.
type DetailProgress struct {
	CurrentGroupMembers *int   `json:"currentGroupMembers,omitempty"`
	Stage               string `json:"stage"`
	Completed           int    `json:"completed"`
	Total               *int   `json:"total,omitempty"`
	Unit                string `json:"unit,omitempty"`
}
type detailProgressKey struct{}

func WithDetailProgress(ctx context.Context, report func(DetailProgress)) context.Context {
	return context.WithValue(ctx, detailProgressKey{}, report)
}
func ReportDetailProgress(ctx context.Context, stage, unit string, done int, total *int) {
	if ctx.Err() != nil {
		return
	}
	if report, ok := ctx.Value(detailProgressKey{}).(func(DetailProgress)); ok {
		report(DetailProgress{Stage: stage, Unit: unit, Completed: done, Total: total})
	}
}
func reportCount(ctx context.Context, stage, unit string, done, total int) {
	ReportDetailProgress(ctx, stage, unit, done, &total)
}

type storedMessagesKey struct{}

// Each wrapper owns one synchronization round, including its internal retries.
func WithStoredMessages(ctx context.Context, report func(int)) context.Context {
	var mu sync.Mutex
	seen := make(map[string]map[string]bool)
	return context.WithValue(ctx, storedMessagesKey{}, func(conversationID string, ids []string) {
		mu.Lock()
		defer mu.Unlock()
		if seen[conversationID] == nil {
			seen[conversationID] = make(map[string]bool)
		}
		added := 0
		for _, id := range ids {
			if !seen[conversationID][id] {
				seen[conversationID][id] = true
				added++
			}
		}
		if added > 0 {
			report(added)
		}
	})
}

// Called only after the chat transaction has committed. Notification processing
// is deliberately excluded from the displayed message count.
func ReportStoredMessages(ctx context.Context, conversationID string, ids []string) {
	if report, ok := ctx.Value(storedMessagesKey{}).(func(string, []string)); ok {
		report(conversationID, ids)
	}
}

func ReportGroupMemberProgress(ctx context.Context, done, total int, members *int) {
	if ctx.Err() != nil {
		return
	}
	if report, ok := ctx.Value(detailProgressKey{}).(func(DetailProgress)); ok {
		report(DetailProgress{Stage: "group_members", Unit: "groups", Completed: done, Total: &total, CurrentGroupMembers: members})
	}
}
