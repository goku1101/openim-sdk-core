package syncv2

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"google.golang.org/protobuf/proto"
)

type diagnosticsKey struct{}

type DiagnosticCounter struct {
	Calls      int64 `json:"calls"`
	Items      int64 `json:"items"`
	Failures   int64 `json:"failures"`
	DurationMS int64 `json:"durationMs"`
	duration   time.Duration
}

// Counters measure attempts, not unique messages. Durations may overlap and
// must not be summed across nested categories to infer wall-clock percentages.
// Keys are fixed code labels; no IDs, tokens, URLs or content are retained.
type Diagnostics struct {
	mu       sync.Mutex
	counters map[string]DiagnosticCounter
}

func WithDiagnostics(ctx context.Context) (context.Context, *Diagnostics) {
	d := &Diagnostics{counters: make(map[string]DiagnosticCounter)}
	return context.WithValue(ctx, diagnosticsKey{}, d), d
}

func RecordDiagnostic(ctx context.Context, key string, items int, elapsed time.Duration, failed bool) {
	d, _ := ctx.Value(diagnosticsKey{}).(*Diagnostics)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.counters[key]
	c.Calls++
	c.Items += int64(items)
	c.duration += elapsed
	if failed {
		c.Failures++
	}
	d.counters[key] = c
}

func (d *Diagnostics) Snapshot() map[string]DiagnosticCounter {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]DiagnosticCounter, len(d.counters))
	for k, v := range d.counters {
		v.DurationMS = v.duration.Milliseconds()
		out[k] = v
	}
	return out
}

func requestDiagnosticKind(req proto.Message) string {
	switch r := req.(type) {
	case *pb.BatchPullRequest:
		return "notification_batch_pull"
	case *pb.StateRequest:
		if r.Notifications {
			return "notification_state"
		}
		return "chat_state"
	case *pb.PullRequest:
		if r.Notifications {
			return "notification_pull"
		}
		return "chat_pull"
	case *pb.ChangeRequest:
		return "changes"
	case *pb.SnapshotRequest:
		return "snapshot"
	default:
		return "control"
	}
}

func (c *Client) invokeMeasured(ctx context.Context, op, route string, req, resp proto.Message) (raw []byte, err error) {
	started := time.Now()
	kind := requestDiagnosticKind(req)
	defer func() {
		RecordDiagnostic(ctx, "http_"+kind, 0, time.Since(started), err != nil)
		if err != nil {
			return
		}
		if result, ok := resp.(*pb.PullResponse); ok {
			request := req.(*pb.PullRequest)
			chat, business, system := 0, 0, 0
			for _, msg := range result.Messages {
				if msg == nil {
					continue
				}
				if !request.Notifications {
					chat++
				} else if msg.ContentType == constant.BusinessNotification {
					business++
				} else {
					system++
				}
			}
			if request.Notifications {
				RecordDiagnostic(ctx, "fetched_business_notification", business, 0, false)
				RecordDiagnostic(ctx, "fetched_system_notification", system, 0, false)
			} else {
				RecordDiagnostic(ctx, "fetched_chat", chat, 0, false)
			}
		}
	}()
	return c.invokeOnce(ctx, op, route, req, resp)
}
