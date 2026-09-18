package syncv2

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

type priorityDirectoryStore interface {
	SaveConversationSyncV2DirectoryPage(context.Context, *model_struct.LocalConversationSyncV2, *model_struct.ConversationSyncV2Task, *model_struct.LocalConversationSyncV2Page) error
	GetConversationSyncV2DirectoryPage(context.Context, string, int) (*model_struct.ConversationSyncV2TaskPage, error)
}

func directoryCaptured(t *model_struct.ConversationSyncV2Task) bool {
	return t.Directory != nil && t.Directory.Complete
}

// Once the homepage is available, capture only IDs before doing slow tail work.
// Each page and its download cursor commit together. At most one page is read.
func (c *Client) capturePriorityDirectory(ctx context.Context, op string, cp *model_struct.LocalConversationSyncV2, caps *pb.Capabilities, store CoordinatorStore) (*model_struct.LocalConversationSyncV2, error) {
	spool, ok := store.(priorityDirectoryStore)
	if !ok {
		return nil, fmt.Errorf("sync v2: directory storage unavailable")
	}
	task := cp.NotificationTask
	if task.Directory == nil {
		task.Directory = &model_struct.ConversationSyncV2Directory{NotificationProcessed: task.NotificationProcessed, ChatProcessed: task.ChatProcessed, NotificationCursor: task.NotificationCursor, ChatCursor: task.ChatCursor, NotificationsDone: task.NotificationsDone, ChatsDone: task.ChatsDone}
	}
	d := task.Directory
	for !d.NotificationsDone || !d.ChatsDone {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		scan := *task
		scan.NotificationProcessed = d.NotificationProcessed
		scan.ChatProcessed = d.ChatProcessed
		scan.NotificationCursor = d.NotificationCursor
		scan.ChatCursor = d.ChatCursor
		page := task.Page
		if page == nil {
			scope, cursor := "notifications", d.NotificationCursor
			if d.NotificationsDone {
				scope, cursor = "chat", d.ChatCursor
			}
			response, err := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: cp.UserID, SnapshotID: cp.SnapshotID, Scope: scope, Cursor: cursor, PageSize: min(caps.MaxPageSize, 500)})
			if err != nil {
				return nil, err
			}
			if response.Scope != scope {
				return nil, fmt.Errorf("sync v2: directory scope mismatch")
			}
			validation := *cp
			validation.NotificationTask = &scan
			if err = setPriorityPage(&validation, response, min(caps.MaxPageSize, 500)); err != nil {
				return nil, err
			}
			page = scan.Page
			task.ChatTotal = scan.ChatTotal
		}
		if page.Scope == "notifications" {
			d.NotificationProcessed += len(page.IDs) - page.Offset
			d.NotificationCursor = page.Cursor
			d.NotificationsDone = !page.HasMore
		} else {
			d.ChatProcessed += len(page.IDs) - page.Offset
			d.ChatCursor = page.Cursor
			d.ChatsDone = !page.HasMore
		}
		row := &model_struct.LocalConversationSyncV2Page{Generation: cp.Generation, Position: d.Pages, Page: page}
		d.Pages++
		d.Complete = d.NotificationsDone && d.ChatsDone
		task.Page = nil
		fresh, err := store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		if fresh.Generation != cp.Generation {
			return nil, fmt.Errorf("sync v2: directory superseded")
		}
		if err = spool.SaveConversationSyncV2DirectoryPage(ctx, fresh, task, row); err != nil {
			return nil, err
		}
		cp, err = store.GetConversationSyncV2(ctx)
		if err != nil {
			return nil, err
		}
		var total *int
		if task.ChatTotal >= 0 {
			n := task.NotificationTotal + task.ChatTotal
			total = &n
		}
		ReportDetailProgress(ctx, "snapshot", "conversations", d.NotificationProcessed+d.ChatProcessed, total)
	}
	if !d.Complete {
		d.Complete = true
		cp.NotificationTask = task
		if err := store.SaveConversationSyncV2(ctx, cp); err != nil {
			return nil, err
		}
	}
	return cp, nil
}

// Percentages are stage weights, never estimates of remaining wall-clock time.
func PriorityProgress(t *model_struct.ConversationSyncV2Task) int {
	if t == nil {
		return 1
	}
	if !t.NotificationsDone {
		if t.NotificationTotal == 0 {
			return 10
		}
		return 10 + 40*t.NotificationProcessed/t.NotificationTotal
	}
	if !t.ChatsDone {
		if t.ChatTotal <= 0 {
			return 50
		}
		return 50 + 35*t.ChatProcessed/t.ChatTotal
	}
	return 85
}
func priorityPageContext(ctx context.Context, t *model_struct.ConversationSyncV2Task, scope string, hooks CoordinatorHooks) context.Context {
	base, total, stage, unit := t.NotificationProcessed, t.NotificationTotal, "notifications", "streams"
	if scope == "chat" {
		base, total, stage, unit = t.ChatProcessed, t.ChatTotal, "messages", "conversations"
	}
	return WithDetailProgress(ctx, func(d DetailProgress) {
		if d.Stage != stage {
			return
		}
		done := base + d.Completed
		reportCount(ctx, stage, unit, done, total)
		copyTask := *t
		if scope == "chat" {
			copyTask.ChatProcessed = done
		} else {
			copyTask.NotificationProcessed = done
		}
		if hooks.Progress != nil {
			hooks.Progress(PriorityProgress(&copyTask))
		}
	})
}
