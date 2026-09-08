package syncv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// Selection is published only after both scopes have been read completely.
// Version is the pre-scan metadata version, not a message/read event watermark.
type Selection struct {
	ChatIDs         []string
	NotificationIDs []string
	IncludeSelf     bool
	VersionID       string
	Version         uint64
	Mode            string
	// ReleaseError is diagnostic only; the server TTL remains the cleanup fallback.
	ReleaseError error
}

func (c *Client) ReadSnapshot(ctx context.Context, op, userID string, pageSize int32) (selection *Selection, err error) {
	if pageSize < 1 || pageSize > 500 {
		return nil, fmt.Errorf("sync v2: invalid page size")
	}
	snapshotID := ""
	defer func() {
		if snapshotID == "" {
			return
		}
		// A cancelled traversal still releases its scarce server reservation.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if releaseErr := c.Release(cleanup, op, &pb.ReleaseRequest{UserID: userID, SnapshotID: snapshotID}); releaseErr != nil {
			if selection != nil {
				selection.ReleaseError = releaseErr
			} else {
				err = errors.Join(err, releaseErr)
			}
		}
	}()
	result := new(Selection)
	for _, scope := range []string{"chat", "notifications"} {
		cursor := ""
		seenCursors := map[string]bool{}
		seenIDs := map[string]bool{}
		var ids []string
		var total int64 = -1
		for {
			page, pageErr := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: userID, SnapshotID: snapshotID, Cursor: cursor, PageSize: pageSize, Scope: scope})
			if pageErr != nil {
				return nil, pageErr
			}
			if snapshotID == "" {
				snapshotID = page.SnapshotID
				result.VersionID, result.Version, result.Mode = page.VersionID, page.Version, page.Mode
			}
			if snapshotID == "" || page.SnapshotID != snapshotID || page.Scope != scope || page.PolicyVersion != PolicyVersion || page.VersionID != result.VersionID || page.Version != result.Version || page.Mode != result.Mode || (page.Mode != "full" && page.Mode != "filtered") {
				return nil, fmt.Errorf("sync v2: inconsistent snapshot metadata")
			}
			if total == -1 {
				total = page.Total
			}
			if page.Total != total || total < 0 || total > 100000 || len(page.ConversationIDs) > int(pageSize) {
				return nil, fmt.Errorf("sync v2: invalid snapshot page size or total")
			}
			for _, id := range page.ConversationIDs {
				if id == "" || seenIDs[id] {
					return nil, fmt.Errorf("sync v2: invalid or duplicate snapshot ID")
				}
				seenIDs[id] = true
				ids = append(ids, id)
			}
			if page.IncludeSelf != (scope == "notifications" && cursor == "") {
				return nil, fmt.Errorf("sync v2: invalid self notification declaration")
			}
			if page.IncludeSelf {
				result.IncludeSelf = true
			}
			if !page.HasMore {
				if int64(len(ids)) != total || page.Cursor != "" {
					return nil, fmt.Errorf("sync v2: incomplete snapshot")
				}
				break
			}
			if page.Cursor == "" || seenCursors[page.Cursor] || len(page.ConversationIDs) == 0 || int64(len(ids)) >= total {
				return nil, fmt.Errorf("sync v2: non-advancing snapshot cursor")
			}
			seenCursors[page.Cursor] = true
			cursor = page.Cursor
		}
		if scope == "chat" {
			result.ChatIDs = ids
		} else {
			result.NotificationIDs = ids
		}
	}
	return result, nil
}
