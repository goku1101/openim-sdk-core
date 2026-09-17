package syncv2

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// Selection is published only after both scopes have been read completely.
// Version is the pre-scan metadata version, not a message/read event watermark.
type Selection struct {
	BaselineCursor  string
	SnapshotID      string
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
	first, err := c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: userID, PageSize: pageSize, Scope: "chat"})
	if err != nil {
		return nil, err
	}
	snapshotID = first.SnapshotID
	if snapshotID == "" || first.Scope != "chat" || first.PolicyVersion != PolicyVersion || (first.Mode != "full" && first.Mode != "filtered") {
		return nil, fmt.Errorf("sync v2: inconsistent snapshot metadata")
	}
	result := &Selection{BaselineCursor: first.BaselineCursor, SnapshotID: snapshotID, VersionID: first.VersionID, Version: first.Version, Mode: first.Mode}
	group, readCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		ids, _, readErr := c.readSnapshotScope(readCtx, op, userID, pageSize, "chat", result, first)
		result.ChatIDs = ids
		return readErr
	})
	group.Go(func() error {
		ids, self, readErr := c.readSnapshotScope(readCtx, op, userID, pageSize, "notifications", result, nil)
		result.NotificationIDs, result.IncludeSelf = ids, self
		return readErr
	})
	if err = group.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

// Scope cursors are independent within the same immutable server snapshot.
func (c *Client) readSnapshotScope(ctx context.Context, op, userID string, pageSize int32, scope string, result *Selection, first *pb.SnapshotResponse) ([]string, bool, error) {
	includeSelf := false
	cursor := ""
	seenCursors := map[string]bool{}
	seenIDs := map[string]bool{}
	var ids []string
	var total int64 = -1
	for {
		page := first
		first = nil
		var pageErr error
		if page == nil {
			page, pageErr = c.Snapshot(ctx, op, &pb.SnapshotRequest{UserID: userID, SnapshotID: result.SnapshotID, Cursor: cursor, PageSize: pageSize, Scope: scope})
		}
		if pageErr != nil {
			return nil, false, pageErr
		}
		if result.SnapshotID == "" || page.SnapshotID != result.SnapshotID || page.BaselineCursor != result.BaselineCursor || page.Scope != scope || page.PolicyVersion != PolicyVersion || page.VersionID != result.VersionID || page.Version != result.Version || page.Mode != result.Mode || (page.Mode != "full" && page.Mode != "filtered") {
			return nil, false, fmt.Errorf("sync v2: inconsistent snapshot metadata")
		}
		if total == -1 {
			total = page.Total
		}
		if page.Total != total || total < 0 || total > 100000 || len(page.ConversationIDs) > int(pageSize) {
			return nil, false, fmt.Errorf("sync v2: invalid snapshot page size or total")
		}
		for _, id := range page.ConversationIDs {
			if id == "" || seenIDs[id] {
				return nil, false, fmt.Errorf("sync v2: invalid or duplicate snapshot ID")
			}
			seenIDs[id] = true
			ids = append(ids, id)
		}
		if page.IncludeSelf != (scope == "notifications" && cursor == "") {
			return nil, false, fmt.Errorf("sync v2: invalid self notification declaration")
		}
		if page.IncludeSelf {
			includeSelf = true
		}
		if !page.HasMore {
			if int64(len(ids)) != total || page.Cursor != "" {
				return nil, false, fmt.Errorf("sync v2: incomplete snapshot")
			}
			break
		}
		if page.Cursor == "" || seenCursors[page.Cursor] || len(page.ConversationIDs) == 0 || int64(len(ids)) >= total {
			return nil, false, fmt.Errorf("sync v2: non-advancing snapshot cursor")
		}
		seenCursors[page.Cursor] = true
		cursor = page.Cursor
	}
	return ids, includeSelf, nil
}
