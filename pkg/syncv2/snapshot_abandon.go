package syncv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"gorm.io/gorm"
)

// AbandonSnapshot is for explicit cancellation, never for a transport reset.
// The releasing marker survives a failed release and prevents paging that task again.
func (c *Client) AbandonSnapshot(ctx context.Context, op, user string, store CoordinatorStore) error {
	cp, err := store.GetConversationSyncV2(ctx)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if cp == nil || cp.NotificationTask == nil {
		return nil
	}
	if cp.UserID != user {
		return fmt.Errorf("sync v2: snapshot owner mismatch")
	}
	cp.NotificationTask.Status = "releasing"
	if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
		return err
	}
	if cp.SnapshotID == "" && cp.NotificationTask.CreateOperationID != "" {
		// Recover a possibly lost creation response using its original identity.
		response, readErr := c.Snapshot(ctx, cp.NotificationTask.CreateOperationID, &pb.SnapshotRequest{UserID: user, Scope: "notifications", NotificationPriorityPrefix: true})
		if readErr != nil {
			return readErr
		}
		if response.SnapshotID == "" {
			return fmt.Errorf("sync v2: missing snapshot owner")
		}
		cp.SnapshotID = response.SnapshotID
		if err = store.SaveConversationSyncV2(ctx, cp); err != nil {
			return err
		}
	}
	if cp.SnapshotID != "" {
		if err = c.releasePrioritySnapshot(ctx, op, user, cp.SnapshotID); err != nil {
			return err
		}
	}
	cp.NotificationTask = nil
	cp.SnapshotID = ""
	cp.ChangeCursor = ""
	cp.RecoveryBaselineReady = false
	cp.Stage = "snapshot"
	return store.SaveConversationSyncV2(ctx, cp)
}
