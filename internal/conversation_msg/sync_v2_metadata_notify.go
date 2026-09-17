package conversation_msg

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/sdkerrs"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/tools/log"
)

func (c *Conversation) notifyConversationSyncV2Metadata(ctx context.Context, before map[string]*model_struct.LocalConversation, checkpoint *model_struct.LocalConversationSyncV2) error {
	for attempt := 0; ; attempt++ {
		err := c.notifyConversationSyncV2MetadataOnce(ctx, before, checkpoint)
		if err == nil || !errors.Is(err, sdkerrs.ErrSyncV2StateChanged) || attempt == 5 {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		log.ZWarn(ctx, "sync v2 metadata state changed; retrying projection", err, "attempt", attempt+1, "selected", len(checkpoint.SelectedIDs), "revision", checkpoint.Revision)
		time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
	}
}

// Metadata refresh can already run in the conversation queue. Publish inline;
// enqueueing and awaiting another task here would deadlock that consumer.
func (c *Conversation) notifyConversationSyncV2MetadataOnce(ctx context.Context, before map[string]*model_struct.LocalConversation, checkpoint *model_struct.LocalConversationSyncV2) error {
	epochs, err := c.syncV2.store.GetConversationSyncV2Epochs(ctx, checkpoint.SelectedIDs)
	if err != nil {
		return err
	}
	changedAny := false
	for start := 0; start < len(checkpoint.SelectedIDs); start += 300 {
		rows, err := c.db.GetMultipleConversationDB(ctx, checkpoint.SelectedIDs[start:min(start+300, len(checkpoint.SelectedIDs))])
		if err != nil {
			return err
		}
		var changed, projections []*model_struct.LocalConversation
		for _, row := range rows {
			old := before[row.ConversationID]
			if old == nil || old.MinSeq != row.MinSeq || old.MaxSeq != row.MaxSeq || !old.IsPinned && row.IsPinned {
				messages, err := c.syncV2.store.GetConversationSyncV2LatestMessage(ctx, row.ConversationID, row.MinSeq, row.MaxSeq)
				if err != nil {
					return err
				}
				row.LatestMsg = ""
				// Clearing history must not hide the conversation: paging uses the
				// existing timestamp as its visibility/order anchor, like ClearConversation.
				if len(messages) > 0 {
					message := LocalChatLogToMsgStruct(messages[0])
					row.LatestMsg = utils.StructToJsonString(message)
					row.LatestMsgSendTime = message.SendTime
				}
				projections = append(projections, row)
			}
			if !reflect.DeepEqual(old, row) {
				changed = append(changed, row)
			}
		}
		if len(projections) > 0 {
			if err = c.syncV2.store.ApplyConversationSyncV2Projection(ctx, checkpoint, epochs, projections); err != nil {
				return err
			}
		}
		visibleChanges := changed[:0]
		for _, row := range changed {
			if row.LatestMsgSendTime > 0 {
				visibleChanges = append(visibleChanges, row)
			}
		}
		changed = visibleChanges
		if len(changed) > 0 {
			c.ConversationListener().OnConversationChanged(utils.StructToJsonString(changed))
			changedAny = true
		}
		// Successful batches must not be replayed if a later batch is invalidated.
		for _, row := range rows {
			before[row.ConversationID] = row
		}
	}
	if changedAny {
		total, err := c.db.GetTotalUnreadMsgCountDB(ctx)
		if err != nil {
			return err
		}
		c.ConversationListener().OnTotalUnreadMessageCountChanged(total)
	}
	return nil
}
