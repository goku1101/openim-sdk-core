package conversation_msg

import (
	"context"
	"fmt"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/protocol/sdkws"
)

type conversationSyncV2MessageStore interface {
	GetConversationSyncV2Stream(context.Context, string) (*model_struct.LocalConversationSyncV2Stream, error)
	CommitConversationSyncV2Messages(context.Context, uint64, *model_struct.LocalConversationSyncV2Stream, []*model_struct.LocalChatLog) error
	CommitConversationSyncV2Notifications(context.Context, uint64, *model_struct.LocalConversationSyncV2Stream, []int64) error
}

// ConsumeConversationSyncV2 serializes message writes with existing conversation
// events. Its caller advances only after receiving the durable acknowledgement.
func (c *Conversation) ConsumeConversationSyncV2(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
	return common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error { return c.consumeConversationSyncV2(ctx, progress, messages) })
}

func (c *Conversation) consumeConversationSyncV2(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store, ok := c.db.(conversationSyncV2MessageStore)
	if !ok || progress == nil {
		return fmt.Errorf("sync v2: message persistence unavailable")
	}
	current, err := store.GetConversationSyncV2Stream(ctx, progress.ConversationID)
	if err != nil {
		return err
	}
	if current.Epoch != progress.Epoch || current.StateToken != progress.StateToken {
		return fmt.Errorf("sync v2: stale queued messages")
	}
	for _, message := range messages {
		if message == nil || message.ClientMsgID == "" || message.Seq < max(int64(1), progress.MinSeq) || progress.PermissionMaxSeq > 0 && message.Seq > progress.PermissionMaxSeq {
			return fmt.Errorf("sync v2: invalid queued message")
		}
	}
	if !progress.Notifications {
		rows := make([]*model_struct.LocalChatLog, 0, len(messages))
		// Use the existing history conversion, retaining original content bytes and
		// explicit server deletion status. Absent sequences are never fabricated.
		for _, message := range messages {
			rows = append(rows, MsgDataToLocalChatLog(message))
		}
		return store.CommitConversationSyncV2Messages(ctx, progress.Epoch, progress, rows)
	}
	ordered := append([]*sdkws.MsgData(nil), messages...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Seq < ordered[j].Seq })
	resolved := make([]int64, 0, len(ordered))
	for _, message := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		if message.Status < constant.MsgStatusHasDeleted {
			switch {
			case message.ContentType > constant.FriendNotificationBegin && message.ContentType < constant.FriendNotificationEnd:
				err = c.relation.DoNotification(ctx, message)
			case message.ContentType > constant.UserNotificationBegin && message.ContentType < constant.UserNotificationEnd:
				err = c.user.DoNotification(ctx, message)
			case message.ContentType > constant.GroupNotificationBegin && message.ContentType < constant.GroupNotificationEnd:
				err = c.group.DoNotification(ctx, message)
			default:
				err = c.DoNotification(ctx, message)
			}
			if err != nil {
				return err
			}
		}
		resolved = append(resolved, message.Seq)
	}
	return store.CommitConversationSyncV2Notifications(ctx, progress.Epoch, progress, resolved)
}
