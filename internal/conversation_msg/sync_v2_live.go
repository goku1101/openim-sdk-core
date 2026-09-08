package conversation_msg

import (
	"context"
	"fmt"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
)

// Live delivery acknowledges storage first and suppresses callbacks for rows
// already present, including self-send acknowledgements. It does not add unread
// counts: V2 authoritative state owns that value.
func (c *Conversation) consumeConversationSyncV2Live(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
	return c.consumeConversationSyncV2LiveSelected(ctx, progress, messages, nil)
}

func (c *Conversation) consumeConversationSyncV2LiveSelected(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData, notifyIDs map[string]bool) error {
	if progress == nil {
		return fmt.Errorf("sync v2: missing live progress")
	}
	if progress.Notifications {
		return c.ConsumeConversationSyncV2(ctx, progress, messages)
	}
	return common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
		ids := make([]string, 0, len(messages))
		for _, message := range messages {
			if message == nil {
				return fmt.Errorf("sync v2: invalid live message")
			}
			ids = append(ids, message.ClientMsgID)
		}
		existing, err := c.db.GetMessagesByClientMsgIDs(ctx, progress.ConversationID, ids)
		if err != nil {
			return err
		}
		seen := make(map[string]bool, len(existing))
		for _, row := range existing {
			seen[row.ClientMsgID] = true
		}
		if err = c.consumeConversationSyncV2(ctx, progress, messages); err != nil {
			return err
		}
		var incoming sdk_struct.NewMsgList
		for _, message := range messages {
			if notifyIDs != nil && !notifyIDs[message.ClientMsgID] || seen[message.ClientMsgID] || message.Status >= constant.MsgStatusHasDeleted || !utils.GetSwitchFromOptions(message.Options, constant.IsConversationUpdate) {
				continue
			}
			seen[message.ClientMsgID] = true
			row, err := c.db.GetMessage(ctx, progress.ConversationID, message.ClientMsgID)
			if err != nil {
				return err
			}
			if row.Status >= constant.MsgStatusHasDeleted {
				continue
			}
			incoming = append(incoming, LocalChatLogToMsgStruct(row))
		}
		if len(incoming) == 0 {
			return nil
		}
		conversation, err := c.db.GetConversation(ctx, progress.ConversationID)
		if err != nil {
			return err
		}
		changed := map[string]*model_struct.LocalConversation{progress.ConversationID: conversation}
		if c.batchMsgListener() != nil {
			c.batchNewMessages(ctx, incoming, changed, nil, nil)
		} else {
			c.newMessage(ctx, incoming, changed, nil, nil)
		}
		return nil
	})
}
