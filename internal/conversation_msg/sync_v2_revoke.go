package conversation_msg

import (
	"context"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
)

// The V2 server persists revocation before publishing this notification and
// returns revoked content on later history reads. Only local quotes need work
// when the original message has never been loaded; do not fetch old chat history.
func (c *Conversation) revokeUnloadedConversationSyncV2Message(ctx context.Context, tips *sdkws.RevokeMsgTips) error {
	quotes, err := c.db.SearchAllMessageByContentType(ctx, tips.ConversationID, constant.Quote)
	if err != nil {
		return err
	}
	var event *sdk_struct.MessageRevoked
	for _, quote := range quotes {
		var elem sdk_struct.QuoteElem
		if err = utils.JsonStringToStruct(quote.Content, &elem); err != nil {
			return err
		}
		if elem.QuoteMessage == nil {
			return fmt.Errorf("sync v2: quote source missing")
		}
		if elem.QuoteMessage.ClientMsgID != tips.ClientMsgID {
			continue
		}
		revoked, err := c.messageRevocation(ctx, tips, MsgStructToLocalChatLog(elem.QuoteMessage))
		if err != nil {
			return err
		}
		if err = c.quoteMsgRevokeHandle(ctx, tips.ConversationID, quote, revoked); err != nil {
			return err
		}
		event = &revoked
	}
	if event != nil {
		c.msgListener().OnNewRecvMessageRevoked(utils.StructToJsonString(event))
	}
	return nil
}
