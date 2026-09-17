package conversation_msg

import (
	"context"
	"errors"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
)

// ProcessConversationSyncV2Push is invoked by MsgSyncer, never its consumer.
// A failed V2 push remains V2; only negotiated legacy permits the old route.
func (c *Conversation) processConversationSyncV2Push(ctx context.Context, push *sdkws.PushMessages, ephemeral bool) (bool, error) {
	if c.syncV2 == nil || c.syncV2.mode.Load() == syncV2Legacy {
		return false, nil
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	newChats := make(map[string]map[string]bool)
	if !ephemeral {
		for id, batch := range push.Msgs {
			ids := make(map[string]bool)
			for _, message := range batch.Msgs {
				if message.Seq > 0 {
					ids[message.ClientMsgID] = true
				}
			}
			newChats[id] = ids
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		err := c.processConversationSyncV2PushRound(ctx, push, ephemeral && attempt == 0, newChats)
		if err == nil {
			return true, nil
		}
		if attempt == 1 || ctx.Err() != nil {
			return true, err
		}
		action := syncv2.RecoveryFor(err)
		if action == syncv2.RebuildSnapshot || action == syncv2.Renegotiate {
			if _, err = c.SynchronizeConversationV2(ctx, false); err != nil {
				return true, err
			}
			continue
		}
		if action != syncv2.Retry && action != syncv2.RefreshState {
			return true, err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return true, ctx.Err()
		case <-timer.C:
		}
	}
	return true, syncv2.ErrStatePending
}

func (c *Conversation) processConversationSyncV2PushRound(ctx context.Context, push *sdkws.PushMessages, ephemeral bool, newChats map[string]map[string]bool) error {
	runtime := c.syncV2
	if err := runtime.acquireRun(ctx); err != nil {
		return err
	}
	defer runtime.releaseRun()
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if runtime.mode.Load() != syncV2Enabled || caps == nil {
		return syncv2.ErrStatePending
	}
	cp, err := runtime.store.GetConversationSyncV2(ctx)
	if err != nil {
		return err
	}
	plan, err := syncv2.PlanPush(cp, push)
	if err != nil {
		return err
	}
	if ephemeral && (len(plan.Ephemeral.Msgs) > 0 || len(plan.Ephemeral.NotificationMsgs) > 0) {
		if err = common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
			if len(plan.Ephemeral.Msgs) > 0 {
				c.doMsgNew(common.Cmd2Value{Ctx: ctx, Value: sdk_struct.CmdNewMsgComeToConversation{Msgs: plan.Ephemeral.Msgs}})
			}
			for _, batch := range plan.Ephemeral.NotificationMsgs {
				for _, message := range batch.Msgs {
					if err := c.dispatchConversationSyncV2Notification(ctx, message); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if len(plan.UnknownNotificationIDs) > 0 {
		return syncv2.ErrMetadataSnapshotRequired
	}
	op := ccontext.Info(ctx).OperationID()
	parents := make([]string, 0, len(plan.Notifications))
	self := false
	for _, req := range plan.Notifications {
		if req.Self {
			self = true
		} else {
			parents = append(parents, req.ConversationID)
		}
	}
	pending := false
	if len(parents) > 0 || self {
		result, err := runtime.client.SyncStreams(ctx, op, &pb.StateRequest{UserID: c.loginUserID, ConversationIDs: parents, Notifications: true, IncludeSelf: self}, caps, runtime.store, c.ConsumeConversationSyncV2)
		if err != nil {
			return err
		}
		pending = len(result.PendingIDs) > 0
		for _, id := range result.UnavailableIDs {
			delete(push.NotificationMsgs, id)
		}
	}
	for _, chat := range plan.Chats {
		checkpoint, err := runtime.client.ActivateConversation(ctx, op, chat.ConversationID, caps, runtime.store)
		if err != nil {
			if syncv2.RecoveryFor(err) == syncv2.Unavailable {
				delete(push.Msgs, chat.ConversationID)
				continue
			}
			if errors.Is(err, syncv2.ErrStatePending) {
				pending = true
				continue
			}
			return err
		}
		var notifyIDs map[string]bool
		if chat.Activate {
			notifyIDs = make(map[string]bool)
			for _, message := range push.Msgs[chat.ConversationID].Msgs {
				if message.Seq > 0 {
					notifyIDs[message.ClientMsgID] = true
				}
			}
			newChats[chat.ConversationID] = notifyIDs
		}
		notifyIDs = newChats[chat.ConversationID]
		var required []int64
		for _, message := range push.Msgs[chat.ConversationID].Msgs {
			if message.Seq > 0 {
				required = append(required, message.Seq)
			}
		}
		stream, err := runtime.client.SyncStream(syncv2.WithChatWindow(ctx, 10), op, &pb.PullRequest{UserID: c.loginUserID, ConversationID: chat.ConversationID, Seqs: required}, caps, runtime.store, func(ctx context.Context, progress *model_struct.LocalConversationSyncV2Stream, messages []*sdkws.MsgData) error {
			return c.consumeConversationSyncV2LiveSelected(ctx, progress, messages, notifyIDs)
		})
		if err != nil {
			if syncv2.RecoveryFor(err) == syncv2.Unavailable {
				delete(push.Msgs, chat.ConversationID)
				continue
			}
			return err
		}
		pending = pending || len(stream.PendingSeqs) > 0
		projection := *checkpoint
		projection.SelectedIDs = []string{chat.ConversationID}
		if err = c.publishConversationSyncV2(ctx, &projection); err != nil {
			return err
		}
		if err = common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
			conversation, err := c.db.GetConversation(ctx, chat.ConversationID)
			if err != nil {
				return err
			}
			if conversation.LatestMsgSendTime == 0 {
				return nil
			}
			payload := utils.StructToJsonString([]*model_struct.LocalConversation{conversation})
			if chat.Activate {
				c.ConversationListener().OnNewConversation(payload)
			} else {
				c.ConversationListener().OnConversationChanged(payload)
			}
			unread, err := c.db.GetTotalUnreadMsgCountDB(ctx)
			if err != nil {
				return err
			}
			c.ConversationListener().OnTotalUnreadMessageCountChanged(unread)
			return nil
		}); err != nil {
			return err
		}
	}
	if pending {
		return syncv2.ErrStatePending
	}
	return nil
}
