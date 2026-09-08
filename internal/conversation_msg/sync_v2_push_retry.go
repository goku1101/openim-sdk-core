package conversation_msg

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

// Save before processing. A paused deadline prevents the worker from racing the
// live attempt; new login resumes it if cancellation/crash interrupts the receipt.
func (c *Conversation) ProcessConversationSyncV2Push(ctx context.Context, push *sdkws.PushMessages) (bool, error) {
	if c.syncV2 == nil || c.syncV2.mode.Load() == syncV2Legacy {
		return false, nil
	}
	durable, err := syncv2.DurablePush(push)
	if err != nil {
		return true, err
	}
	if len(durable.Msgs) == 0 && len(durable.NotificationMsgs) == 0 {
		return c.processConversationSyncV2Push(ctx, push, true)
	}
	payload, err := json.Marshal(durable)
	if err != nil {
		return true, err
	}
	job := &model_struct.LocalConversationSyncV2Retry{TaskID: "push:" + utils.OperationIDGenerator(), Kind: "push", Payload: string(payload), DueAt: math.MaxInt64, Revision: 1}
	if err = c.syncV2.store.PutConversationSyncV2Retry(ctx, job); err != nil {
		return true, err
	}
	live := proto.Clone(push).(*sdkws.PushMessages)
	handled, runErr := c.processConversationSyncV2Push(ctx, live, true)
	if ctx.Err() != nil {
		return handled, runErr
	}
	if runErr == nil {
		if err = c.syncV2.store.AcknowledgeConversationSyncV2Retry(ctx, job); err != nil {
			return true, err
		}
		return handled, nil
	}
	if err = c.updateConversationSyncV2PushRetry(ctx, job, live); err != nil {
		return true, err
	}
	action := syncv2.RecoveryFor(runErr)
	if errors.Is(runErr, syncv2.ErrStatePending) || action == syncv2.Retry || action == syncv2.RefreshState || action == syncv2.RebuildSnapshot || action == syncv2.Renegotiate {
		if err = c.syncV2.store.RescheduleConversationSyncV2Retry(ctx, job, time.Now().Add(time.Second).UnixMilli()); err != nil {
			return true, err
		}
		c.syncV2.retryMu.Lock()
		if c.syncV2.retryWake != nil {
			select {
			case c.syncV2.retryWake <- struct{}{}:
			default:
			}
		}
		c.syncV2.retryMu.Unlock()
	}
	return handled, runErr
}

func (c *Conversation) updateConversationSyncV2PushRetry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry, push *sdkws.PushMessages) error {
	durable, err := syncv2.DurablePush(push)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(durable)
	if err != nil {
		return err
	}
	if string(payload) == job.Payload {
		return nil
	}
	return c.syncV2.store.UpdateConversationSyncV2RetryPayload(ctx, job, string(payload))
}
