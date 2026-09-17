package conversation_msg

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/protocol/sdkws"
)

// StartConversationSyncV2Retries runs once per authenticated session. The owner
// cancels that session and joins this worker before closing its database.
func (c *Conversation) StartConversationSyncV2Retries(ctx context.Context) error {
	r := c.syncV2
	if r == nil {
		return nil
	}
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	if r.retryDone != nil {
		return nil
	}
	if err := r.store.ResumeConversationSyncV2Retries(ctx, time.Now().UnixMilli()); err != nil {
		return err
	}
	r.retryDone = make(chan struct{})
	r.retryWake = make(chan struct{}, 1)
	go func() {
		defer close(r.retryDone)
		_ = syncv2.RunRetryTasks(ctx, r.store, r.retryWake, c.runConversationSyncV2Retry)
	}()
	return nil
}

func (c *Conversation) WaitConversationSyncV2Retries() {
	if c == nil || c.syncV2 == nil {
		return
	}
	c.syncV2.retryMu.Lock()
	done := c.syncV2.retryDone
	c.syncV2.retryMu.Unlock()
	if done != nil {
		<-done
	}
}

func (c *Conversation) persistConversationSyncV2Pending(ctx context.Context, result *syncv2.SyncResult) error {
	jobs, err := syncv2.PendingRetryTasks(result, time.Now().Add(time.Second).UnixMilli())
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err = c.syncV2.store.PutConversationSyncV2Retry(ctx, job); err != nil {
			return err
		}
	}
	c.syncV2.retryMu.Lock()
	defer c.syncV2.retryMu.Unlock()
	if c.syncV2.retryWake != nil {
		select {
		case c.syncV2.retryWake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *Conversation) runConversationSyncV2Retry(ctx context.Context, job *model_struct.LocalConversationSyncV2Retry) error {
	r := c.syncV2
	if r.mode.Load() == syncV2Legacy {
		return fmt.Errorf("sync v2: retry paused in legacy session")
	}
	if c.GetConversationSyncState().Phase != "ready" {
		return syncv2.ErrStatePending
	}
	if job.Kind == "push" {
		var push sdkws.PushMessages
		if err := json.Unmarshal([]byte(job.Payload), &push); err != nil {
			return err
		}
		durable, err := syncv2.DurablePush(&push)
		if err != nil {
			return err
		}
		_, err = c.processConversationSyncV2Push(ctx, durable, false)
		if ctx.Err() == nil && err != nil {
			if updateErr := c.updateConversationSyncV2PushRetry(ctx, job, durable); updateErr != nil {
				return updateErr
			}
		}
		return err
	}
	if err := r.acquireRun(ctx); err != nil {
		return err
	}
	defer r.releaseRun()
	if job.Kind != "reconcile" && job.Kind != "state" {
		return fmt.Errorf("sync v2: unsupported retry task")
	}
	var req pb.PullRequest
	if err := json.Unmarshal([]byte(job.Payload), &req); err != nil {
		return err
	}
	if req.UserID != c.loginUserID {
		return fmt.Errorf("sync v2: retry account mismatch")
	}
	r.capsMu.RLock()
	caps := r.caps
	r.capsMu.RUnlock()
	if caps == nil {
		return syncv2.ErrStatePending
	}
	ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	if job.Kind == "state" {
		if req.Notifications || req.Self {
			return fmt.Errorf("sync v2: invalid metadata retry")
		}
		state, err := r.client.RevalidatePendingState(ctx, ccontext.Info(ctx).OperationID(), req.ConversationID, caps, r.store)
		if err != nil {
			return err
		}
		cp, err := r.store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		if !state.IsPinned && state.UnreadCount == 0 && !slices.Contains(cp.SelectedIDs, req.ConversationID) {
			return nil
		}
	}
	if !req.Notifications {
		if _, err := r.client.ActivateConversation(ctx, ccontext.Info(ctx).OperationID(), req.ConversationID, caps, r.store); err != nil {
			return err
		}
	}
	stream, err := r.client.SyncStream(syncv2.WithChatWindow(ctx, 10), ccontext.Info(ctx).OperationID(), &req, caps, r.store, c.ConsumeConversationSyncV2)
	if err != nil {
		return err
	}
	if !req.Notifications {
		cp, err := r.store.GetConversationSyncV2(ctx)
		if err != nil {
			return err
		}
		projection := *cp
		projection.SelectedIDs = []string{req.ConversationID}
		if err = c.publishConversationSyncV2(ctx, &projection); err != nil {
			return err
		}
		if err = common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
			conversation, err := c.db.GetConversation(ctx, req.ConversationID)
			if err != nil {
				return err
			}
			if conversation.LatestMsgSendTime == 0 {
				return nil
			}
			c.ConversationListener().OnConversationChanged(utils.StructToJsonString([]*model_struct.LocalConversation{conversation}))
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
	if len(stream.PendingSeqs) > 0 {
		return syncv2.ErrStatePending
	}
	return nil
}
