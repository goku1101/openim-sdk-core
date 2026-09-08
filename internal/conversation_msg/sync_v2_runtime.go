package conversation_msg

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/constant"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
)

const (
	syncV2Negotiating int32 = iota
	syncV2Legacy
	syncV2Enabled
)

type conversationSyncV2Store interface {
	syncv2.CoordinatorStore
	syncv2.ActivationStore
	InvalidateConversationSyncV2Stream(context.Context, string) error
	CommitConversationSyncV2History(context.Context, uint64, *model_struct.LocalConversationSyncV2Stream, []*model_struct.LocalChatLog, *pb.State) error
	GetConversationSyncV2LatestMessage(context.Context, string, int64, int64) ([]*model_struct.LocalChatLog, error)
	ApplyConversationSyncV2Projection(context.Context, *model_struct.LocalConversationSyncV2, map[string]uint64, []*model_struct.LocalConversation) error
	PrepareConversationSyncV2Legacy(context.Context) error
}

type conversationSyncV2Runtime struct {
	client *syncv2.Client
	store  conversationSyncV2Store
	mode   atomic.Int32
	capsMu sync.RWMutex
	caps   *pb.Capabilities
	runMu  sync.Mutex
}

// ConfigureConversationSyncV2 runs once per login before connection workers.
// The JS database has no native V2 tables and retains its existing integration.
func (c *Conversation) ConfigureConversationSyncV2(ctx context.Context) bool {
	store, ok := c.db.(conversationSyncV2Store)
	if !ok {
		return false
	}
	info := ccontext.Info(ctx)
	runtime := &conversationSyncV2Runtime{client: syncv2.NewClient(info.ApiAddr(), info.Token(), nil), store: store}
	c.syncV2 = runtime
	c.LongConnMgr.SetSyncRequestGuard(func(_ context.Context, identifier int) error {
		if runtime.mode.Load() == syncV2Legacy {
			return nil
		}
		switch identifier {
		case constant.GetNewestSeq, constant.GetConvMaxReadSeq, constant.PullMsgByRange, constant.PullMsgBySeqList, constant.PullConvLastMessage:
			return fmt.Errorf("sync v2: legacy message synchronization route disabled")
		}
		return nil
	})
	return true
}

func (c *Conversation) SynchronizeConversationV2(ctx context.Context, reinstalled bool) (bool, error) {
	runtime := c.syncV2
	if runtime == nil {
		return false, nil
	}
	runtime.runMu.Lock()
	defer runtime.runMu.Unlock()
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	started := false
	hooks := syncv2.CoordinatorHooks{
		Negotiated: func(ctx context.Context, caps *pb.Capabilities) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !caps.ConversationSyncV2 {
				if started {
					return fmt.Errorf("sync v2: capability changed during active round")
				}
				if err := runtime.store.PrepareConversationSyncV2Legacy(ctx); err != nil {
					return err
				}
				runtime.mode.Store(syncV2Legacy)
				return nil
			}
			runtime.capsMu.Lock()
			runtime.caps = caps
			runtime.capsMu.Unlock()
			runtime.mode.Store(syncV2Enabled)
			if !started {
				started = true
				c.ConversationListener().OnSyncServerStart(reinstalled)
				c.ConversationListener().OnSyncServerProgress(1)
			}
			return nil
		},
		PrepareRelated: func(ctx context.Context) error {
			for _, run := range []func(context.Context) error{c.group.SyncAllJoinedGroupsAndMembersWithLock, c.relation.IncrSyncFriendsWithLock, c.user.SyncLoginUserInfoWithoutNotice, c.relation.SyncAllBlackListWithoutNotice} {
				if err := run(ctx); err != nil {
					return err
				}
			}
			return nil
		},
		ReadMetadata: c.getIncrementalConversationFromServer,
		Commit:       c.ConsumeConversationSyncV2,
		Publish:      c.publishConversationSyncV2,
	}
	var result *syncv2.SyncResult
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		result, err = runtime.client.Synchronize(ctx, ccontext.Info(ctx).OperationID(), c.loginUserID, runtime.store, hooks)
		if err == nil {
			break
		}
		action := syncv2.RecoveryFor(err)
		if attempt == 1 || ctx.Err() != nil || action != syncv2.RebuildSnapshot && action != syncv2.RefreshState && action != syncv2.Retry && action != syncv2.Renegotiate {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return true, ctx.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		if ctx.Err() == nil {
			c.ConversationListener().OnSyncServerFailed(reinstalled)
		}
		return true, err
	}
	if !result.Enabled {
		return false, nil
	}
	version, err := c.db.GetAppSDKVersion(ctx)
	if err == nil {
		version.Installed = true
		err = c.db.SetAppSDKVersion(ctx, version)
	}
	if err != nil {
		c.ConversationListener().OnSyncServerFailed(reinstalled)
		return true, err
	}
	total, err := c.db.GetTotalUnreadMsgCountDB(ctx)
	if err != nil {
		c.ConversationListener().OnSyncServerFailed(reinstalled)
		return true, err
	}
	if err = ctx.Err(); err != nil {
		return true, err
	}
	c.ConversationListener().OnTotalUnreadMessageCountChanged(total)
	c.ConversationListener().OnSyncServerProgress(100)
	c.ConversationListener().OnSyncServerFinish(reinstalled)
	return true, nil
}

// Metadata handlers can execute within the conversation queue. Do not acquire
// runMu here: the round may be waiting for that queue's consumption receipt.
func (c *Conversation) refreshConversationSyncV2Metadata(ctx context.Context) error {
	runtime := c.syncV2
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if runtime.mode.Load() != syncV2Enabled || caps == nil {
		return syncv2.ErrStatePending
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	checkpoint, err := runtime.store.GetConversationSyncV2(ctx)
	if err != nil {
		return err
	}
	delta, err := c.getIncrementalConversationFromServer(ctx, checkpoint.Version, checkpoint.VersionID)
	if err != nil {
		return err
	}
	_, err = runtime.client.ApplyMetadataDelta(ctx, ccontext.Info(ctx).OperationID(), checkpoint, delta, caps, runtime.store)
	return err
}

func (c *Conversation) publishConversationSyncV2(ctx context.Context, checkpoint *model_struct.LocalConversationSyncV2) error {
	return common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
		epochs, err := c.syncV2.store.GetConversationSyncV2Epochs(ctx, checkpoint.SelectedIDs)
		if err != nil {
			return err
		}
		for start := 0; start < len(checkpoint.SelectedIDs); start += 300 {
			conversations, err := c.db.GetMultipleConversationDB(ctx, checkpoint.SelectedIDs[start:min(start+300, len(checkpoint.SelectedIDs))])
			if err != nil {
				return err
			}
			if err = c.batchAddFaceURLAndName(ctx, conversations...); err != nil {
				return err
			}
			for _, conversation := range conversations {
				messages, err := c.syncV2.store.GetConversationSyncV2LatestMessage(ctx, conversation.ConversationID, conversation.MinSeq, conversation.MaxSeq)
				if err != nil {
					return err
				}
				conversation.LatestMsg = ""
				conversation.LatestMsgSendTime = 0
				if len(messages) > 0 {
					message := LocalChatLogToMsgStruct(messages[0])
					conversation.LatestMsg = utils.StructToJsonString(message)
					conversation.LatestMsgSendTime = message.SendTime
				}
			}
			if err = c.syncV2.store.ApplyConversationSyncV2Projection(ctx, checkpoint, epochs, conversations); err != nil {
				return err
			}
			for _, conversation := range conversations {
				c.maxSeqRecorder.Set(conversation.ConversationID, conversation.MaxSeq)
			}
		}
		return nil
	})
}
