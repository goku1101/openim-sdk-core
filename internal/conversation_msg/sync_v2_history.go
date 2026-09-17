package conversation_msg

import (
	"context"
	"sort"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/common"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	sdk "github.com/openimsdk/openim-sdk-core/v3/pkg/sdk_params_callback"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
)

// This attempt-local flag distinguishes durable sequence holes from unknown
// permissions and network errors. Only the former allow partial delivery.
type historyUnresolvedKey struct{}

func (c *Conversation) fetchConversationSyncV2History(ctx context.Context, id string, seqs []int64, isReverse bool, count int, list *[]*model_struct.LocalChatLog, callback *sdk.GetAdvancedHistoryMessageListCallback) {
	ctx = syncv2.WithForegroundRead(ctx)
	if callback.ErrCode != 0 {
		return
	}
	fail := func(err error) { callback.ErrCode = 100; callback.ErrMsg = err.Error(); callback.IsEnd = false }
	runtime := c.syncV2
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if runtime.mode.Load() != syncV2Enabled || caps == nil {
		fail(syncv2.ErrStatePending)
		return
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	page, err := runtime.client.ReadSequencePageAndCommit(ctx, ccontext.Info(ctx).OperationID(), &pb.PullRequest{UserID: c.loginUserID, ConversationID: id, Seqs: seqs}, caps, runtime.store, func(page *syncv2.SequencePage) error {
		rows := make([]*model_struct.LocalChatLog, 0, len(page.Response.Messages))
		for _, message := range page.Response.Messages {
			rows = append(rows, MsgDataToLocalChatLog(message))
		}
		return common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
			return runtime.store.CommitConversationSyncV2History(ctx, page.Progress.Epoch, page.Progress, rows, page.Response.State)
		})
	})
	if err != nil {
		fail(err)
		return
	}
	resolved := make([]int64, 0, len(page.Response.Messages))
	for _, message := range page.Response.Messages {
		resolved = append(resolved, message.Seq)
	}
	state := page.Response.State
	c.maxSeqRecorder.Set(id, state.MaxSeq)
	var incoming []*model_struct.LocalChatLog
	for start := 0; start < len(resolved); start += 500 {
		messages, err := c.db.GetMessagesBySeqs(ctx, id, resolved[start:min(start+500, len(resolved))])
		if err != nil {
			fail(err)
			return
		}
		incoming = append(incoming, messages...)
	}
	// Keep already-read local flags/deletion state from the committed rows.
	window := make([]*model_struct.LocalChatLog, 0, len(*list))
	seen := make(map[string]bool, len(*list))
	for _, message := range *list {
		if message.Seq == 0 || message.Seq >= state.MinSeq && message.Seq <= state.MaxSeq {
			window = append(window, message)
			seen[message.ClientMsgID] = true
		}
	}
	unique := incoming[:0]
	for _, message := range incoming {
		if !seen[message.ClientMsgID] {
			unique = append(unique, message)
			seen[message.ClientMsgID] = true
		}
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].SendTime == unique[j].SendTime {
			if isReverse {
				return unique[i].Seq < unique[j].Seq
			}
			return unique[i].Seq > unique[j].Seq
		}
		if isReverse {
			return unique[i].SendTime < unique[j].SendTime
		}
		return unique[i].SendTime > unique[j].SendTime
	})
	*list = mergeSortedArrays(window, unique, count, !isReverse)
	if len(page.Response.UnresolvedSeqs) > 0 {
		if pending, ok := ctx.Value(historyUnresolvedKey{}).(*bool); ok {
			*pending = true
		}
		fail(syncv2.ErrStatePending)
	}
}

func (c *Conversation) ensureConversationSyncV2HistoryState(ctx context.Context, id string) error {
	ctx = syncv2.WithForegroundRead(ctx)
	runtime := c.syncV2
	if runtime.mode.Load() != syncV2Enabled {
		return syncv2.ErrStatePending
	}
	stream, err := runtime.store.GetConversationSyncV2Stream(ctx, id)
	if err != nil {
		return err
	}
	if stream.Initialized && stream.StateToken != "" {
		return nil
	}
	runtime.capsMu.RLock()
	caps := runtime.caps
	runtime.capsMu.RUnlock()
	if caps == nil {
		return syncv2.ErrStatePending
	}
	if ccontext.Info(ctx).OperationID() == "" {
		ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())
	}
	_, err = runtime.client.ReadSequencePageAndCommit(ctx, ccontext.Info(ctx).OperationID(), &pb.PullRequest{UserID: c.loginUserID, ConversationID: id}, caps, runtime.store, func(page *syncv2.SequencePage) error {
		return common.RunConversationSyncV2(ctx, c.conversationEventQueue, func() error {
			return runtime.store.CommitConversationSyncV2History(ctx, page.Progress.Epoch, page.Progress, nil, page.Response.State)
		})
	})
	return err
}
