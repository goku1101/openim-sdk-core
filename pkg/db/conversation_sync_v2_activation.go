//go:build !js

package db

import (
	"context"
	"fmt"
	"slices"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
)

// Activation refreshes one authorized chat without treating other selected
// conversations as missing from this deliberately partial response.
func (d *DataBase) ActivateConversationSyncV2(ctx context.Context, before *model_struct.LocalConversationSyncV2, epoch uint64, state *pb.State) error {
	if before == nil || before.UserID != d.loginUserID || state == nil || state.ConversationID == "" || !state.Known || state.Conversation == nil || state.StateToken == "" || before.Stage != "ready" && before.Stage != "messages" && before.Stage != "catchup" {
		return fmt.Errorf("sync v2: invalid activation state")
	}
	id := state.ConversationID
	next := *before
	next.SelectedIDs = append(model_struct.StringArray(nil), before.SelectedIDs...)
	next.NotificationIDs = append(model_struct.StringArray(nil), before.NotificationIDs...)
	if !slices.Contains(next.SelectedIDs, id) {
		next.SelectedIDs = append(next.SelectedIDs, id)
	}
	if !slices.Contains(next.NotificationIDs, id) {
		next.NotificationIDs = append(next.NotificationIDs, id)
	}
	next.StateEpochs = map[string]uint64{id: epoch}
	return d.applyConversationSyncV2Details(ctx, before, &next, &pb.StateResponse{States: []*pb.State{state}}, nil, []string{id}, false)
}
