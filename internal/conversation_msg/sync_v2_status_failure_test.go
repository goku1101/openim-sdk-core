package conversation_msg

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"testing"
)

func TestBackgroundFailureRetainsOnlyConfirmedReadiness(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		for _, ready := range []bool{false, true} {
			for _, action := range []syncv2.Recovery{syncv2.Retry, syncv2.RefreshState, syncv2.Unavailable, syncv2.Stop, syncv2.RebuildSnapshot} {
				state := conversationSyncV2FailureState(sdk_struct.ConversationSyncState{UserID: "u", InitialDataReady: ready, PriorityReady: ready}, cancelled, action)
				expected := !cancelled && ready && (action == syncv2.Retry || action == syncv2.RefreshState || action == syncv2.Unavailable)
				if (state.Phase == "retry_wait") != expected || state.InitialDataReady != expected || state.PriorityReady != expected || state.UserID != "u" {
					t.Fatalf("cancel=%v ready=%v action=%v state=%+v", cancelled, ready, action, state)
				}
			}
		}
	}
}
