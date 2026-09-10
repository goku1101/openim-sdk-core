//go:build !js

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
)

// BenchmarkSyncV2MetadataCommit measures only the real SQLite transaction and
// its epoch precondition read; HTTP and UI projection are intentionally absent.
func BenchmarkSyncV2MetadataCommit(b *testing.B) {
	for _, size := range []int{500, 10000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			ctx := context.Background()
			database, err := NewDataBase(ctx, "benchmark-user", b.TempDir(), 0)
			if err != nil {
				b.Fatal(err)
			}
			defer database.Close(ctx)
			before := &model_struct.LocalConversationSyncV2{UserID: "benchmark-user", Generation: "benchmark-generation", SnapshotID: "benchmark-snapshot", Stage: "snapshot", VersionID: "benchmark-version", Version: 1}
			response := new(pb.StateResponse)
			for i := 0; i < size; i++ {
				id := fmt.Sprintf("sg_benchmark_%d", i)
				before.SelectedIDs = append(before.SelectedIDs, id)
				response.States = append(response.States, &pb.State{ConversationID: id, Known: true, MinSeq: 1, MaxSeq: 10, UnreadCount: 2, Conversation: &conversation.Conversation{ConversationID: id, OwnerUserID: before.UserID, ConversationType: 3, GroupID: id}})
			}
			if err = database.SaveConversationSyncV2(ctx, before); err != nil {
				b.Fatal(err)
			}
			before.Stage = "messages"
			if err = database.SaveConversationSyncV2(ctx, before); err != nil {
				b.Fatal(err)
			}
			apply := func() {
				next := *before
				next.StateEpochs, err = database.GetConversationSyncV2Epochs(ctx, before.SelectedIDs)
				if err != nil {
					b.Fatal(err)
				}
				if err = database.ApplyConversationSyncV2Metadata(ctx, before, &next, response, nil); err != nil {
					b.Fatal(err)
				}
				before, err = database.GetConversationSyncV2(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}
			apply() // Populate existing rows before timing recurring metadata refreshes.
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				apply()
			}
		})
	}
}
