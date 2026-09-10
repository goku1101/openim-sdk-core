package syncv2

import (
	"github.com/openimsdk/openim-sdk-core/v3/pkg/syncv2/pb"
	conversation "github.com/openimsdk/protocol/conversation"
	"reflect"
	"testing"
)

func TestInitialPageIsBoundedOrderedAndNeverActivatesLocalArchives(t *testing.T) {
	states := []*pb.State{
		{ConversationID: "old", Known: true, LastMessageTime: 1},
		{ConversationID: "new", Known: true, LastMessageTime: 100},
		{ConversationID: "pin", Known: true, IsPinned: true},
		{ConversationID: "draft", Known: true, LastMessageTime: 2},
		{ConversationID: "tie-b", Known: true, LastMessageTime: 50},
		{ConversationID: "tie-a", Known: true, LastMessageTime: 50},
		{ConversationID: "unknown", Known: false, IsPinned: true},
	}
	for _, s := range states {
		s.Conversation = &conversation.Conversation{ConversationID: s.ConversationID}
	}
	drafts := map[string]int64{"draft": 200, "local-archive": 1000}
	want := []string{"pin", "draft", "new", "tie-a", "tie-b"}
	if got := InitialPageIDs(states, drafts, 5); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := InitialPageIDs(nil, drafts, 50); len(got) != 0 {
		t.Fatalf("archive activated: %v", got)
	}
	if got := InitialPageIDs(states, drafts, 0); len(got) != 0 {
		t.Fatalf("zero limit: %v", got)
	}
}
