package group

import (
	"context"
	"reflect"
	"testing"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
)

func TestGroupMemberFetchOnlyDistinctMissingIDs(t *testing.T) {
	g := &Group{groupMemberCache: newMemberProfileCache()}
	g.groupMemberCache.Store(g.buildGroupMemberKey("g", "cached"), &model_struct.LocalGroupMember{UserID: "cached"})
	calls := 0
	fetch := func(_ context.Context, ids []string) ([]*model_struct.LocalGroupMember, error) {
		calls++
		if !reflect.DeepEqual(ids, []string{"a", "b"}) {
			t.Fatalf("requested IDs = %v, want only distinct missing IDs [a b]", ids)
		}
		return []*model_struct.LocalGroupMember{{UserID: "a"}, {UserID: "b"}}, nil
	}
	for i := 0; i < 2; i++ {
		got, err := g.GetGroupMembersInfoFunc(context.Background(), "g", []string{"cached", "a", "b", "a"}, fetch)
		if err != nil || len(got) != 3 {
			t.Fatalf("result count=%d err=%v", len(got), err)
		}
	}
	if calls != 1 {
		t.Fatalf("fetch calls=%d, want 1", calls)
	}
}
