package conversation_msg

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/internal/group"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/db_interface"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type profileLocalDB struct {
	db_interface.DataBase
	members map[string]*model_struct.LocalGroupMember
	err     error
}

func (d *profileLocalDB) GetGroupSomeMemberInfo(ctx context.Context, g string, ids []string) ([]*model_struct.LocalGroupMember, error) {
	if d.err != nil {
		return nil, d.err
	}
	out := []*model_struct.LocalGroupMember{}
	for _, id := range ids {
		if m := d.members[g+":"+id]; m != nil {
			out = append(out, m)
		}
	}
	return out, nil
}
func TestGroupProfileLocalOnly(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(502)
		fmt.Fprint(w, "<html>Bad Gateway</html>")
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "test-only", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "test-local-only")
	db := &profileLocalDB{members: map[string]*model_struct.LocalGroupMember{}}
	c := &Conversation{group: group.NewGroup("self", db, nil)}
	message := func() *model_struct.LocalChatLog {
		return &model_struct.LocalChatLog{SendID: "9223372036854775807", SenderFaceURL: "snapshot.png", SenderNickname: "snapshot"}
	}
	lc := &model_struct.LocalConversation{GroupID: "group-a"}
	for i := 0; i < 960; i++ {
		m := message()
		c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, lc)
		if m.SenderFaceURL != "snapshot.png" || m.SenderNickname != "snapshot" {
			t.Fatal("snapshot lost")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("960 message batches caused %d network requests", calls.Load())
	}
	// Simulate an ordinary member API updating local storage; no enrichment fetch is needed.
	db.members["group-a:9223372036854775807"] = &model_struct.LocalGroupMember{UserID: "9223372036854775807", Nickname: "new", FaceURL: "new.png"}
	m := message()
	c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, lc)
	if m.SenderNickname != "new" || m.SenderFaceURL != "new.png" {
		t.Fatal("normal cached update not used")
	}
	db.members["group-a:9223372036854775807"].FaceURL = ""
	m = message()
	c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, lc)
	if m.SenderFaceURL != "snapshot.png" || m.SenderNickname != "new" {
		t.Fatal("empty cached field erased snapshot")
	}
	m = message()
	c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, &model_struct.LocalConversation{GroupID: "group-b"})
	if m.SenderNickname != "snapshot" {
		t.Fatal("profile crossed group boundary")
	}
	db.err = errors.New("local database unavailable")
	m = message()
	c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, lc)
	if m.SenderNickname != "snapshot" {
		t.Fatal("database failure erased snapshot")
	}
	if calls.Load() != 0 {
		t.Fatal("network called")
	}
}
