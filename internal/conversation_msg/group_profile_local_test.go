package conversation_msg

import (
	"context"
	"encoding/json"
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

func TestGroupProfileMissingOrdinarySenderIsEnriched(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"errCode":0,"data":{"members":[{"groupID":"group-a","userID":"9223372036854775807","nickname":"fetched","faceURL":"fetched.png"}]}}`)
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "test-only", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "display")
	c := &Conversation{group: group.NewGroup("self", &profileLocalDB{members: map[string]*model_struct.LocalGroupMember{}}, nil)}
	for i := 0; i < 2; i++ {
		m := &model_struct.LocalChatLog{SendID: "9223372036854775807", ContentType: 101}
		c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, &model_struct.LocalConversation{GroupID: "group-a"})
		if m.SenderNickname != "fetched" || m.SenderFaceURL != "fetched.png" {
			t.Fatal("ordinary sender was not enriched")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("requests=%d", calls.Load())
	}
}

func TestGroupProfileNotificationNeverFetchesAndReusesBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "test-only", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "notification-display")
	c := &Conversation{group: group.NewGroup("self", &profileLocalDB{members: map[string]*model_struct.LocalGroupMember{}}, nil)}
	for _, tc := range []struct {
		kind   int32
		detail string
		want   string
	}{
		{1510, `{"entrantUser":{"userID":"u","nickname":"entrant","faceURL":"face"}}`, "entrant"},
		{1509, `{"opUser":{"userID":"u","nickname":"operator","faceURL":"face"}}`, "operator"},
		{1509, `{"inviterUser":{"userID":"u","nickname":"inviter","faceURL":"face"}}`, "inviter"},
		{1509, `{}`, ""},
	} {
		content, _ := json.Marshal(map[string]string{"detail": tc.detail})
		m := &model_struct.LocalChatLog{SendID: "u", ContentType: tc.kind, Content: string(content)}
		c.groupHandle(ctx, nil, []*model_struct.LocalChatLog{m}, &model_struct.LocalConversation{GroupID: "g"})
		if m.SenderNickname != tc.want {
			t.Fatalf("notification profile %q", m.SenderNickname)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("system notification caused remote display query")
	}
}
