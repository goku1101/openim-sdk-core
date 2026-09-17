package group

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/ccontext"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/db_interface"
	"github.com/openimsdk/openim-sdk-core/v3/pkg/db/model_struct"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/sdkws"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type displayDB struct{ db_interface.DataBase }

func (*displayDB) GetGroupSomeMemberInfo(context.Context, string, []string) ([]*model_struct.LocalGroupMember, error) {
	return nil, nil
}

func TestDisplayProfilesCoalesceOverlapAndBoundBatches(t *testing.T) {
	var calls, active, peak atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		calls.Add(1)
		var req struct {
			GroupID string   `json:"groupID"`
			UserIDs []string `json:"userIDs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if len(req.UserIDs) > 100 {
			t.Error("unbounded batch")
		}
		members := []*sdkws.GroupMemberFullInfo{}
		for _, id := range req.UserIDs {
			members = append(members, &sdkws.GroupMemberFullInfo{GroupID: req.GroupID, UserID: id, Nickname: "name"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errCode": 0, "data": map[string]any{"members": members}})
	}))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "fixture", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "profile")
	g := NewGroup("self", &displayDB{}, nil)
	ids := []string{}
	for i := 0; i < 205; i++ {
		ids = append(ids, fmt.Sprintf("id-%d", i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := g.GetMissingDisplayProfiles(ctx, "g", ids)
			if err != nil || len(got) != 205 {
				t.Errorf("count=%d error=%v", len(got), err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 3 || peak.Load() != 1 {
		t.Fatalf("calls=%d peak=%d", calls.Load(), peak.Load())
	}
}

func TestDisplayProfilesFailureDeduplicatesWithoutClaimingMembership(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	defer server.Close()
	ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "fixture", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "profile")
	g := NewGroup("self", &displayDB{}, nil)
	for i := 0; i < 10; i++ {
		got, _ := g.GetMissingDisplayProfiles(ctx, "g", []string{"u"})
		if len(got) != 0 {
			t.Fatal("missing member invented")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("transient error retried immediately")
	}
	g.displayQuerySlot <- struct{}{}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := g.GetMissingDisplayProfiles(canceled, "g", []string{"other"}); err != context.Canceled {
		t.Fatal(err)
	}
	<-g.displayQuerySlot
	if calls.Load() != 1 {
		t.Fatal("canceled waiter sent request")
	}
}

func TestDisplayProfilesRespectServerRetryAfterAcrossUsers(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"errCode":8,"errMsg":"busy","errDlt":"try later"}`))
			}))
			defer server.Close()
			ctx := ccontext.WithOperationID(ccontext.WithInfo(context.Background(), &ccontext.GlobalConfig{UserID: "self", Token: "fixture", IMConfig: sdk_struct.IMConfig{ApiAddr: server.URL}}), "profile-backoff")
			g := NewGroup("self", &displayDB{}, nil)
			started := time.Now()
			if _, err := g.GetMissingDisplayProfiles(ctx, "g", []string{"u"}); err == nil {
				t.Fatal("expected admission error")
			}
			if got, err := g.GetMissingDisplayProfiles(ctx, "another-group", []string{"other-user"}); err != nil || len(got) != 0 {
				t.Fatalf("optional lookup during cooldown: %v %v", got, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("different notifications bypassed Retry-After: calls=%d", calls.Load())
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("optional profiles blocked on cooldown")
			}
			if g.displayRetryAt.Before(started.Add(time.Minute)) {
				t.Fatal("server deadline was shortened")
			}
			g.displayRetryAt = time.Now().Add(-time.Second)
			_, _ = g.GetMissingDisplayProfiles(ctx, "another-group", []string{"other-user"})
			if calls.Load() != 2 {
				t.Fatal("lookup did not resume after cooldown")
			}

		})
	}
}
